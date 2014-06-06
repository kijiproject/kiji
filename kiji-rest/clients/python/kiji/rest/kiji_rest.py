#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""KijiRest client.

For more documentation on Kiji and KijiREST,
see http://docs.kiji.org/userguides.html
"""

import collections
import getpass
import http
import logging
import os
import shutil
import signal
import socket
import sys
import time
import urllib.request
import yaml

from base import base
from base import cli
from base import command


FLAGS = base.FLAGS
LogLevel = base.LogLevel
HttpMethod = base.HttpMethod
ContentType = base.ContentType
ExitCode = base.ExitCode


class Error(Exception):
  """Errors raised in this module."""
  pass


# ------------------------------------------------------------------------------


def _RestEntityId(eid):
  """Normalizes an entity ID for a KijiREST call.

  KijiREST expects formatted entity IDs always.
  This means that non formatted entity IDs must be wrapped in a singleton list.

  Args:
    eid: Entity ID, as a Python value.
  Returns:
    Normalized Python value accepted by the KijiREST server.
  """
  if isinstance(eid, str):
    return [eid]
  elif isinstance(eid, collections.Iterable):
    return eid
  else:
    return [eid]


class KijiRestClient(object):
  """Client for a KijiREST server."""

  def __init__(
      self,
      address,
      admin_address=None,
      instance_name=None,
      table_name=None,
  ):
    """Initializes a KijiREST client.

    Args:
      address: host:port address of the KijiREST server.
      admin_address: Optional explicit admin address.
          Defaults to host:(port + 1).
      instance_name: Optional default Kiji instance name.
      table_name: Optional default Kiji table name.
    """
    (host, port) = address.split(':')
    port = int(port)
    self._address = address

    if admin_address is None:
      admin_address = '%s:%d' % (host, port + 1)
    self._admin_address = admin_address

    self._instance_name = instance_name
    self._table_name = table_name
    self._url_base = 'http://%s/v1' % self._address

  @property
  def address(self):
    """Returns: 'host:port' address of the KijiREST server, as a string."""
    return self._address

  @property
  def admin_address(self):
    """Returns: 'host:port' admin address of the KijiREST server, as a string."""
    return self._admin_address

  def Request(
      self,
      path,
      query=None,
      data=None,
      method=HttpMethod.GET,
      raw=False,
  ):
    """Performs an HTTP request.

    Args:
      path: Path of the request to append to the URL base.
      query: Optional URL query parameters (dictionary).
      data: Optional request data (body).
      method: HTTP request method.
      raw: When true, the default, returns the HTTP reply as text.
          When false, returns the raw HTTP reply object.
    Returns:
      The HTTP reply (decoded text if raw=false, or HTTP reply object).
    """
    if query is None:
      query = dict()
    url = '%(url_base)s/%(path)s?%(query)s' % dict(
        url_base = self._url_base,
        path = path,
        query = '&'.join(map(lambda kv: '%s=%s' % kv, query.items())),
    )

    http_req = urllib.request.Request(url=url, data=data)
    http_req.add_header('Accept', ContentType.JSON)
    http_req.add_header('Content-Type', ContentType.JSON)
    if data is not None:
      http_req.add_header('Content-Length', len(data))
    http_req.get_method = lambda: method
    logging.debug(
        'Sending HTTP %s request: %s with headers %s and data %r',
        method, http_req.full_url, http_req.header_items(), data)

    http_reply = urllib.request.urlopen(http_req)
    assert (http_reply.getcode() == http.client.OK), \
        ('HTTP reply with code %d' % http_reply.getcode())
    if raw:
      return http_reply
    else:
      text_reply = http_reply.readall().decode()
      return text_reply

  def ListInstances(self):
    text_reply = self.Request(path='instances')
    return sorted(map(lambda entry: entry['name'], base.JsonDecode(text_reply)))

  def ListTables(self, instance):
    """Lists the names of the Kiji tables that exist in a Kiji instance.

    Args:
      instance: Name of the Kiji instance to list the table of.
    Returns:
      Sorted list of Kiji table names.
    """
    text_reply = self.Request(
        path=os.path.join('instances', instance, 'tables'),
        method=HttpMethod.GET,
    )
    return sorted(map(lambda entry: entry['name'], base.JsonDecode(text_reply)))

  def Get(
      self,
      entity_id,
      columns=None,
      data=None,
      query=None,
      instance=None,
      table=None,
  ):
    """Retrieves a single row of data.

    Args:
      entity_id: ID of the row entity to query.
      columns: Optional list of columns to request.
          Default is to request all columns.
      data: Optional HTTP request body (data).
      query: Optional extra HTTP URL query parameters, as a Python dict.
      instance: Optional explicit Kiji instance name.
      table: Optional explicit Kiji table name.
    Returns:
      Row content, as a Python value (decoded from JSON).
    """
    if instance is None:
      instance = self._instance_name
    assert (instance is not None), 'No Kiji instance specified.'

    if table is None:
      table = self._table_name
    assert (table is not None), 'No Kiji table specified.'

    path = os.path.join('instances', instance, 'tables', table, 'rows')
    if query is None:
      query = dict()
    query['eid'] = base.JsonEncode(_RestEntityId(entity_id), pretty=False)
    if (columns is not None) and (len(columns) > 0):
      query['cols'] = ','.join(columns)
    text_reply = self.Request(
        path=path,
        query=query,
        data=data,
        method=HttpMethod.GET,
    )
    return base.JsonDecode(text_reply)

  def Scan(
      self,
      start_eid=None,
      end_eid=None,
      columns=None,
      data=None,
      query=None,
      instance=None,
      table=None,
      max_rows=3,
  ):
    """Retrieves a range of rows.

    Args:
      start_eid: ID of the entity to scan from (included).
          Default is to scan from the first available row.
      end_eid: ID of the entity to scan to (excluded).
          Default is to scan to the last row in the table.
      columns: Optional list of columns to request.
          Default is to request all columns.
      data: Optional HTTP request body (data).
      query: Optional extra HTTP URL query parameters, as a Python dict.
      instance: Optional explicit Kiji instance name.
      table: Optional explicit Kiji table name.
      max_rows: Optional maximum number of rows to return.
          None means return all rows.
          Default is to return 3 rows.
    Yields:
      Row content, as a Python value (decoded from JSON).
    """
    if instance is None:
      instance = self._instance_name
    assert (instance is not None), 'No Kiji instance specified.'

    if table is None:
      table = self._table_name
    assert (table is not None), 'No Kiji table specified.'

    path = os.path.join('instances', instance, 'tables', table, 'rows')
    if query is None:
      query = dict()

    if start_eid is not None:
      query['start_eid'] = \
          base.JsonEncode(_RestEntityId(start_eid), pretty=False)

    if end_eid is not None:
      query['end_eid'] = \
          base.JsonEncode(_RestEntityId(end_eid), pretty=False)

    if max_rows is None:
      query['limit'] = -1
    else:
      query['limit'] = max_rows

    if (columns is not None) and (len(columns) > 0):
      query['cols'] = ','.join(columns)
    http_reply = self.Request(
        path=path,
        query=query,
        data=data,
        method=HttpMethod.GET,
        raw=True,
    )
    while True:
      line = http_reply.readline()
      if (line is not None) and (len(line) > 0):
        yield base.JsonDecode(line.decode())
      else:
        break

  def Put(
      self,
      entity_id,
      family,
      qualifier,
      value,
      schema,
      timestamp=None,
      instance=None,
      table=None,
  ):
    """Writes a single cell into a row.

    Args:
      entity_id: ID of the row entity to query.
      family: Name of the family to write.
      qualifier: Name of the column to write.
      value: Value of the cell to write, represented as a Python value
          for a JSON object.
      schema: Avro schema of the value to write, represented as a
          Python value describing an Avro JSON schema.
      instance: Optional explicit Kiji instance name.
      table: Optional explicit Kiji table name.
    Returns:
      Row content, as a Python value (decoded from JSON).
    """
    if instance is None:
      instance = self._instance_name
    assert (instance is not None), 'No Kiji instance specified.'

    if table is None:
      table = self._table_name
    assert (table is not None), 'No Kiji table specified.'

    path = os.path.join('instances', instance, 'tables', table, 'rows')
    data = dict(
        entityId = _RestEntityId(entity_id),
        cells = {
            family: {
                qualifier: [dict(
                    timestamp = timestamp,
                    value = value,
                    writer_schema = schema,
                )]
            }
        }
    )
    text_reply = self.Request(
        path=path,
        data=base.JsonEncode(data, pretty=False).encode(),
        method=HttpMethod.POST,
    )
    return base.JsonDecode(text_reply)

  def CloseInstance(self, instance):
    """Stop serving a Kiji instance and close all connections to it."""

    assert (instance is not None), 'No Kiji instance specified.'
    url = 'http://%s/tasks/close?instance=%s' % (self._admin_address, instance)
    method = HttpMethod.POST
    http_req = urllib.request.Request(url=url, method=method)
    http_req.get_method = lambda: method
    try:
      http_reply = urllib.request.urlopen(http_req)
      http_code = http_reply.getcode()
      assert (http_code == http.client.OK), \
          ('Close instance failed with HTTP code %d' % http_code)
      return True
    except urllib.error.URLError as err:
      logging.debug('Error closing instance: %r', err)
      return False
    except urllib.error.HTTPError as err:
      logging.debug('Error closing instance: %r\n%s', err, err.readlines())
      return False

  def CloseTable(self, instance, table):
    """Stop serving a Kiji table and close all connections to it."""

    assert (instance is not None), 'No Kiji instance specified.'
    assert (table is not None), 'No Kiji table specified.'
    url = 'http://%s/tasks/close?instance=%s&table=%s' % (self._admin_address, instance, table)
    method = HttpMethod.POST
    http_req = urllib.request.Request(url=url, method=method)
    http_req.get_method = lambda: method
    try:
      http_reply = urllib.request.urlopen(http_req)
      http_code = http_reply.getcode()
      assert (http_code == http.client.OK), \
          ('Close table failed with HTTP code %d' % http_code)
      return True
    except urllib.error.URLError as err:
      logging.debug('Error closing table: %r', err)
      return False
    except urllib.error.HTTPError as err:
      logging.debug('Error closing table: %r\n%s', err, err.readlines())
      return False

  def Ping(self):
    """Ping the REST server on the admin endpoint."""
    url = 'http://%s/ping' % self._admin_address
    method = HttpMethod.GET
    http_req = urllib.request.Request(url=url, method=method)
    http_req.get_method = lambda: method
    try:
      http_reply = urllib.request.urlopen(http_req)
      http_code = http_reply.getcode()
      assert (http_code == http.client.OK), \
          ('Ping failed with HTTP code %d' % http_code)
      text_reply = http_reply.readall().decode().strip()
      assert (text_reply == 'pong'), \
          ('Ping failed with reply %r' % text_reply)
      return True
    except urllib.error.URLError as err:
      logging.debug('Error pinging REST server: %r', err)
      return False
    except urllib.error.HTTPError as err:
      logging.debug('Error pinging REST server: %r\n%s', err, err.readlines())
      return False

  def GetMetrics(self):
    """Retrieves the metrics exposed by this REST server.

    Returns:
      Python value decoded from the KijiREST JSON metric record.
    """
    url =  'http://%s/metrics' % self.admin_address
    data = None
    http_req = urllib.request.Request(url=url, data=data)
    http_req.add_header('Accept', ContentType.JSON)
    http_req.add_header('Content-Type', ContentType.JSON)
    method = HttpMethod.GET
    http_req.get_method = lambda: method
    logging.debug(
        'Sending HTTP %s request: %s with headers %s and data %r',
        method, http_req.full_url, http_req.header_items(), data)

    http_reply = urllib.request.urlopen(http_req)
    assert (http_reply.getcode() == http.client.OK), \
        ('HTTP reply with code %d' % http_reply.getcode())
    text_reply = http_reply.readall().decode()
    return base.JsonDecode(text_reply)


# ------------------------------------------------------------------------------


class KijiRestServer(object):
  """Wrapper for a KijiRest server."""

  def __init__(
      self,
      kiji_rest_path,
      conf_dir,
      logs_dir,
      pid_file_path,
      jar_paths = tuple(),
      jvm_args = tuple(),
  ):
    """Initializes a wrapper for a KijiREST server.

    Args:
      kiji_rest_path: Path of the kiji-rest binary.
      conf_dir: Path of the conf/ directory.
      logs_dir: Path of the directory where to write logs.
      pid_file_path: Path of PID file.
    """
    # Path of the kiji-rest binary:
    self._kiji_rest_path = kiji_rest_path
    assert os.path.exists(self._kiji_rest_path), \
        ('kiji-rest binary not found in %r' % self._kiji_rest_path)

    # Path of the kiji-rest conf/ directory:
    self._conf_dir = conf_dir
    self._yml_conf_path = os.path.join(self._conf_dir, 'configuration.yml')
    assert os.path.exists(self._yml_conf_path), \
        ('Cannot find YML configuration for KijiREST: %r' % self._yml_conf_path)
    self._conf = self._ReadConf()

    # Path of the directory for KijiRest's logs:
    self._logs_dir = logs_dir

    # Path of the PID file:
    self._pid_file_path = pid_file_path

    # List of extra JARs to add to the classpath:
    self._jar_paths = tuple(jar_paths)

    # List of extra JVM arguments:
    self._jvm_args = tuple(jvm_args)

  @property
  def pid(self):
    """Returns: the PID file's content, or None."""
    if not os.path.exists(self._pid_file_path):
      return None

    with open(self._pid_file_path, 'rt') as f:
      pid = int(f.read())

    if not base.ProcessExists(pid):
      logging.debug('Removing stale PID file for KijiREST server.')
      os.remove(self._pid_file_path)
      pid = None

    return pid

  @property
  def conf(self):
    return self._conf

  def GetClient(self):
    """Returns: a new client for this KijiREST server."""
    return KijiRestClient(
        address='%s:%d' % self.address,
        admin_address='%s:%d' % self.admin_address,
    )

  def SetPort(self, port):
    """Configures the port for the main/data interface of this REST server."""
    self._conf['http']['port'] = int(port)

  def SetAdminPort(self, port):
    """Configures the port for the admin interface of this REST server."""
    self._conf['http']['adminPort'] = int(port)

  def SetHBaseCluster(self, cluster):
    """Configures the HBase cluster this REST server interacts with.

    Args:
      cluster: KijiURI of the HBase cluster.
    """
    assert cluster.startswith('kiji://'), \
        ('Invalid HBase cluster URI: %r' % cluster)
    self._conf['cluster'] = cluster

  def WriteConf(self):
    """Writes an updated configuration file."""
    # Note: I dislike to overwrite the existing configuration file.
    #     But cloning the config dir comes with other problems too...
    #
    # conf_dir = os.path.join(
    #     os.path.dirname(self._conf_dir),
    #     '%s.%s' % (os.path.basename(self._conf_dir), base.Timestamp()))
    # logging.info('Cloning configuration in %r', conf_dir)
    # shutil(src=self._conf_dir, dst=conf_dir)
    # self._conf_dir = conf_dir
    # self._yml_conf_path = os.path.join(self._conf_dir, 'configuration.yml')

    # Make a copy of the original configuration file:
    shutil.copy(
        src=self._yml_conf_path,
        dst='%s.bak.%s' % (self._yml_conf_path, base.Timestamp()))

    with open(self._yml_conf_path, 'wt') as f:
      f.write(yaml.dump(self._conf, default_flow_style=False))

  @property
  def address(self):
    """Returns: (host, port) for the main REST endpoint."""
    host = socket.getfqdn()
    port = int(self.conf['http']['port'])
    return (host, port)

  @property
  def admin_address(self):
    """Returns: (host, port) for the admin endpoint."""
    host = socket.getfqdn()
    port = int(self.conf['http']['adminPort'])
    return (host, port)

  @property
  def hbase_uri(self):
    """Returns: the HBase cluster URI the REST server connects to."""
    return self.conf['cluster']

  def _ReadConf(self):
    """Reads and parses the YML configuration file.

    Returns:
      The parsed YML configuration file, as a Python dictionary.
    """
    with open(self._yml_conf_path, 'r') as f:
      return yaml.load(f)

  def Start(self, timeout=10.0):
    """Starts the KijiREST server.

    Args:
      timeout: Timeout in seconds, while waiting for the process to start.
    Returns:
      True if the process is running, False otherwise.
    """
    pid = self.pid
    if pid is not None:
      logging.info('KijiREST already running as PID %d', pid)
      return

    base.MakeDir(self._logs_dir)
    base.MakeDir(os.path.dirname(self._pid_file_path))
    assert os.path.exists(self._conf_dir), self._conf_dir
    for jar_path in self._jar_paths:
      assert os.path.exists(jar_path), jar_path

    # Build environment for the KijiREST daemon:
    env = dict(os.environ)

    kiji_classpath = env.get('KIJI_CLASSPATH')
    if kiji_classpath is None:
      jar_paths = self._jar_paths
    else:
      jar_paths = list(self._jar_paths)
      jar_paths.extend(kiji_classpath.split(':'))

    logging.info(
        'Starting KijiREST server for HBase cluster %s on %s (admin %s).',
        self.hbase_uri,
        'http://%s:%d' % self.address,
        'http://%s:%d' % self.admin_address)

    env.update(
        KIJI_CLASSPATH=':'.join(jar_paths),
        KIJI_REST_CONF_DIR=self._conf_dir,
        KIJI_REST_JAVA_ARGUMENTS=' '.join(self._jvm_args),
        KIJI_REST_LOGS_DIR=self._logs_dir,
        PIDFILE=self._pid_file_path,
    )
    cmd = command.Command(
        args=[self._kiji_rest_path, 'start'],
        env=env,
    )

    logging.info('Waiting for the KijiREST process to start')
    deadline = time.time() + timeout
    while (self.pid is None) and (time.time() < deadline):
      sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(0.1)
    sys.stdout.write('\n')

    if self.pid is None:
      logging.error('KijiREST process not started')
      return False

    pid = self.pid
    logging.info('KijiREST started with PID %d', pid)

    client = self.GetClient()
    ping_success = client.Ping()
    while ((not ping_success)
           and (self.pid is not None)
           and (time.time() < deadline)):
      sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(0.1)
      ping_success = client.Ping()
    sys.stdout.write('\n')

    if ping_success:
      logging.info('KijiREST with PID %d : ping OK', pid)
      return True
    elif self.pid is None:
      logging.info('KijiREST with PID %d died', pid)
      return False
    else:
      logging.info('KijiREST with PID %d : ping not OK after %fs', pid, timeout)
      return False

  def Stop(self, sig=signal.SIGTERM, timeout=10.0):
    """Stops this KijiREST server.

    Args:
      sig: Optional signal to send to the server.
          By default, this sends a TERM signal.
      timeout: Timeout in seconds, while waiting for the process to stop.
    Returns:
      True if the KijiREST process is no longer running, False otherwise.
    """
    pid = self.pid
    if pid is None:
      logging.info('Cannot stop: no KijiREST process running.')
      return True

    logging.info('Sending signal %s to KijiREST with PID %d', sig, pid)
    os.kill(self.pid, sig)

    # Wait for the process to stop:
    deadline = time.time() + timeout
    while (self.pid is not None) and (time.time() < deadline):
      sys.stdout.write('.')
      sys.stdout.flush()
      time.sleep(0.1)
    sys.stdout.write('\n')

    if self.pid is None:
      logging.info('KijiREST with PID %d stopped.', pid)
      return True
    else:
      logging.error('KijiREST with PID %d is still running.', pid)
      return False


# ------------------------------------------------------------------------------


class RestAction(cli.Action):
  """Base class for CLI actions on a KijiRest client."""

  def __init__(self, parent, client):
    super(RestAction, self).__init__(parent_flags=parent.flags)
    self._client = client

  @property
  def client(self):
    return self._client


class Get(RestAction):
  """Performs a "get" on a Kiji table row."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        default='default',
        help='Name of the Kiji instance to interact with.',
    )
    self.flags.AddString(
        name='table',
        default=None,
        help='Name of the Kiji table to interact with.',
    )
    self.flags.AddString(
        name='eid',
        default=None,
        help=('ID of the row to get.\n'
              'Note: numbers are specified, as in --eid=123, '
              'but strings must be quoted: --eid=\'"string"\'.'),
    )
    self.flags.AddString(
        name='columns',
        default=None,
        help='Column(s) to request.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    assert (self.flags.table is not None), 'Specify --table=...'
    assert (self.flags.eid is not None), 'Specify --eid=...'
    entity_id = base.JsonDecode(self.flags.eid)
    logging.debug('Retrieving row with entity ID: %r', entity_id)

    if (self.flags.columns is not None) and (len(self.flags.columns) > 0):
      columns = self.flags.columns.split(',')
    else:
      columns = None

    json = self.client.Get(
        instance=self.flags.instance,
        table=self.flags.table,
        entity_id=entity_id,
        columns=columns,
    )
    print(base.JsonEncode(json))
    return ExitCode.SUCCESS


class Put(RestAction):
  """Performs a "put" on a Kiji table row."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        default='default',
        help='Name of the Kiji instance to interact with.',
    )
    self.flags.AddString(
        name='table',
        default=None,
        help='Name of the Kiji table to interact with.',
    )
    self.flags.AddString(
        name='eid',
        default=None,
        help=('ID of the row to get.\n'
              'Note: numbers are specified, as in --eid=123, '
              'but strings must be quoted: --eid=\'"string"\'.'),
    )
    self.flags.AddString(
        name='column',
        default=None,
        help='Column (family:qualifier) to write to.',
    )
    self.flags.AddString(
        name='schema',
        default=None,
        help='Avro schema of the value to write.',
    )
    self.flags.AddString(
        name='value',
        default=None,
        help='Value of the cell to write.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    assert (self.flags.table is not None), 'Specify --table=...'
    assert (self.flags.eid is not None), 'Specify --eid=...'
    assert (self.flags.column is not None), 'Specify --column=family:qualifier'
    assert (':' in self.flags.column), 'Specify --column=family:qualifier'
    assert (self.flags.value is not None), 'Specify --value=...'
    assert (self.flags.schema is not None), 'Specify --schema=...'

    entity_id = base.JsonDecode(self.flags.eid)
    logging.debug('Writing cell to row with entity ID: %r', entity_id)

    (family, qualifier) = self.flags.column.split(':', 1)

    value = base.JsonDecode(self.flags.value)
    schema = base.JsonDecode(self.flags.schema)

    json = self.client.Put(
        instance=self.flags.instance,
        table=self.flags.table,
        entity_id=entity_id,
        family=family,
        qualifier=qualifier,
        value=value,
        schema=schema,
    )
    logging.debug('JSON result for put: %r', json)
    return ExitCode.SUCCESS



class Scan(RestAction):
  """Performs a "scan" on a Kiji table."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        default='default',
        help='Name of the Kiji instance to interact with.',
    )
    self.flags.AddString(
        name='table',
        default=None,
        help='Name of the Kiji table to interact with.',
    )
    self.flags.AddString(
        name='start_eid',
        default=None,
        help=('ID of the row to scan from (included).\n'
              'Note: numbers are specified, as in --start-eid=123, '
              'but strings must be quoted: --start-eid=\'"string"\'.'),
    )
    self.flags.AddString(
        name='end_eid',
        default=None,
        help=('ID of the row to scan to (excluded).\n'
              'Note: numbers are specified, as in --end-eid=123, '
              'but strings must be quoted: --end-eid=\'"string"\'.'),
    )
    self.flags.AddString(
        name='columns',
        default=None,
        help='Column(s) to request.',
    )
    self.flags.AddInteger(
        name='max_rows',
        default=3,
        help=('Maximum number of rows to scan. '
              'A negative number means no maximum, ie. read all rows.'),
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    assert (self.flags.table is not None), 'Specify --table=...'

    if self.flags.start_eid is not None:
      start_eid = base.JsonDecode(self.flags.start_eid)
    else:
      start_eid = None

    if self.flags.end_eid is not None:
      end_eid = base.JsonDecode(self.flags.end_eid)
    else:
      end_eid = None

    logging.debug(
        'Scanning rows from entity ID: %r to entity ID: %r',
        start_eid, end_eid)

    if (self.flags.columns is not None) and (len(self.flags.columns) > 0):
      columns = self.flags.columns.split(',')
    else:
      columns = None

    max_rows = self.flags.max_rows
    if max_rows < 0:
      max_rows = None

    for row in self.client.Scan(
        instance=self.flags.instance,
        table=self.flags.table,
        start_eid=start_eid,
        end_eid=end_eid,
        columns=columns,
        max_rows=max_rows,
    ):
      print('-' * 80)
      print(base.JsonEncode(row))
    return ExitCode.SUCCESS


class RestPing(RestAction):
  """Tests the REST server's health via a ping request."""

  NAME = 'ping'

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    if self.client.Ping():
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILURE')
      return ExitCode.FAILURE

class CloseInstance(RestAction):
  """Close all REST connections to a Kiji instance."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        help='Name of the Kiji instance whose connection to close.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    if self.client.CloseInstance(instance = self.flags.instance):
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILURE')
      return ExitCode.FAILURE

class CloseTable(RestAction):
  """Close all REST connections to a Kiji table."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        help='Name of the Kiji instance containing the table whose connection to close.',
    )
    self.flags.AddString(
        name='table',
        help='Name of the Kiji table whose connection to close.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    if self.client.CloseTable(instance = self.flags.instance, table = self.flags.table):
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILURE')
      return ExitCode.FAILURE

class ListInstances(RestAction):
  """Lists the Kiji instances accessible through the REST server."""

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    print(self.client.ListInstances())
    return ExitCode.SUCCESS


class ListTables(RestAction):
  """Lists the table in a Kiji instance."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='instance',
        default=None,
        help='Name of the Kiji instance to interact with.',
    )

  def Run(self, args):
    instance = self.flags.instance
    if (instance is None) and (len(args) > 0):
      instance, args = args[0], args[1:]
    assert (instance is not None), 'Specific Kiji instance with --instance=...'
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    print(self.client.ListTables(instance=instance))
    return ExitCode.SUCCESS


class KijiRestClientCLI(cli.Action):
  """CLI interface to the KijiREST client."""

  def __init__(self, **kwargs):
    super(KijiRestClientCLI, self).__init__(
        help_flag=cli.HelpFlag.ADD_NO_HANDLE,
        **kwargs
    )

  @classmethod
  def GetActionMap(cls):
    if not hasattr(cls, '_ACTIONS'):
      action_map = dict(
          (action_class.GetName(), action_class)
          for action_class in RestAction.__subclasses__()
      )
      cls._ACTIONS = action_map
    return cls._ACTIONS

  def RegisterFlags(self):
    self.flags.AddString(
        name='server',
        default='localhost:8000',
        help='Address of the KijiREST server to connect to.',
    )
    self.flags.AddString(
        name='do',
        default=None,
        help=('Action to perform: %s.' % ', '.join(sorted(self.GetActionMap()))),
    )

  def Run(self, args):
    try:
      return self._RunInternal(args)
    except urllib.error.HTTPError as err:
      # Pretty-print the JSON error trace, if possible:
      error_text = err.read().decode()
      if err.headers.get_content_type() == ContentType.JSON:
        json = base.JsonDecode(error_text)
        logging.error(
            'Error during HTTP request: %s\n%s',
            err, base.JsonEncode(json))
        if 'trace' in json:
          logging.error('Trace:\n%s', json['trace'])
      else:
        logging.error(
            'Error during HTTP request: %s\n%s',
            err, error_text)
      return ExitCode.FAILURE

  def _RunInternal(self, args):
    if (self.flags.do is None) and (len(args) > 0):
      self.flags.do, args = args[0], args[1:]

    client = KijiRestClient(address=self.flags.server)

    action_class = self.GetActionMap().get(self.flags.do)
    if action_class is None:
      if self.flags.help:
        self.flags.PrintUsage()
        return ExitCode.SUCCESS
      else:
        logging.error(
            'Invalid action, must use one of: %s.',
            ', '.join(sorted(self.GetActionMap())))
        return ExitCode.FAILURE
    else:
      action = action_class(parent=self, client=client)
      return action(args)


# ------------------------------------------------------------------------------


class ServerAction(cli.Action):
  def __init__(self, parent, server, **kwargs):
    super(ServerAction, self).__init__(parent_flags=parent.flags, **kwargs)
    self._server = server

  @property
  def server(self):
    return self._server


class Status(ServerAction):
  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    pid = self.server.pid
    if pid is None:
      logging.info('No KijiREST server running')
    else:
      logging.info(
          'KijiREST running as PID %d:\n'
          ' - Listening on %s\n'
          ' - Admin URL: %s\n'
          ' - Connected to cluster: %s\n'
          ' - Kiji instances: %s',
          pid,
          'http://%s:%d' % self.server.address,
          'http://%s:%d' % self.server.admin_address,
          self.server.conf['cluster'],
          self.server.conf.get('instances', 'all'),
      )


class Start(ServerAction):
  def RegisterFlags(self):
    self.flags.AddInteger(
        name='port',
        default=None,
        help='Override the port for the HTTP interface.',
    )
    self.flags.AddInteger(
        name='admin_port',
        default=None,
        help='Override the port for the admin HTTP interface.',
    )
    self.flags.AddString(
        name='hbase_uri',
        default=None,
        help='Override the HBase cluster to connect to, specified as a Kiji URI.',
    )
    self.flags.AddFloat(
        name='timeout',
        default=10.0,
        help='How many seconds to wait for the process to start.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    dirty = False
    if self.flags.port is not None:
      self.server.SetPort(self.flags.port)
      dirty = True
    if self.flags.admin_port is not None:
      self.server.SetAdminPort(self.flags.admin_port)
      dirty = True
    if self.flags.hbase_uri is not None:
      self.server.SetHBaseCluster(self.flags.hbase_uri)
      dirty = True
    if dirty:
      self.server.WriteConf()

    if self.server.Start(timeout=self.flags.timeout):
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILURE')
      return ExitCode.FAILURE


class Stop(ServerAction):
  def RegisterFlags(self):
    self.flags.AddString(
        name='signal',
        default='term',
        help='Signal to send to the KijiREST process.',
    )
    self.flags.AddFloat(
        name='timeout',
        default=10.0,
        help='How many seconds to wait for the process to stop.',
    )
    self.flags.AddBoolean(
        name='kill_on_timeout',
        default=True,
        help='Whether to force kill on timeout.',
    )

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    signal_name = ('sig%s' % self.flags.signal).upper()
    sig = getattr(signal, signal_name, None)
    assert (sig is not None), ('Invalid signal: %r' % self.flags.signal)

    if self.server.Stop(sig=sig, timeout=self.flags.timeout):
      print('OK')
      return ExitCode.SUCCESS

    if self.flags.kill_on_timeout:
      logging.info(
          'Forcibly stopping KijiREST server with PID %d',
          self.server.pid)
    if self.server.Stop(sig=signal.SIGKILL, timeout=1.0):
      print('OK')
      return ExitCode.SUCCESS

    print('FAILED')
    return ExitCode.FAILURE

class ServerPing(ServerAction):
  NAME = 'ping'

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    assert (len(args) == 0), ('Extra CLI args: %r' % args)
    if self.server.GetClient().Ping():
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILED')
      return ExitCode.FAILURE

class ServerPing(ServerAction):
  NAME = 'ping'

  def Run(self, args):
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    assert (len(args) == 0), ('Extra CLI args: %r' % args)
    if self.server.GetClient().Ping():
      print('OK')
      return ExitCode.SUCCESS
    else:
      print('FAILED')
      return ExitCode.FAILURE


class KijiRestServerCLI(cli.Action):
  """CLI interface to a KijiREST server."""

  def __init__(self, rest_server, **kwargs):
    super(KijiRestServerCLI, self).__init__(
        help_flag=cli.HelpFlag.ADD_NO_HANDLE,
        **kwargs
    )
    self._rest_server = rest_server

  @classmethod
  def GetActionMap(cls):
    if not hasattr(cls, '_ACTIONS'):
      action_map = dict(
          (action_class.GetName(), action_class)
          for action_class in ServerAction.__subclasses__()
      )
      cls._ACTIONS = action_map
    return cls._ACTIONS

  def RegisterFlags(self):
    self.flags.AddString(
        name='do',
        default=None,
        help=('Action to perform: %s.' % ', '.join(sorted(self.GetActionMap()))),
    )

  def Run(self, args):
    if (self.flags.do is None) and (len(args) > 0):
      self.flags.do, args = args[0], args[1:]

    rest = self._rest_server

    action_class = self.GetActionMap().get(self.flags.do)
    if action_class is None:
      if self.flags.help:
        self.flags.PrintUsage()
        return ExitCode.SUCCESS
      else:
        logging.error(
            'Invalid action, must use one of: %s.',
            ', '.join(sorted(self.GetActionMap())))
        return ExitCode.FAILURE
    else:
      action = action_class(parent=self, server=rest)
      return action(args)


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Error('Not a standalone module')
