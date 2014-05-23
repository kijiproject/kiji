#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

import collections
import http.client
import logging
import socket
import time
import traceback
import urllib.request

from base import base
from base import cli

from kiji.rest import kiji_rest
from kiji.rest import tsdb_client


LogLevel = base.LogLevel
HttpMethod = base.HttpMethod
ContentType = base.ContentType
ExitCode = base.ExitCode


class Error(Exception):
  """Errors raised in this module."""
  pass


# ------------------------------------------------------------------------------


def ListMetrics(values, path=None):
  """Lists the metrics that can be populated from a KijiREST metrics report.

  Args:
    values: KijiREST metrics report (JSON decoded object).
    path: metric name prefix.
  Yield:
    OpenTSDB metric name.
  """
  for key, value in values.items():
    vpath = key if path is None else '%s.%s' % (path, key)
    if isinstance(value, collections.Iterable) and not isinstance(value, str):
      yield from ListMetrics(values=value, path=vpath)
    elif isinstance(value, (int, float)):
      yield vpath
    else:
      logging.log(
          LogLevel.DEBUG_VERBOSE, 'Not a numeric metric: %r = %r', vpath, value)


def Push(tsdb, hostname, values, path=None, **kwargs):
  """Recursively converts a Python value (nested dicts) into a set of TSDB puts.

  Args:
    tsdb: TSDB client to use.
    hostname: Name of the host to report metrics for.
    values: Python value to convert (nested dicts).
    path: Optional path prefix for the metrics to write.
    **kwargs: Optional set of tags to set on the metrics.
  """
  for key, value in values.items():
    vpath = key if path is None else '%s.%s' % (path, key)

    if isinstance(value, collections.Iterable) and not isinstance(value, str):
      Push(
          tsdb=tsdb,
          hostname=hostname,
          values=value,
          path=vpath,
          **kwargs
      )
    elif isinstance(value, (int, float)):
      tsdb.Put(metric=vpath, value=value, host=hostname, **kwargs)
    else:
      logging.log(
          LogLevel.DEBUG_VERBOSE, 'Not a numeric metric: %r = %r', vpath, value)


# ------------------------------------------------------------------------------


class TSDBCollector(cli.Action):
  """TSDB collector that pushes KijiREST metrics."""

  def RegisterFlags(self):
    self.flags.AddString(
        name='tsdb_address',
        default='localhost:4242',
        help='host:port address of the TSDB server.',
    )

    self.flags.AddFloat(
        name='interval_secs',
        default=10.0,
        help='Time interval between samples, in seconds.',
    )

    self.flags.AddString(
        name='kiji_rest_address',
        default='localhost:8000',
        help='host:port address of the KijiREST main interface.',
    )
    self.flags.AddString(
        name='kiji_rest_admin_address',
        default=None,
        help=('host:port address of the KijiREST admin interface.\n'
              'Default is to use the main interface address (port + 1).'),
    )

    self.flags.AddString(
        name='tsdb_metric_hostname',
        default=None,
        help=('hostname to use when writing samples to TSDB.\n'
              'None or empty means use localhost fully-qualified name.'),
    )

    self.flags.AddString(
        name='tags',
        default=None,
        help=('Comma-separated list of extra tags (key=value) '
              'to put on the reported metrics.'),
    )

  @property
  def hostname(self):
    """Returns: the hostname of the KijiREST server to report metrics of."""
    return self._hostname

  def Run(self, args):
    assert (len(args) == 0), ('Extra arguments: %r' % args)

    if self.flags.kiji_rest_admin_address is None:
      (host, port) = self.flags.kiji_rest_address.split(':')
      port = int(port)
      self.flags.kiji_rest_admin_address = '%s:%d' % (host, port + 1)

    hostname = self.flags.tsdb_metric_hostname
    if (hostname is None) or (len(hostname) == 0):
      hostname = socket.getfqdn()
    self._hostname = hostname

    self._tags = {}

    tags = self.flags.tags
    if (tags is not None) and (len(tags) > 0):
      for tag in tags.split(','):
        key, value = tag.split('=')
        key = key.strip()
        value = value.strip()
        self._tags[key] = value

    logging.info('Using extra tags: %r', self._tags)

    self._rest_client = kiji_rest.KijiRestClient(
      address = self.flags.kiji_rest_address,
      admin_address = self.flags.kiji_rest_admin_address,
    )

    while True:
      logging.info(
          'Starting KijiREST metrics collection loop for host %s',
          self.hostname)
      self._CollectLoop()

  def _CollectLoop(self):
    """Internal loop that collects KijiREST metrics and pushes them to TSDB."""
    self._tsdb = tsdb_client.TSDB(self.flags.tsdb_address)
    try:
      while True:
        timestamp = int(time.time())
        try:
          self._CollectAndPush(timestamp)
        except ConnectionResetError:
          logging.error(
              'Error while pushing KijiREST metrics for hostname=%s timestamp=%s',
              self.hostname, timestamp)
          traceback.print_exc()
          return  # Exit the loop to close the TSDB and re-open another one

        except:
          logging.error(
              'Error while pushing KijiREST metrics for hostname=%s timestamp=%s',
              self.hostname, timestamp)
          traceback.print_exc()
          raise  # Die

        time.sleep(self.flags.interval_secs)
    finally:
      self._tsdb.Close()

  def _CollectAndPush(self, timestamp):
    """Collects and pushes one monitoring sample.

    Args:
      timestamp: Timestamp of the sample.
    """
    logging.debug(
        'Collecting metrics from KijiREST server %r (admin %r)',
        self._rest_client.address, self._rest_client.admin_address)
    metrics = self._rest_client.GetMetrics()
    logging.debug('Pushing KijiREST metrics: %r', metrics)
    Push(
        tsdb = self._tsdb,
        hostname = self.hostname,
        timestamp = timestamp,
        values = metrics,
        **self._tags
    )


# ------------------------------------------------------------------------------


def Main(args):
  collector_cli = TSDBCollector(parent_flags=base.FLAGS)
  return collector_cli(args)


if __name__ == '__main__':
  base.Run(Main)
