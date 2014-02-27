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


def Push(tsdb, hostname, values, path=None, mkmetric=False, **kwargs):
  """Recursively converts a Python value (nested dicts) into a set of TSDB puts.

  Args:
    tsdb: TSDB client to use.
    hostname: Name of the host to report metrics for.
    values: Python value to convert (nested dicts).
    path: Optional path prefix for the metrics to write.
    mkmetric: When set, lists the metrics instead of sending puts to TSDB.
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
          mkmetric=mkmetric,
          **kwargs
      )
    elif isinstance(value, (int, float)):
      if mkmetric:
        yield vpath
      else:
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

    while True:
      logging.info(
          'Starting KijiREST metrics collection loop for host %s',
          self.hostname)
      self._CollectLoop()

  def _CollectLoop(self):
    """Internal loop that collects KijiREST metrics and pushes them to TSDB."""
    rest_client = kiji_rest.KijiRestClient(
      address = self.flags.kiji_rest_address,
      admin_address = self.flags.kiji_rest_admin_address,
    )
    tsdb = tsdb_client.TSDB(self.flags.tsdb_address)
    try:
      while True:
        timestamp = int(time.time())
        try:
          logging.debug(
              'Pushing KijiREST metrics for hostname=%s timestamp=%s',
              self.hostname, timestamp)
          Push(
              tsdb = tsdb,
              hostname = self.hostname,
              timestamp = timestamp,
              values = rest_client.GetMetrics(),
          )
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
      tsdb.Close()


# ------------------------------------------------------------------------------


def Main(args):
  collector_cli = TSDBCollector(parent_flags=base.FLAGS)
  return collector_cli(args)


if __name__ == '__main__':
  base.Run(Main)
