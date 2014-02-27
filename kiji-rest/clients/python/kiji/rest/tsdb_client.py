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


LogLevel = base.LogLevel


class Error(Exception):
  """Errors raised in this module."""
  pass


# ------------------------------------------------------------------------------


class TSDB(object):
  """Client for a TSDB instance."""

  def __init__(self, address):
    """Creates a new TSDB client.

    Args:
      address: 'host:port' address of the TSDB instance to connect to.
    """
    self._address = address

    (host, port) = address.split(':')
    port = int(port)
    self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._socket.connect((host, port))

  @property
  def address(self):
    """Returns: the 'host:port' address this TSDB client connects to."""
    return self._address

  def Put(
      self,
      metric,
      value,
      timestamp=None,
      **kwargs
  ):
    """Writes a new sample into the TSDB this client is connected to.

    Args:
      metric: Metric name ([A-Za-z0-9_.]+).
      value: Numeric value to write.
      timestamp: Optional timestamp (number of seconds since Epoch).
          Defaults to now.
      **kwargs: Optional extra tags for the sample to write.
    """
    assert (len(kwargs) >= 1)
    if timestamp is None:
      timestamp = time.time()

    command = 'put %(metric)s %(timestamp)s %(value)s %(tags)s\n' % dict(
        metric=metric,
        timestamp=int(timestamp),  # OpenTSDB doesn't support ms precision
        value=value,
        tags=' '.join(map(lambda kv: '%s=%s' % kv, kwargs.items())),
    )
    logging.log(LogLevel.DEBUG_VERBOSE, 'Sending command: %r', command)
    self._socket.send(command.encode())

  def Close(self):
    """Close this TSDB client."""
    self._socket.close()


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Error('Not a standalone module')
