#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Manages a Bento cluster.

Wraps an external Bento installation.
"""

import logging
import os
import re
import signal
import subprocess
import sys
import xml.etree.ElementTree as etree


class Error(Exception):
  """Errors in this module."""
  pass


def Find(root, regex):
  """Finds files whose name match a given regexp.

  Equivalent of 'find <root> -name <regex>'

  Args:
    root: Base directory to scan for files.
    regex: Match file names against this regexp.
  Yields:
    Paths of the files matching the regexp.
  """
  assert os.path.exists(root), root
  pattern = re.compile(regex)
  for dir_path, dir_names, file_names in os.walk(root):
    for file_name in file_names:
      if pattern.match(file_name):
        yield os.path.join(dir_path, file_name)


class BentoCluster(object):
  """Bento cluster.

  Wraps a Bento cluster installation.
  """

  def __init__(self, home, enable_log=True):
    """Initializes the Bento cluster object.

    Args:
      home: Bento install directory.
      enable_log: True means capture the Bento logs in the file.
    """
    self._home = home
    self._enable_log = enable_log
    assert os.path.exists(self._home), self._home
    assert os.path.exists(os.path.join(self._home, 'bin', 'bento')), (
      'Invalid Bento home dir: %s' % self._home)
    self._pid = None
    self._pid_file = os.path.join(self._home, 'state', 'bento-cluster.pid')
    self._checkin_pid_file = (
      os.path.join(self._home, 'state', 'checkin-daemon.pid'))

    self._hdfs_address = None
    self._zk_address = None
    self._mapred_address = None

  def Start(self):
    """Starts the Bento cluster, if necessary."""
    if os.path.exists(self._pid_file):
      with open(self._pid_file, 'r') as f:
        pid = int(f.read())
      if os.path.exists('/proc/%d' % pid):
        logging.info('Bento cluster already started as PID=%d', pid)
        self._pid = pid
      else:
        # Stale PID file, remove and start a new Bento:
        os.remove(self._pid_file)

    if self._pid is None:
      # No PID file, start a Bento:

      env = dict(os.environ)
      if self._enable_log:
        env['BENTO_LOG_ENABLE'] = '1'
      else:
        if 'BENTO_LOG_ENABLE' in env:
          del env['BENTO_LOG_ENABLE']

      with open('/dev/null', 'r') as input_fd:
        proc = subprocess.Popen(
            args=['bin/bento', 'start'],
            stdin=input_fd,
            env=env,
            cwd=self._home
        )
        proc.communicate()

      assert (proc.returncode == 0), ('bento start returned %d' % proc.returncode)
      with open(self._pid_file, 'r') as f:
        self._pid = int(f.read())
        logging.info('Bento cluster created and starter as PID=%d', self._pid)

    self._hdfs_address = self._GetHDFSAddress()
    self._zk_address = self._GetZooKeeperAddress()
    self._mapred_address = self._GetMapReduceAddress()
    logging.info('HBase URI: %s', self.hbase_uri)
    logging.info('MapReduce address: %s', self.mapreduce_address)
    logging.info('HDFS URI: %s', self.hdfs_address)

  def _GetZooKeeperAddress(self):
    """Returns: the ZooKeeper address for this Bento cluster."""
    config = etree.fromstring(self.GetHBaseConfig())
    for prop in config.iterfind('property'):
      if prop.findtext('name') == 'hbase.zookeeper.property.clientPort':
        port = int(prop.findtext('value'))
        return 'localhost:%d' % port
    raise Error('Cannot determine ZooKeeper address')

  def _GetHDFSAddress(self):
    """Returns: the HDFS address for this Bento cluster."""
    config = etree.fromstring(self.GetCoreConfig())
    for prop in config.iterfind('property'):
      if prop.findtext('name') == 'fs.defaultFS':
        return prop.findtext('value')
    raise Error('Cannot determine HDFS address')

  def _GetMapReduceAddress(self):
    """Returns: the Map/Reduce job tracker address for this Bento cluster."""
    config = etree.fromstring(self.GetMapRedConfig())
    for prop in config.iterfind('property'):
      if prop.findtext('name') == 'mapred.job.tracker':
        return prop.findtext('value')
    raise Error('Cannot determine Map/Reduce job tracker address')

  def Stop(self):
    """Stops the running Bento cluster, if necessary."""
    if self._pid == None:
      logging.info('Bento cluster not started, nothing to stop.')
      return
    logging.info('Killing Bento cluster running as PID=%d', self._pid)
    try:
      os.kill(self._pid, signal.SIGKILL)
    except OSError as err:
      logging.debug('Could not kill process with PID=%d: %r', self._pid, err)
    if os.path.exists(self._pid_file):
      os.remove(self._pid_file)
    self._pid = None

    # Cleanup the checkin daemon too
    if os.path.exists(self._checkin_pid_file):
      with open(self._checkin_pid_file, 'r') as f:
        pid = int(f.read())
      logging.debug('Killing Bento check-in daemon running as PID=%d', pid)
      try:
        os.kill(pid, signal.SIGKILL)
      except OSError as err:
        logging.debug('Could not kill process with PID=%d: %r', pid, err)
      os.remove(self._checkin_pid_file)

  def _GetConfigFile(self, file_name):
    files = tuple(Find(self._home, file_name))
    assert len(files) == 1, files
    with open(files[0], 'r') as f:
      return f.read()

  def GetHBaseConfig(self):
    return self._GetConfigFile(r'bento-hbase-site\.xml')

  def GetHDFSConfig(self):
    return self._GetConfigFile(r'bento-hdfs-site\.xml')

  def GetMapRedConfig(self):
    return self._GetConfigFile(r'bento-mapred-site\.xml')

  def GetCoreConfig(self):
    return self._GetConfigFile(r'bento-core-site\.xml')

  def WriteConf(self, dir):
    """Writes configuration files into the specified directory.

    Args:
      dir: Where to write the configuration files for the Bento cluster.
    """
    with open(os.path.join(dir, 'hbase-site.xml'), 'w') as f:
      f.write(self.GetHBaseConfig())
    with open(os.path.join(dir, 'mapred-site.xml'), 'w') as f:
      f.write(self.GetMapRedConfig())
    with open(os.path.join(dir, 'hdfs-site.xml'), 'w') as f:
      f.write(self.GetHDFSConfig())
    with open(os.path.join(dir, 'core-site.xml'), 'w') as f:
      f.write(self.GetCoreConfig())

  @property
  def zookeeper_address(self):
    """Address of the ZooKeeper cluster, eg. 'localhost:2181'."""
    return self._zk_address

  @property
  def hdfs_address(self):
    """Address of the HDFS cluster, eg. 'hdfs://localhost:8020/'."""
    return self._hdfs_address

  @property
  def mapreduce_address(self):
    """Address of the MapReduce cluster, eg. 'localhost:8021'."""
    return self._zk_address

  @property
  def hbase_uri(self):
    return 'kiji://%s' % self.zookeeper_address


if __name__ == '__main__':
  raise Error('%r cannot be used as a standalone script.' % args[0])
