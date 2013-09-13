#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Manages a Bento cluster."""

import logging
import os
import re
import signal
import subprocess
import sys
import xml.etree.ElementTree as etree

from base import base
from base import cli

from kiji import maven_repo


class Error(Exception):
  """Errors in this module."""
  pass


# ------------------------------------------------------------------------------


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


# ------------------------------------------------------------------------------


def ExtractArchive(archive, work_dir, strip_components=0):
  """Extracts a tar archive.

  Args:
    archive: Path to the tar archive to extract.
    work_dir: Where to extract the archive.
    strip_components: How many leading path components to strip.
  """
  assert os.path.exists(archive), (
      'Archive %r does not exist', archive)
  if not os.path.exists(work_dir):
    os.makedirs(work_dir)
  command = (
      '/bin/tar xf {archive}'
      ' --directory {dir}'
      ' --strip-components={strip_components}'
  ).format(
      archive=archive,
      dir=work_dir,
      strip_components=strip_components,
  )
  logging.info('Running command: %r', command)
  os.system(command)


# ------------------------------------------------------------------------------


class BentoCluster(object):
  """Bento cluster.

  Wraps a Bento cluster installation.
  """

  def __init__(self, path, version, enable_log=True):
    """Initializes the Bento cluster object.

    Args:
      path: Path of the Bento install directory.
      version: Bento version, eg. '1.0.1' or '1.0.2-SNAPSHOT'.
      enable_log: True means capture the Bento logs in the file.
    """
    self._path = path
    self._enable_log = enable_log

    if not os.path.exists(os.path.join(self.path, 'bin', 'bento')):
      self._Fetch(version=version)

    self._pid_file = os.path.join(self.path, 'state', 'bento-cluster.pid')
    self._pid = self._ReadPidFile(self._pid_file)

    self._checkin_pid_file = (
        os.path.join(self.path, 'state', 'checkin-daemon.pid'))

    self._hdfs_address = None
    self._zk_address = None
    self._mapred_address = None

  @property
  def path(self):
    return self._path

  @property
  def is_running(self):
    return self.pid is not None

  @property
  def pid(self):
    return self._pid

  def _Fetch(self, version):
    """Fetches and installs the specified version of BentoCluster."""
    repo = maven_repo.MavenRepository(
        remotes=[
            maven_repo.KIJI_PUBLIC_REPO,
            maven_repo.KIJI_SNAPSHOT_REPO,
        ],
    )
    local_path = repo.Get(
        group='org.kiji.bento',
        artifact='bento-cluster',
        version=version,
        classifier='release',
        type='tar.gz',
    )
    ExtractArchive(archive=local_path, work_dir=self.path, strip_components=1)
    assert os.path.exists(os.path.join(self.path, 'bin', 'bento'))

  def _ReadPidFile(self, pid_file_path):
    if not os.path.exists(pid_file_path):
      return None

    try:
      with open(pid_file_path, 'r') as f:
        pid = int(f.read())
    except ValueError as err:
      # Invalid PID file content:
      os.remove(pid_file_path)
      return None

    if os.path.exists('/proc/%d' % pid):
      return pid
    else:
      # Stale PID file, remove:
      os.remove(pid_file_path)
      return None

  def Start(self):
    """Starts the Bento cluster, if necessary."""
    self._pid = self._ReadPidFile(self._pid_file)
    if self._pid is not None:
      logging.info('Bento cluster already started as PID=%d', self.pid)
    else:
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
            cwd=self.path,
            env=env,
        )
        proc.communicate()

      assert (proc.returncode == 0), (
          'bento start returned %d' % proc.returncode)
      with open(self._pid_file, 'r') as f:
        self._pid = int(f.read())
        logging.info('Bento cluster created and starter as PID=%d', self._pid)

    self._ReadConfig()

  def _ReadConfig(self):
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
    if self._pid is None:
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
    files = tuple(Find(self.path, file_name))
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


# ------------------------------------------------------------------------------


class CLI(cli.Action):
  def RegisterFlags(self):
    self.flags.AddString(
        name='install_dir',
        default='/tmp/bento-cluster',
        help='Path where BentoCluster is installed.',
    )
    self.flags.AddString(
        name='version',
        default='1.0.0',
        help='BentoCluster version.',
    )
    self.flags.AddString(
        name='do',
        default='start',
        help='Action to perform: start, stop or status.',
    )

  def Run(self, args):
    cluster = BentoCluster(
        path=self.flags.install_dir,
        version=self.flags.version,
    )
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    if self.flags.do == 'install':
      pass
    elif self.flags.do == 'start':
      cluster.Start()
    elif self.flags.do == 'stop':
      cluster.Stop()
    else:
      raise Error('Unknown action %r' % self.flags.do)


def Main(args):
  cli = CLI()
  return cli(args)


if __name__ == '__main__':
  base.Run(Main)
