#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Runs KijiSchema unit-tests against a real HBase."""

import logging
import os
import shutil
import sys
import tempfile

# Add the root directory to the Python path if necessary:
__path = os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0])))
if __path not in sys.path:
  sys.path.append(__path)

from base import base
from base import command
from kiji import kiji_bento

FLAGS = base.FLAGS
LogLevel = base.LogLevel


class Error(Exception):
  """Errors used in this module."""
  pass


# ------------------------------------------------------------------------------


FLAGS.AddString(
  'work_dir',
  help='Working directory.',
)

FLAGS.AddString(
  'maven_local_repo',
  help='Optional Maven local repository from where to fetch artifacts.',
)

FLAGS.AddString(
  'maven_remote_repo',
  help='Optional Maven remote repository from where to fetch artifacts.',
)

FLAGS.AddString(
  'kiji_bento_version',
  default=None,
  help=('Version of KijiBento to download and test against. '
        'For example "1.0.0-rc4" or "1.0.0-rc5-SNAPSHOT". '
        'If not specified, uses the most recent version in the nightly repo.'),
)

FLAGS.AddBoolean(
  'cleanup_after_test',
  default=True,
  help=('When set, disables cleaning up after test. '
        'Bento cluster stay alive, working directory is not wiped.'),
)

FLAGS.AddBoolean(
  'help',
  default=False,
  help='Prints a help message.',
)


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
  assert os.path.exists(work_dir), (
      'Working directory %r does not exist', work_dir)
  os.system(
      '/bin/tar xf %s --directory %s --strip-components=%d'
      % (archive, work_dir, strip_components))


# ------------------------------------------------------------------------------


class Test(object):
  """Runs the KijiMusic tutorial."""

  def __init__(
      self,
      work_dir,
      version,
      maven_local_repo=None,
      maven_remote_repo=None):
    """Initializes the tutorial runner.

    Args:
      work_dir: Working directory where to operate.
      version: Version of KijiBento to test, eg. '1.0.0-rc5-SNAPSHOT'.
      maven_local_repo: Optional local Maven repository.
      maven_remote_repo: Optional remote Maven repository.
    """
    self._work_dir = work_dir
    self._run_id = base.NowMS()
    self._kiji_version = version

    # TODO: Propagate these to KijiBento
    self._maven_local_repo = maven_local_repo
    self._maven_remote_repo = maven_remote_repo

    self._kiji_bento = kiji_bento.KijiBento(
      path=os.path.join(self._work_dir, 'kiji-bento-%s' % self._kiji_version),
      version=self._kiji_version,
    )

  @property
  def kiji_bento(self):
    """Returns the KijiBento install."""
    return self._kiji_bento

  @property
  def bento_cluster(self):
    """Returns the BentoCluster install."""
    return self.kiji_bento.bento_cluster

  def Setup(self):
    """Initializes the tutorial runner.

    Fetches the KijiBento Maven artifact, unzip it, starts a Bento cluster,
    and prepares a working environment.
    """
    self.kiji_bento.Install()
    self.bento_cluster.Start()

  def Run(self):
    git_repo_name = 'kiji_schema'
    git_repo_dir = os.path.join(self._work_dir, git_repo_name)

    if not os.path.exists(git_repo_dir):
      command.Command(
          'git', 'clone', 'git://github.com/kijiproject/kiji-schema.git', git_repo_name,
          work_dir=self._work_dir,
          exit_code=0,
      )

    command.Command(
        'git', 'fetch', 'origin',
        work_dir=git_repo_dir,
        exit_code=0,
    )
    command.Command(
        'git', 'checkout', 'origin/master',
        work_dir=git_repo_dir,
        exit_code=0,
    )
    maven = command.Command(
        'mvn', 'clean', 'test',
        ('-DargLine=-Dorg.kiji.schema.KijiClientTest.HBASE_ADDRESS=%s'
         % self.bento_cluster.zookeeper_address),
        work_dir=git_repo_dir,
    )
    if maven.exit_code == 0:
      print('No test failure')
      logging.info('Maven error stream:\n%s', maven.error_text)
      logging.info('Maven output stream:\n%s', maven.output_text)
      return os.EX_OK
    else:
      logging.error('There are test failures')
      logging.info('Maven error stream:\n%s', maven.error_text)
      logging.info('Maven output stream:\n%s', maven.output_text)
      return os.EX_DATAERR


  # ----------------------------------------------------------------------------
  # Cleanup:

  def Cleanup(self):
    self.bento_cluster.Stop()
    shutil.rmtree(self._work_dir)


# ------------------------------------------------------------------------------


def Main(args):
  """Program entry point."""
  if FLAGS.help:
    FLAGS.PrintUsage()
    return os.EX_OK

  if len(args) > 0:
    logging.error('Unexpected command-line arguments: %r' % args)
    FLAGS.PrintUsage()
    return os.EX_USAGE

  # Create a temporary working directory:
  cwd = os.getcwd()
  work_dir = FLAGS.work_dir
  if work_dir is None:
    work_dir = tempfile.mkdtemp(prefix='work_dir.', dir=os.getcwd())
  work_dir = os.path.abspath(work_dir)
  if not os.path.exists(work_dir):
    os.makedirs(work_dir)
  FLAGS.work_dir = work_dir

  logging.info('Working directory: %r', work_dir)

  logging.info('Using KijiBento %s', FLAGS.kiji_bento_version)

  # Runs the tests:
  test = Test(
      work_dir=work_dir,
      version=FLAGS.kiji_bento_version,
      maven_local_repo=FLAGS.maven_local_repo,
      maven_remote_repo=FLAGS.maven_remote_repo,
  )
  test.Setup()
  try:
    return test.Run()
  finally:
    if FLAGS.cleanup_after_test:
      test.Cleanup()


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  base.Run(Main)
