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

from kiji import base
from kiji import bento_cluster
from kiji import command
from kiji import maven_fetcher

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
  help=('Version of KijiBento to download and test against. '
        + 'For example "1.0.0-rc4" or "1.0.0-rc5-SNAPSHOT".'),
)

FLAGS.AddBoolean(
  'cleanup_after_test',
  default=True,
  help=('When set, disables cleaning up after test. '
        + 'Bento cluster stay alive, working directory is not wiped.'),
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
      self, work_dir, version,
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
    self._maven_local_repo = maven_local_repo
    self._maven_remote_repo = maven_remote_repo

  def Setup(self):
    """Initializes the tutorial runner.

    Fetches the KijiBento Maven artifact, unzip it, starts a Bento cluster,
    and prepares a working environment.
    """

    archive_name = 'kiji-bento-%s-release.tar.gz' % self._kiji_version
    archive = os.path.join(self._work_dir, archive_name)

    if not os.path.exists(archive):
      maven_fetcher.FetchMavenArtifact(
        group_id='org.kiji.kiji-bento',
        artifact_id='kiji-bento',
        version=self._kiji_version,
        type='tar.gz',
        classifier='release',
        transitive=False,
        output_dir=self._work_dir,
        local_repo=self._maven_local_repo,
        remote_repo=self._maven_remote_repo,
      )

    self._kiji_bento_dir = (
        os.path.join(self._work_dir, 'kiji-bento-%s' % self._kiji_version))
    if not os.path.exists(self._kiji_bento_dir):
      # Strip the first path component from the kiji-bento release archive:
      # The top-level directory is "kiji-bento-<code-name>/",
      # but we don't know the code-name at this point.
      os.makedirs(self._kiji_bento_dir)
      ExtractArchive(archive, work_dir=self._kiji_bento_dir, strip_components=1)
    assert os.path.exists(self._kiji_bento_dir), (
        'KijiBento root directory not found: %r' % self._kiji_bento_dir)

    self._bento_cluster_dir = os.path.join(self._kiji_bento_dir, 'cluster')
    assert os.path.exists(self._bento_cluster_dir), (
        'BentoCluster root directory not found: %r' % self._bento_cluster_dir)

    self._bento = bento_cluster.BentoCluster(home=self._bento_cluster_dir)
    self._bento.Start()

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
         % self._bento.zookeeper_address),
        '-Dtest=TestHBaseQualifierPager', '-DfailIfNoTests=false',
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
    self._bento.Stop()
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

  if not FLAGS.kiji_bento_version:
    print('Specify the version of KijiBento to test '
          + 'with --kiji_bento_version=...')
    return os.EX_USAGE
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
