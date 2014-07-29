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

from kiji import bento_cluster
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


class KijiBento(object):
  """KijiBento distribution.

  Wraps a KijiBento installation.
  """

  def __init__(self, path, version=None):
    """Initializes the KijiBento object.

    Args:
      path: Path of the KijiBento install directory.
      version: Bento version, eg. '1.0.1' or '1.0.2-SNAPSHOT'.
               Latest version if not specified or None.
    """
    self._path = path
    self._version = version
    self._bento_cluster = None

  @property
  def path(self):
    return self._path

  @property
  def version(self):
    return self._version

  @property
  def installed(self):
    return os.path.exists(os.path.join(self.path, 'bin', 'kiji-env.sh'))

  @property
  def bento_cluster(self):
    if self._bento_cluster is None:
      bento_path = os.path.join(self.path, 'cluster')
      self._bento_cluster = bento_cluster.BentoCluster(
          path=bento_path,
          version=None,  # Bento must not be fetched separately
          enable_log=True,
      )
    return self._bento_cluster

  def Install(self):
    """Ensures KijiBento is installed properly."""
    if not self.installed:
      self._Fetch(version=self.version)

  def GetMostRecentVersion(self):
    """Reports the most recent version of KijiBento from the SNAPSHOT repo."""
    snapshot_repo = maven_repo.RemoteRepository(maven_repo.KIJI_SNAPSHOT_REPO)
    bento_versions = list(snapshot_repo.ListVersions(group='org.kiji.kiji-bento',
                                                     artifact='kiji-bento'))

    def order_versions(version_string):
      """Splits the version string into a tuple of numbers that can be compared with their
      natural ordering to get ordering that matches version semantics.

      rc versions, since we no longer use them, are replaced with 0."""
      split_str = re.split("[.-]", version_string)
      if (split_str[-1] == "SNAPSHOT"):
        split_str[-1] = "1"
        if (re.search("rc.*", split_str[-2])):
          split_str[-2] = 0
      else:
        logging.error("Encountered unexpected non-snapshot version %s in repo %s.",
                      version_string,
                      snapshot_repo)
        split_str.append("0")
      return list(map(int, split_str))

    latest_version = sorted(
        bento_versions,
        reverse=True,
        key=order_versions
    )[0]
    logging.info("Using latest version %r from possible versions %r.",
                 latest_version,
                 bento_versions)
    return latest_version

  def _Fetch(self, version):
    """Fetches and installs the specified version of BentoCluster."""
    assert not self.installed
    repo = maven_repo.MavenRepository(
        remotes=[
            maven_repo.KIJI_PUBLIC_REPO,
            maven_repo.KIJI_SNAPSHOT_REPO,
        ],
    )
    if version is None:
        version = self.GetMostRecentVersion()
    local_path = repo.Get(
        group='org.kiji.kiji-bento',
        artifact='kiji-bento',
        version=version,
        classifier='release',
        type='tar.gz',
    )

    # Strip the first path component from the kiji-bento release archive:
    # The top-level directory is "kiji-bento-<code-name>/",
    # but we don't know the code-name at this point.
    ExtractArchive(archive=local_path, work_dir=self.path, strip_components=1)

    assert self.installed

    # Ensure the BentoCluster exists:
    self.bento_cluster


# ------------------------------------------------------------------------------


class CLI(cli.Action):
  def RegisterFlags(self):
    self.flags.AddString(
        name='install_dir',
        default='/tmp/kiji-bento',
        help='Path where KijiBento is installed.',
    )
    self.flags.AddString(
        name='version',
        default='1.0.0',
        help='KijiBento version.',
    )
    self.flags.AddString(
        name='do',
        default='install',
        help='Action to perform: install.',
    )

  def Run(self, args):
    bento = None
    assert (len(args) == 0), ('Unexpected arguments: %r' % args)
    if self.flags.do == 'install':
      bento = KijiBento(
          path=self.flags.install_dir,
          version=self.flags.version,
      )
    else:
      raise Error('Unknown action %r' % self.flags.do)


def Main(args):
  cli = CLI()
  return cli(args)


if __name__ == '__main__':
  base.Run(Main)
