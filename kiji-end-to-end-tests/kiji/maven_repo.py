#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Maven repository wrapper."""

import hashlib
import logging
import os
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as etree

from base import base


FLAGS = base.FLAGS
LogLevel = base.LogLevel
Default = base.Default


class Error(Exception):
  """Errors used in this module."""
  pass


# ------------------------------------------------------------------------------


KIJI_PUBLIC_REPO = 'https://repo.wibidata.com/artifactory/kiji-packages'
KIJI_SNAPSHOT_REPO = 'https://repo.wibidata.com/artifactory/kiji-nightly'
KIJI_ALL_REPO = 'https://repo.wibidata.com/artifactory/all'
CLOUDERA_REPO = 'https://repository.cloudera.com/artifactory/cloudera-repos'
MAVEN_CENTRAL_REPO = 'http://repo.maven.apache.org/maven2'

# ------------------------------------------------------------------------------


def GetFileMD5(path):
  assert os.path.exists(path)
  buffer_size = 128 * 1024

  md5 = hashlib.md5()
  with open(path, 'rb') as f:
    while True:
      bytes = f.read(buffer_size)
      if len(bytes) == 0: break
      md5.update(bytes)
  return md5.hexdigest()


def GetFileSHA1(path):
  assert os.path.exists(path)
  buffer_size = 128 * 1024

  sha1 = hashlib.sha1()
  with open(path, 'rb') as f:
    while True:
      bytes = f.read(buffer_size)
      if len(bytes) == 0: break
      sha1.update(bytes)
  return sha1.hexdigest()


def GetFileFingerprints(path):
  assert os.path.exists(path)
  buffer_size = 128 * 1024

  md5 = hashlib.md5()
  sha1 = hashlib.sha1()
  with open(path, 'rb') as f:
    while True:
      bytes = f.read(buffer_size)
      if len(bytes) == 0: break
      md5.update(bytes)
      sha1.update(bytes)
  return (md5.hexdigest(), sha1.hexdigest())


# ------------------------------------------------------------------------------


class RemoteRepository(object):
  """Wraps a single Maven repository, whether local or remote."""

  def __init__(self, path):
    """Initializes a Maven repository wrapper.

    Args:
      path: Path (URL) of the Maven repository,
          eg. 'file:///home/x/.m2/repository' for a local repository,
          and 'https://repo.wibidata.com/artifactory/kiji-packages' for a
          remote repository.
    """
    self._path = path

  @property
  def path(self):
    return self._path

  @property
  def is_local(self):
    """Returns: whether this Maven repository is local."""
    return self.path.startswith('file://')

  @property
  def is_remote(self):
    """Returns: whether this Maven repository is remote."""
    return (self.path.startswith('http://') or self.path.startswith('https://'))

  def ReadFile(self, path):
    """Reads a file and returns its content as a byte array.

    Args:
      path: Path of the file to read.
    Returns:
      The file content as a byte array, or None if the file does not exist.
    """
    url = os.path.join(self._path, path)
    try:
      logging.info('Fetching %r', url)
      with urllib.request.urlopen(url) as opened:
        return opened.read()
    except urllib.error.HTTPError as err:
      if err.code == 404:
        return None
      else:
        raise err

  def GetPath(
      self,
      group,
      artifact=None,
      version=None,
      type=None,
      classifier=None,
      snapshot_version=None,
   ):
    """Reports the relative path for the specified resource.

    Args:
      group: Artifact group ID.
      artifact: Optional artifact ID.
      version: Optional artifact version.
          Requires artifact to be set.
      type: Optional artifact type (eg. 'jar', 'pom' or 'tar.gz').
          Requires version to be set.
      classifier: Optional classifier (eg. 'release' or 'tests').
          Requires type to be set.
      snapshot_version: Optional snapshot version.
          Require version ending with '-SNAPSHOT'.
    Returns:
      The path for the specified resource.
    """
    path = os.path.join(*group.split('.'))
    if artifact is None: return path  # Return group directory
    path = os.path.join(path, artifact)
    if version is None: return path  # Return artifact directory
    path = os.path.join(path, version)
    if type is None: return path  # Return artifact version directory

    # Requested resource is a file:

    name_parts = [artifact]
    if snapshot_version is not None:
      assert version.endswith('-SNAPSHOT')
      name_parts.append(snapshot_version)
    else:
      name_parts.append(version)
    if classifier is not None: name_parts.append(classifier)
    name = '%s.%s' % ('-'.join(name_parts), type)
    path = os.path.join(path, name)

    return path

  def GetURL(self, **coordinate):
    return os.path.join(self.path, self.GetPath(**coordinate))

  def ReadMetadataFile(self, group, artifact, version=None):
    """Retrieves the metadata file content for an artifact.

    Args:
      group: Artifact group ID.
      artifact: Artifact name.
      version: Optional version ID.
    Returns:
      The metadata file content, as a byte array, or None.
    """
    if not self.is_remote:
      return None
    path = self.GetPath(group=group, artifact=artifact, version=version)
    metadata_path = os.path.join(path, 'maven-metadata.xml')
    return self.ReadFile(metadata_path)

  def ListVersions(self, group, artifact):
    """Lists the versions of an artifact.

    Args:
      group: Artifact group ID.
      artifact: Artifact name.
    Yields:
      Version IDs.
    """
    try:
      metadata = self.ReadMetadataFile(group=group, artifact=artifact)
    except urllib.error.HTTPError as err:
      return
    if metadata is None: return
    xml = etree.fromstring(metadata)
    [versioning] = xml.getiterator('versioning')
    [versions] = versioning.findall('versions')
    for version in versions.findall('version'):
      yield version.text.strip()

  def Resolve(
      self, group, artifact, version, type,
      classifier=None,
      snapshot_version=None,
  ):
    if version.endswith('-SNAPSHOT') and (snapshot_version is None):
      if self.is_remote:
        # Resolve remote snapshot version:
        metadata = self.ReadMetadataFile(
            group=group,
            artifact=artifact,
            version=version,
        )
        if metadata is not None:
          xml = etree.fromstring(metadata)
          [snapshot] = xml.getiterator(tag='snapshot')

          [timestamp] = snapshot.findall('timestamp')
          timestamp = timestamp.text

          [build_number] = snapshot.findall('buildNumber')
          build_number = build_number.text

          base_version = base.StripSuffix(version, '-SNAPSHOT')
          snapshot_version = '-'.join([base_version, timestamp, build_number])

      else:
        # Snapshot from local repository:
        # Anything to do here?
        pass

    coordinate = dict(
        group=group,
        artifact=artifact,
        version=version,
        type=type,
        classifier=classifier,
        snapshot_version=snapshot_version,
    )
    return coordinate

  def ReadMD5File(self, path):
    md5 = self.ReadFile('%s.md5' % path)
    if md5 is not None: md5 = md5.decode()
    return md5

  def ReadSHA1File(self, path):
    sha1 = self.ReadFile('%s.sha1' % path)
    if sha1 is not None: sha1 = sha1.decode()
    return sha1

  def Open(
      self,
      group, artifact, version, type, classifier=None,
      snapshot_version=None,
  ):
    """Fetches a Maven artifact.

    Args:
      group: Artifact group ID.
      artifact: Artifact name.
      version: Version of the artifact.
      type: Type of the artifact to retrieve, eg. 'pom', 'jar', 'tar.gz'.
      classifier: Optional classifier, eg. 'release' or 'tests'.
      snapshot_version: Optional snapshot version.

    Returns:
      A tuple with (HTTP reply, MD5 sum, SHA1 sum) for the specified artifact,
      or None if the artifact does not exist.
    """
    coordinate = dict(
        group=group,
        artifact=artifact,
        version=version,
        type=type,
        classifier=classifier,
    )
    coordinate = self.Resolve(**coordinate)
    file_path = self.GetPath(**coordinate)
    md5 = self.ReadMD5File(file_path)
    sha1 = self.ReadSHA1File(file_path)
    file_url = os.path.join(self._path, file_path)

    logging.info('Opening %r', file_url)
    http_req = urllib.request.Request(url=file_url)
    try:
      http_reply = urllib.request.urlopen(http_req)
      return (http_reply, md5, sha1)
    except urllib.error.HTTPError as err:
      if err.code == 404:
        logging.error('Artifact %r does not exist in %s', file_path, self.path)
        return None
      else:
        raise err

  @staticmethod
  def ReadToFile(http_reply, output_path, buffer_size=128*1024):
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    logging.info('Fetching %r to %r', http_reply.url, output_path)

    md5 = hashlib.md5()
    sha1 = hashlib.sha1()

    with open(output_path, 'wb') as f:
      while True:
        data = http_reply.read(buffer_size)
        if len(data) == 0: break
        f.write(data)
        md5.update(data)
        sha1.update(data)

        sys.stdout.write('.')
        sys.stdout.flush()
      sys.stdout.write('\n')

    logging.info('Fetch of %r completed to %r', http_reply.url, output_path)
    return (md5.hexdigest(), sha1.hexdigest())


# ------------------------------------------------------------------------------


class MavenRepository(object):
  """Maven repository."""

  def __init__(
      self,
      local=Default,
      remotes=(),
  ):
    """Initializes a Maven repository.

    Args:
      local: Path of the directory for the local repository.
      remotes: Ordered list of remote repository URLs (file:// or http[s]://).
    """
    if local is Default:
      local = os.path.join(os.environ['HOME'], '.m2', 'repository')
    if len(urllib.request.urlparse(local).scheme) == 0:
      local = 'file://%s' % local
    assert (urllib.request.urlparse(local).scheme == 'file'), (
        'Invalid local repository path: %r' % local)
    self._local_path = local
    self._local = RemoteRepository(path=self._local_path)

    self._remote_paths = tuple(remotes)
    self._remotes = tuple(map(RemoteRepository, self._remote_paths))

  @property
  def local(self):
    """Local writable Maven repository."""
    return self._local

  @property
  def remotes(self):
    """Ordered list of remote Maven repositories."""
    return self._remotes

  def Get(self, group, artifact, version, type, classifier=None):
    """Retrieves an artifact locally.

    Returns:
      The path of the artifact in the local repository.
    """
    coordinate = dict(
        group=group,
        artifact=artifact,
        version=version,
        type=type,
        classifier=classifier,
    )
    path = self.local.GetPath(**coordinate)
    parsed = urllib.request.urlparse(self.local.GetURL(**coordinate))
    assert parsed.scheme == 'file'
    local_path = parsed.path
    md5_path = '%s.md5' % local_path
    sha1_path = '%s.sha1' % local_path

    if (os.path.exists(local_path)
        and os.path.exists(md5_path) and os.path.exists(sha1_path)
        and (GetFileFingerprints(local_path) ==
             (self.local.ReadMD5File(path), self.local.ReadSHA1File(path)))):
      return local_path

    for remote in self.remotes:
      try:
        open_result = remote.Open(**coordinate)
        if open_result is None: continue
        (http_reply, md5, sha1) = open_result
        (actual_md5, actual_sha1) = RemoteRepository.ReadToFile(
            http_reply=http_reply,
            output_path=local_path,
        )
        if md5 != actual_md5:
          logging.error(
              'MD5 mismatch for %r from %r: expected %r, got %r',
              local_path, remote.path, md5, actual_md5)
        if sha1 != actual_sha1:
          logging.error(
              'SHA1 mismatch for %r from %r: expected %r, got %r',
              local_path, remote.path, sha1, actual_sha1)
        if (md5 == actual_md5) and (sha1 == actual_sha1):
          with open(md5_path, 'w') as f:
            f.write(md5)
          with open(sha1_path, 'w') as f:
            f.write(sha1)
          return local_path
        else:
          os.remove(local_path)

      except urllib.error.HTTPError as err:
        logging.error('Error: %s', err.readlines())

    return None


# ------------------------------------------------------------------------------


if __name__ == '__main__':
  raise Error('Cannot run %s as a standalone script.' % base.GetProgramName())
