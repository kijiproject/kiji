#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fetches Maven artifacts."""

import logging
import os
import subprocess
import sys
import tempfile


class Error(Exception):
  """Errors used in this module"""
  pass


# Template for a pom.xml file describing a project that depends on a single
# externally specified Maven artifact.
POM_XML_TEMPLATE = """
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.kiji</groupId>
  <artifactId>maven-fetcher</artifactId>
  <version>0.0.0</version>
  <packaging>jar</packaging>

  <dependencies>
    <dependency>
      <groupId>%(group_id)s</groupId>
      <artifactId>%(artifact_id)s</artifactId>
      <version>%(version)s</version>
      <classifier>%(classifier)s</classifier>
      <type>%(type)s</type>
      <scope>runtime</scope>
    </dependency>
  </dependencies>

  <repositories>
    %(remote_repo)s
  </repositories>
</project>
"""


def FetchMavenArtifact(
    group_id,
    artifact_id,
    version,
    classifier,
    type,
    transitive,
    output_dir,
    local_repo=None,
    remote_repo=None,
):
  """Fetches a Maven artifact.

  Args:
    group_id: Group of the artifact to fetch.
    artifact_id: ID of the artifact to fetch.
    version: Version of the artifact to fetch.
    classifier: Classifier of the artifact to fetch (eg. '', 'release').
    type: Type of the artifact to fetch (eg. 'jar', 'tar.gz').
    transitive: Whether to fetch transitive dependencies.
    output_dir: Where to write the artifact.
    local_repo: Optional local repository.
    remote_repo: Optional remote repository to fetch from.

  Raises:
    Error if the artifact cannot be fetched.
  """
  # Full Maven artifact ID:
  full_id = (
    '%s:%s:%s:%s:%s' % (group_id, artifact_id, version, classifier, type))

  # Generate a pom.xml for a dummy Maven project that depends on the specified
  # artifact. :
  params = {
      'group_id': group_id,
      'artifact_id': artifact_id,
      'classifier': classifier,
      'version': version,
      'type': type,
      'remote_repo': '',
  }
  if remote_repo:
    # A remote Maven repository is specified, include it in the pom.xml:
    params['remote_repo'] = (
      '<repository> <id>remote_repo</id> <url>%s</url> </repository>'
      % remote_repo)

  with tempfile.TemporaryDirectory(prefix='maven-fetcher.') as work_dir:
    logging.debug('Maven fetch working directory is %s', work_dir)

    # Write the pom.xml:
    pom_xml = os.path.join(work_dir, 'pom.xml')
    with open(pom_xml, 'w') as f:
      pom_file_content = POM_XML_TEMPLATE % params
      logging.debug('Writing pom.xml file:\n%s', pom_file_content)
      f.write(pom_file_content)

    logging.info('Fetching Maven artifact %s', full_id)

    output_dir = os.path.abspath(output_dir)
    logging.debug('Maven fetch output directory: %r', output_dir)

    # Run a 'mvn dependency:copy-dependencies' command:
    args=[
        'mvn',
        'dependency:copy-dependencies',
        '-DoutputDirectory=%s' % output_dir,
        '-DexcludeTransitive=%s' % ('false' if transitive else 'true'),
        '-U',
    ]
    if local_repo:
      args.append('-Dmaven.repo.local=%s' % local_repo)

    if logging.getLogger().level > logging.DEBUG:
      args.append('--quiet')

    logging.debug('Running command: %r in directory %r', args, work_dir)
    proc = subprocess.Popen(args=args, cwd=work_dir)
    proc.communicate()
    if proc.returncode != 0:
      raise Error('Error fetching artifact: %s' % full_id)


def Main(args):
  logging.error('%r cannot be used as a standalone script.', args[0])


if __name__ == '__main__':
  Main(sys.argv)
