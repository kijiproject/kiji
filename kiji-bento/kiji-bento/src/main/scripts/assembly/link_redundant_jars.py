#!/usr/bin/env python2.7

""" Copies all JARs needed for a project into a bento box lib dir. """

import argparse
import collections
import functools
import logging
import os
import re
import shutil
import subprocess
import sys

myname = os.path.split(sys.argv[0])[-1]
description = \
    "This script will help you copy JAR files from your local maven repo and into the Bento " + \
    "Box lib dir."

# Regex to get the bento version, assumes for now that each part of the version is only one digit
# (makes getting the most-recent version easy - just sort lexographically).
p_bento = re.compile(r'kiji-bento-(?P<name>\w+)-(?P<version>\d\.\d\.\d)-release\.tar\.gz')

def run(cmd):
  result = ""
  try:
    result = subprocess.check_output(cmd, shell=True)
  except subprocess.CalledProcessError as e:
    sys.stderr.write("Error running command '%s'\n" % cmd)
    sys.stderr.write("Exit code = %s\n" % e.returncode)
    sys.stderr.write("Output = %s\n" % e.output)
    raise e
  return result

class RedundantJarLinker(object):

  def __init__(self):
    super(RedundantJarLinker, self).__init__()

    # Root bento box directory.
    self._bento_dir = None

  def _create_parser(self):
    """ Returns a parser for the script """

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        default=False,
        help='Verbose mode (turn on logging.info)')

    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        default=False,
        help='Debug (turn on logging.debug)')

    parser.add_argument(
        '-r',
        '--root-dir',
        type=str,
        default=os.getcwd(),
        help='Root directory (containing tgz for bento) [pwd]')

    parser.add_argument(
        '-x',
        '--skip-link',
        action='store_true',
        default=False,
        help='Do not actually sym link')

    return parser

  def _parse_options(self, cmd_line_args):

    # ----------------------------------------------------------------------------------------------
    # Parse command-line arguments
    args = self._create_parser().parse_args(cmd_line_args)

    if args.verbose:
      logging.basicConfig(level=logging.INFO)

    if args.debug:
      logging.basicConfig(level=logging.DEBUG)

    self._bento_dir = args.root_dir
    assert os.path.isdir(self._bento_dir)
    logging.info("Bento directory is " + self._bento_dir)

    self._do_link = not args.skip_link

  def _get_symlink_candidates(self):
    """
    Starting at the Bento root dir, create a map from JAR file names to locations.  For any JAR in
    more than one location, symlink all locations to the location with the shortest name.

    """

    jarsToLocations = collections.defaultdict(set)

    # TODO: Make this more efficient?
    for (dirpath, _, filenames) in os.walk(os.getcwd()):
      for fname in filenames:
        if not fname.endswith('.jar'):
          continue
        jarsToLocations[fname].add(dirpath)

    logging.info("Found %d unique jars" % len(jarsToLocations.keys()))
    logging.debug("JARS that we can symlink:")
    logging.debug("^^^^^^^^^^^^^^^^^^^^^^^^^")
    for jarname in jarsToLocations.keys():
      if (len(jarsToLocations[jarname]) > 1):
        logging.debug(jarname)
        for jarpath in jarsToLocations[jarname]:
          logging.debug("\t" + jarpath)
    return jarsToLocations

  def _symlink_jars(self, jarsToLocations):
    self._link_count = 0
    for jarname in jarsToLocations.keys():
      if (len(jarsToLocations[jarname]) > 1):
        self._reduce_to_one_jar(jarname, jarsToLocations[jarname])

  def _reduce_to_one_jar(self, jar_name, jar_dirs):
    target_dir = sorted(list(jar_dirs))[-1]
    target_jar = os.path.join(target_dir, jar_name)


    for link_dir in jar_dirs:
      if link_dir == target_dir:
        continue
      link_jar = os.path.join(link_dir, jar_name)

      if not self._is_same_size_as_target_jar(target_jar, link_jar):
        continue

      relpath = os.path.relpath(target_dir, link_dir)
      logging.debug("relpath from %s to %s is %s" % (link_dir, target_dir, relpath))

      if self._do_link:
        os.remove(link_jar)
        os.symlink(os.path.join(relpath, jar_name), link_jar)
        self._link_count += 1

  def _is_same_size_as_target_jar(self, target_jar, link_jar):
    """ All of the JARs should be the same size, or else something weird is going on... """
    size_a = os.path.getsize(target_jar)
    size_b = os.path.getsize(link_jar)
    return size_a == size_b

  def go(self, cmd_line_args):
    self._parse_options(cmd_line_args)
    old_dir = os.getcwd()
    os.chdir(self._bento_dir)
    jarsToLocations = self._get_symlink_candidates()
    self._symlink_jars(jarsToLocations)
    print("Added %d symlinks" % self._link_count)
    os.chdir(old_dir)


if __name__ == "__main__":
  foo = RedundantJarLinker()
  foo.go(sys.argv[1:])
