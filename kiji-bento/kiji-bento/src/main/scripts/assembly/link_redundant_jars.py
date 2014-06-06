#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

"""Copies all JARs needed for a project into a bento box lib dir."""

import argparse
import collections
import hashlib
import logging
import os
import re
import subprocess
import sys


description = (
    "This script will help you copy JAR files from your local maven repo and into the Bento "
    "Box lib dir.")


RE_JAR = re.compile(r".*[.]jar")


def md5_sum(file_path):
    """Computes the MD5 sum of a given file.

    Args:
        file_path: Path of the file to sum.
    Returns:
        The MD5 hex sum of the specified file.
    """
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def list_files(root_dir, name_pattern):
    """List the files whose name matches a specified pattern.

    Args:
        root_dir: Path of the directory to recursively scan for files.
        name_pattern: Regex of the file names to match.
    Yields:
        The full paths of the files that match the specified pattern.
    """
    for (dir_path, _, filenames) in os.walk(root_dir):
        for file_name in filenames:
            if name_pattern.match(file_name):
                yield os.path.join(dir_path, file_name)


def run(cmd):
    """Runs a shell command as a sub-process and reports the command output.

    Args:
        cmd: Shell command to run.
    Returns:
        The captured command output.
    """
    try:
        return subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as err:
        logging.error("Error running command %r", cmd)
        logging.error("Exit code = %s", e.returncode)
        logging.error("Output = %s", e.output)
        raise err


# --------------------------------------------------------------------------------------------------


class RedundantJarLinker(object):
    """Identify identical JARs found in a directory tree and symlink them together.

    The directory tree should not contain multiple copies of the same JARs.
    For now, the process uses JAR file names only.
    """

    def __init__(self):
        super(RedundantJarLinker, self).__init__()

        # Root bento box directory.
        self._bento_dir = None

        # Counter for the number of bytes saved.
        self._bytes_saved = 0

    def _create_parser(self):
        """Returns a parser for the script."""
        parser = argparse.ArgumentParser(
                description=description,
                formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument(
                "--verbose",
                action="store_true",
                default=False,
                help="Verbose mode (turn on logging.info)")
        parser.add_argument(
                "--debug",
                action="store_true",
                default=False,
                help="Debug (turn on logging.debug)")
        parser.add_argument(
                "--root-dir",
                type=str,
                default=os.getcwd(),
                help="Root directory (containing tgz for bento) [pwd]")
        parser.add_argument(
                "--dry-run",
                action="store_true",
                default=False,
                help="Do not actually sym link")
        return parser

    def _parse_options(self, cmd_line_args):
        """Processes command-line flags.

        Args:
            cmd_line_args: List of the command-line arguments to process.
        """
        args = self._create_parser().parse_args(cmd_line_args)

        if args.verbose:
            logging.basicConfig(level=logging.INFO)

        if args.debug:
            logging.basicConfig(level=logging.DEBUG)

        self._bento_dir = os.path.abspath(args.root_dir)
        assert os.path.isdir(self._bento_dir), ("Invalid Bento root directory: %r" % self._bento_dir)
        logging.info("Bento directory is %r", self._bento_dir)

        self._args = args

    def _build_md5_jars_map(self):
        """Scans the Bento file tree for JAR files and build a map indexed by MD5 sum.

        Returns:
            Map from MD5 sum of JAR files to the set of the JAR file paths sharing the MD5 sum.
        """
        # Map: JAR file MD5 sum -> set of JAR file paths sharing the same MD5
        md5_map = collections.defaultdict(set)

        jar_count = 0

        for jar_path in list_files(root_dir=self._bento_dir, name_pattern=RE_JAR):
            jar_count += 1
            jar_md5 = md5_sum(jar_path)
            md5_map[jar_md5].add(jar_path)

        logging.info("Found %d unique MD5 sums out of %d jar files.", len(md5_map), jar_count)
        for md5, jar_paths in md5_map.items():
            if len(jar_paths) < 1:
                continue
            logging.debug("Deduplicating %d JARs with MD5 %r: %r",
                          len(jar_paths), md5, sorted(jar_paths))

        return md5_map

    def _deduplicate_jars(self, md5, jar_paths):
        """Deduplicates the specified JAR files.

        Args:
            md5: MD5 sum of the JAR files to deduplicate.
            jar_paths: Set of JAR file paths to deduplicate.
                    The JAR files are expected to share the same MD5.
        """
        jar_paths = set(jar_paths)
        logging.debug("Deduplicating %d JARs with MD5 %r", len(jar_paths), md5)

        target_jar_path = min(jar_paths, key=lambda path: len(path))
        target_jar_size = os.path.getsize(target_jar_path)
        jar_paths.remove(target_jar_path)
        logging.debug("Symlinking %d JARs with MD5 %r to %r (%d bytes)",
                                    len(jar_paths), md5, target_jar_path, target_jar_size)

        for duplicate_jar_path in jar_paths:
            assert (os.path.getsize(duplicate_jar_path) == target_jar_size), (
                    'Inconsistent file size for duplicate JAR %r' % duplicate_jar_path)

            rel_jar_path = os.path.relpath(target_jar_path, start=os.path.dirname(duplicate_jar_path))
            logging.debug("Symlinking %r -> %r", duplicate_jar_path, rel_jar_path)

            if not self._args.dry_run:
                os.remove(duplicate_jar_path)
                os.symlink(src=rel_jar_path, dst=duplicate_jar_path)

        self._bytes_saved += target_jar_size * len(jar_paths)

    def go(self, cmd_line_args):
        """Entry point of the JAR deduplicator."""
        self._parse_options(cmd_line_args)
        md5_jars_map = self._build_md5_jars_map()
        for md5, jar_paths in md5_jars_map.items():
            if len(jar_paths) > 1:
                self._deduplicate_jars(md5, jar_paths)
        logging.info("Saved %d bytes.", self._bytes_saved)



if __name__ == "__main__":
    foo = RedundantJarLinker()
    foo.go(sys.argv[1:])
