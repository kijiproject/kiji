#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Analyzes a classpath and reports JAR files whose content overlap."""

import argparse
import collections
import glob
import hashlib
import itertools
import logging
import os
import zipfile

from base import base
from base import cli


def md5_sum(file_path):
    """Computes the MD5 sum of a file.

    Args:
      file_path: Path of the file to compute the MD5 sum for.
    Returns:
      The file MD5 sum, represented as an hex string (32 characters).
    """
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        md5.update(f.read())
    return md5.hexdigest()


def expand_classpath_entry(entry):
    """Expand the specified classpath entry if it contains a wildcard.

    Expand the '*' classpath wildcard by applying the glob '*.jar'.

    Yields:
      Expanded classpath entries.
    """
    if os.path.basename(entry) != "*":
        yield entry
    else:
        expanded = glob.glob(os.path.join(os.path.dirname(entry), "*.jar"))
        expanded = sorted(expanded)
        yield from expanded


def flat_map(operator, iterable):
    """Concatenates the collections produced by an operator mapped on a given collection.

    Args:
      operator: Operator to apply on each input element from iterable.
          The expected signature for operator is: element -> iterable.
      iterable: Iterable of elements to apply the operator onto.
    Returns:
      An iterable of the concatenation of the resulting collections.
    """
    return itertools.chain.from_iterable(map(operator, iterable))


def unique(iterable, key=None):
    """Removes duplicate items from the specified iterable.

    Args:
      iterable: Collection of items to filter duplicates from.
      key: Optional function with a signature: item -> item_key.
          Specifying a custom key allows to identify duplicates through some indirect attributes
          of the items.
    Returns:
      Original iterable with duplicate items removed.
    """
    watched = set()
    if key is None:
        key = lambda x: x  # identity

    def watch(item):
        """Stateful filter that remembers items previously watched."""
        item_key = key(item)
        if item_key in watched:
            return False
        else:
            watched.add(item_key)
            return True

    return filter(watch, iterable)


def tab_indent(text):
    """Left-indents a string of text."""
    return "\t" + text


def _exists_or_log(entry):
    exist = os.path.exists(entry)
    if not exist:
        logging.warning("Classpath entry does not exist: %r", entry)
    return exist


def normalize_classpath(classpath):
    """Normalizes the given classpath entries.

    Performs the following normalizations:
     - Classpath wildcards are expanded.
     - Symlinks are expanded.
     - Paths are made absolute.
     - Duplicate paths are eliminated.
     - Non-existent paths are removed.

    Args:
      classpath: Iterable of classpath entries.
    Returns:
      Iterable of normalized classpath entries.
    """
    classpath = flat_map(expand_classpath_entry, classpath)
    classpath = filter(_exists_or_log, classpath)
    classpath = map(os.path.realpath, classpath)
    classpath = unique(classpath)
    #classpath = unique(classpath, key=os.path.basename)

    # Filter out JAR files whose MD5 is known already:
    def md5_or_path(path):
        if os.path.isfile(path):
            return md5_sum(path)
        else:
            return 'path:%s' % path

    classpath = unique(classpath, key=md5_or_path)

    return classpath


# def normalize(entries):
#     """Normalizes a classpath.

#     Globs are expanded.
#     Exact path duplicates are removed.

#     Args:
#         entries: Iterable of classpath entries.
#     Yields:
#         Normalized classpath entries.
#     """
#     emitted_path = set()
#     emitted_md5 = set()

#     for entry in entries:
#         if entry is None or len(entry) == 0:
#             continue
#         for expanded in glob.glob(entry):
#             expanded = os.path.abspath(expanded)
#             if expanded in emitted_path:
#                 continue
#             else:
#                 emitted_path.add(expanded)

#             if os.path.isfile(expanded):
#                 md5 = md5_sum(expanded)
#                 if md5 in emitted_md5:
#                     logging.debug("Ignoring classpath entry %r with duplicate MD5 sum: %r.",
#                                   expanded, md5)
#                     continue
#                 else:
#                     emitted_md5.add(md5)

#             yield expanded


def list_jar_file_entries(jar_path):
    """Scans a JAR file and lists the file paths it contains.

    Args:
        jar_path: Path of the JAR file to scan.
    Yield:
        Path of the files included in the JAR file.
    """
    if os.path.isdir(jar_path):
        return

    try:
        with zipfile.ZipFile(jar_path) as zf:
            for info in zf.infolist():
                yield info.filename
    except zipfile.BadZipFile:
        logging.error("%r is not a valid JAR file.", jar_path)



class CLI(cli.Action):
    def RegisterFlags(self):
        pass

    def Run(self, args):
        assert (len(args) == 1), ("Specify a classpath to analyze.")
        classpath = args[0]
        cp_entries = classpath.split(":")
        cp_entries = list(normalize_classpath(cp_entries))

        # Map: class name -> set of jar path
        class_map = collections.defaultdict(set)

        # Map: jar path -> set of class name
        jar_map = collections.defaultdict(set)

        for cp_entry in cp_entries:
            logging.debug("Processing classpath entry: %r", cp_entry)
            for zip_entry in list_jar_file_entries(cp_entry):
                if not zip_entry.endswith(".class"):
                    continue
                class_name = base.strip_suffix(zip_entry, ".class").replace("/", ".")
                class_map[class_name].add(cp_entry)

                jar_map[cp_entry].add(class_name)

        for jar_paths in frozenset(map(frozenset, class_map.values())):
            if len(jar_paths) > 1:
                logging.info("-" * 80)
                logging.info(
                    "Overlapping JARs: %s\n%s",
                    ", ".join(sorted(frozenset(map(lambda k: os.path.basename(k), jar_paths)))),
                    "\n".join(map(lambda k: "\t%s" % k, jar_paths)))

                union_classes = set()
                xsect_classes = None
                for jar_path in sorted(jar_paths):
                    classes = jar_map[jar_path]
                    union_classes.update(classes)
                    if xsect_classes is None:
                        xsect_classes = set(classes)
                    else:
                        xsect_classes.intersection_update(classes)
                logging.info("Union: %d classes - Intersection: %d classes.",
                             len(union_classes), len(xsect_classes))


def main(args):
    cli = CLI()
    return cli(args)


if __name__ == "__main__":
    base.run(Main)
