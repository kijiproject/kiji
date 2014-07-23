#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

import argparse
import express
import logging
import os
import sys
import tempfile
import unittest


# --------------------------------------------------------------------------------------------------


def touch(path, content=''):
    with open(path, 'wt') as f:
        f.write(content)


class ExpressTest(unittest.TestCase):
    """Unit-tests for the express startup program."""

    def test_md5_sum(self):
        with tempfile.NamedTemporaryFile(prefix='test_md5_sum', mode='wt') as f:
            touch(path=f.name, content='hello')
            self.assertEqual('5d41402abc4b2a76b9719d911017c592', express.md5_sum(f.name))

    def test_flat_map(self):
        self.assertEqual(
            [0, 0, 1, 0, 1, 2],
            list(express.flat_map(operator=range, iterable=range(4))))

    def test_unique(self):
        self.assertEqual(
            [0, 1, 2, 3, 4],
            list(express.unique([0, 1, 2, 1, 2, 3, 4])))

    def test_expand_classpath_entry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            logging.debug('Using temporary directory %r', temp_dir)
            touch(path=os.path.join(temp_dir, 'a.jar'))
            touch(path=os.path.join(temp_dir, 'b.jar'))

            # Nothing to expand here, path should remain unchanged:
            expanded = list(express.expand_classpath_entry(entry=os.path.join(temp_dir, 'a.jar')))
            self.assertEqual([os.path.join(temp_dir, 'a.jar')], expanded)

            # The JVM expands entries '/path/to/*' as the shell expands '/path/to/*.jar':
            expanded = list(express.expand_classpath_entry(entry=os.path.join(temp_dir, '*')))
            self.assertEqual(
                [os.path.join(temp_dir, 'a.jar'), os.path.join(temp_dir, 'b.jar')],
                expanded)

            # However, the JVM does not expand arbitrary globs, such as '*.jar':
            expanded = list(express.expand_classpath_entry(entry=os.path.join(temp_dir, 'a*.jar')))
            self.assertEqual([os.path.join(temp_dir, 'a*.jar')], expanded)

    def test_normalize_classpath(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            logging.debug('Using temporary directory %r', temp_dir)
            touch(path=os.path.join(temp_dir, 'a.jar'), content='content1')
            touch(path=os.path.join(temp_dir, 'b.jar'), content='content1')
            touch(path=os.path.join(temp_dir, 'c.jar'), content='content2')
            os.symlink(src=os.path.join(temp_dir, 'c.jar'), dst=os.path.join(temp_dir, 'd.jar'))
            os.makedirs(os.path.join(temp_dir, 'x'))

            # b.jar duplicates a.jar (detected via MD5 sum)
            # d.jar duplicates c.jar (detected by symlink normalization)
            # x/../d.jar duplicates c.jar (detected by path reduction)
            # f.jar is eliminated because it does not exist
            classpath = [
                os.path.join(temp_dir, 'a.jar'),
                os.path.join(temp_dir, 'b.jar'),
                os.path.join(temp_dir, 'c.jar'),
                os.path.join(temp_dir, 'd.jar'),
                os.path.join(temp_dir, 'x', '..', 'd.jar'),
                os.path.join(temp_dir, 'f.jar'),
            ]
            normalized = list(express.normalize_classpath(classpath))
            self.assertEqual(
                [os.path.join(temp_dir, 'a.jar'), os.path.join(temp_dir, 'c.jar')],
                normalized)




# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    print('Not a standalone module: %r. Use: "test_runner.py <module>" instead.' % __file__)
    sys.exit(os.EX_USAGE)
