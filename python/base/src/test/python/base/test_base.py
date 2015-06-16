#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tests for module base.base"""

import os

from base import base
from base import unittest


class TestBase(unittest.BaseTestCase):
    """Tests for the base module."""

    def test_touch(self):
        path = base.random_alpha_num_word(16)
        try:
            self.assertFalse(os.path.exists(path))
            base.touch(path)
            self.assertTrue(os.path.exists(path))
            base.touch(path)
            self.assertTrue(os.path.exists(path))
        finally:
            base.remove(path)

    def test_un_camel_case(self):
        self.assertEqual('jira', base.un_camel_case('JIRA'))
        self.assertEqual('jira_tool', base.un_camel_case('JIRATool'))
        self.assertEqual('jira_tool', base.un_camel_case('jira_tool'))
        self.assertEqual('jira_tool', base.un_camel_case('jira tool'))
        self.assertEqual('jira_tool', base.un_camel_case('Jira tool'))
        self.assertEqual('jira_tool', base.un_camel_case(' Jira tool'))
        self.assertEqual('status_csv', base.un_camel_case(' StatusCSV'))

    def test_memoized_property(self):
        class Test(object):
            def __init__(self):
                self._counter = 0

            @base.memoized_property
            def memoized(self):
                self._counter += 1
                return self._counter

        test = Test()
        self.assertEqual(test.memoized, 1)
        self.assertEqual(test.memoized, 1)
        self.assertEqual(test._counter, 1)

    def test_wrap_text(self):
        """Text should be wrapped at word breaks if possible."""

        # empty lines have no transformation
        self.assertEqual("\n\n\n\n", base.wrap_text("\n\n\n\n", 3))

        # with no spaces, break words on ncolumns
        self.assertEqual("0", base.wrap_text("0", 3))
        self.assertEqual("01", base.wrap_text("01", 3))
        self.assertEqual("012", base.wrap_text("012", 3))
        self.assertEqual("012\n3", base.wrap_text("0123", 3))
        self.assertEqual("012\n34", base.wrap_text("01234", 3))
        self.assertEqual("012\n345", base.wrap_text("012345", 3))
        self.assertEqual("012\n345\n6", base.wrap_text("0123456", 3))

        # leading spaces are preserved until the leading spaces exceed ncolumns
        self.assertEqual(" \n  \n   \n\n", base.wrap_text(" \n  \n   \n    \n     ", 3))

        # trailing spaces are stripped if they exceed ncolumns
        self.assertEqual("1 \n2  \n3\n4", base.wrap_text("1 \n2  \n3   \n4    ", 3))

        # break at word break where possible
        self.assertEqual("0 23\n567\n890123\n4", base.wrap_text("0 23 567 8901234", 6))

        # strip out extra spaces around line breaks but not at beginning of a line
        self.assertEqual("0 23\n567\n89012", base.wrap_text("0 23\t    567  \t    89012  \t", 6))
        self.assertEqual("   34\n9012\n7", base.wrap_text("   34    9012    7", 6))

        self.assertEqual("0\n0 23\n0 23\n56 890\n", base.wrap_text("0\n0 23\n0 23 56 890\n", 6))

    def test_get_resource(self):
        self.assertEqual(b"1.1.9-SNAPSHOT", base.get_resource("base", "VERSION"))

        # Make sure get_resource() handles unknown resources in existing modules:
        self.assertEqual(None, base.get_resource("base", "VERSION-DOES-NOT-EXIST"))

        # Make sure get_resource() handles nested packages:
        self.assertEqual(b"Test resource\n", base.get_resource("base.package", "resource.txt"))
        self.assertEqual(None, base.get_resource("base.package", "DOES-NOT-EXIST"))

        # Make sure get_resource() handles unknown modules:
        self.assertEqual(None, base.get_resource("base.package.does.not.exist", "DOES-NOT-EXIST"))


if __name__ == '__main__':
    base.run(unittest.base_main)
