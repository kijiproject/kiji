#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tests for the Java linker.

Invoke as follows:

    $ [TEST_ARGS="--log-console-level=debug ..."] python3 -m unittest test_java_linker [-v]
"""

import logging
import os
import tempfile
import unittest

import kiji_testing
from base import base

from wibi.build import java_linker


class TestJavaLinker(unittest.TestCase):
    def test_linker(self):
        """Absolute minimal test for the java linker. More will come."""
        with tempfile.TemporaryDirectory() as temp_dir:
            bin_path = os.path.join(temp_dir, "binary")
            self.assertFalse(os.path.exists(bin_path))
            linker = java_linker.JavaLinker(
                output_path=bin_path,
                maven_repo=None,
            )
            linker.link(
                artifacts=tuple(),
                main_class=None,
            )
            self.assertTrue(os.path.exists(bin_path))
            os.system("{} -version".format(bin_path))
