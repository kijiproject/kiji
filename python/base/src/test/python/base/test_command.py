#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tests for module base.base"""

import logging
import os
import tempfile

from base import base
from base import command
from base import unittest


class TestCommand(unittest.BaseTestCase):

    def test_command(self):
        cmd = command.Command('/bin/ls', '-ald', '/tmp', exit_code=0)
        self.assertEqual(0, cmd.exit_code)
        self.assertTrue('/tmp' in cmd.output_text)

    def test_exit_code(self):
        command.Command('false', exit_code=1)

    def test_args_list(self):
        command.Command(args=['false'], exit_code=1)

    def test_collect_log(self):
        cmd = command.Command(
            args=['bash', '-c', 'echo -n Hello'],
            exit_code=0,
        )
        self.assertEqual("Hello", cmd.output_text)
        self.assertEqual("", cmd.error_text)

    def test_direct_log(self):
        cmd = command.Command(
            args=['bash', '-c', 'echo -n Hello'],
            direct_log=True,
            exit_code=0,
        )
        self.assertEqual("Hello", cmd.output_text)
        self.assertEqual("", cmd.error_text)

    def test_no_collect_log(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            logging.debug("Using temporary directory %r", temp_dir)
            cmd = command.Command(
                args=['bash', '-c', 'echo -n Hello'],
                log_dir=temp_dir,
                collect_log=False,
                exit_code=0,
            )
            self.assertEqual("Hello", cmd.output_text)
            self.assertEqual("", cmd.error_text)
            self.assertEqual(temp_dir, os.path.dirname(cmd.output_path))
            self.assertEqual(temp_dir, os.path.dirname(cmd.error_path))

if __name__ == '__main__':
    base.run(unittest.base_main)
