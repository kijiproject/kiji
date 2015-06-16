#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tests for the module: base.log."""

import io
import logging
import sys

from base import base
from base import log
from base import unittest


class TestLog(unittest.BaseTestCase):
    def test_log(self):
        sio = io.StringIO()
        handler = logging.StreamHandler(stream=sio)
        logger = log.Logger(handlers=[handler])
        logger.set_module_level("base.test_log", log.Level.INFO)
        logger.info("Fo{x}", x="o")  # == Foo
        logger.debug("Bar")
        handler.close()

        sio.seek(0)
        stmt = sio.read()
        self.assertIn("/base/test_log.py:21 [base.test_log] test_log() : Foo\n", stmt)
        self.assertIn(" INFO ", stmt)
