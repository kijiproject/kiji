#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Unit-tests for the Record object."""

import logging
import tempfile

from base import base
from base import record
from base import unittest

UNDEFINED = base.UNDEFINED


class TestRecord(unittest.BaseTestCase):
    def testConfig(self):
        conf = record.Record()
        self.assertEqual([], dir(conf))

        logging.info('Step 1')
        self.assertFalse('x' in conf)
        self.assertEqual(UNDEFINED, conf.x)
        self.assertEqual(UNDEFINED, conf.get('x'))

        logging.info('Step 2')
        conf.x = 1
        self.assertTrue('x' in conf)
        self.assertEqual(1, conf.x)
        self.assertEqual(1, conf['x'])
        self.assertEqual(['x'], dir(conf))

        logging.info('Step 3')
        conf.x = 2
        self.assertTrue('x' in conf)
        self.assertEqual(2, conf.x)
        self.assertEqual(2, conf['x'])
        self.assertEqual(['x'], dir(conf))

        logging.info('Step 4')
        del conf['x']
        self.assertFalse('x' in conf)
        self.assertEqual(UNDEFINED, conf.x)
        self.assertEqual(UNDEFINED, conf.get('x'))
        self.assertEqual([], dir(conf))

        conf.x = 1
        del conf.x
        self.assertFalse('x' in conf)

    def test_write_load(self):
        conf = record.Record()
        conf.x = 1

        with tempfile.NamedTemporaryFile() as f:
            conf.write_to_file(f.name)
            logging.info('Writing record: %r', conf)
            new = record.load_from_file(f.name)
            logging.info('Loaded record: %r', new)
            self.assertEqual(1, new.x)

    def test_iter(self):
        """The names of parameters set on the record should be in iter(record)."""
        conf = record.Record()
        conf.x = 1
        conf.y = 2
        self.assertSetEqual({"x", "y"}, set(iter(conf)))


# ------------------------------------------------------------------------------


if __name__ == '__main__':
    base.run(unittest.base_main)
