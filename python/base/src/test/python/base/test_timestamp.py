#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Unit-tests for the base.timestamp() and friends."""

import datetime
import os
import time

from base import base
from base import unittest


class TestTimestamp(unittest.BaseTestCase):

    def setUp(self):
        # Note: we are modifying a global, os.environ. This may limit running tests in parallel.
        # always run like PST/PDT.
        self._old_tz = os.environ.get("TZ", None)
        os.environ["TZ"] = "PST+08PDT,M4.1.0,M10.5.0"
        time.tzset()

    def tearDown(self):
        del os.environ["TZ"]
        if self._old_tz is not None:
            os.environ["TZ"] = self._old_tz
        time.tzset()

    def test_timestamp(self):
        dt = datetime.datetime(2014, 12, 11, 12, 13, 14, 654320)
        tstamp_str = base.timestamp(dt.timestamp())
        self.assertEqual("20141211-121314-654320-PST", tstamp_str)
        self.assertEqual(
            dt,
            datetime.datetime.fromtimestamp(base.parse_timestamp_str(tstamp_str))
        )

    def test_timestamp_dst(self):
        dt = datetime.datetime(2014, 6, 7, 8, 9, 10, 123456)
        tstamp = dt.timestamp()
        tstamp_str = base.timestamp(tstamp)
        self.assertEqual("20140607-080910-123456-PDT", tstamp_str)
        self.assertEqual(
            dt,
            datetime.datetime.fromtimestamp(base.parse_timestamp_str(tstamp_str))
        )

    def test_timestamp_from_utc(self):
        self.assertEqual(
            "20141211-041314-654320-PST",
            base.timestamp(base.parse_timestamp_str("20141211-121314-654320-UTC"))
        )
        self.assertEqual(
            "20140607-010910-123456-PDT",
            base.timestamp(base.parse_timestamp_str("20140607-080910-123456-UTC"))
        )

    def test_timestamp_to_utc(self):
        os.environ["TZ"] = "UTC+00"
        time.tzset()
        self.assertEqual(
            "20141211-201314-654320-UTC",
            base.timestamp(base.parse_timestamp_str("20141211-121314-654320-PST"))
        )
        self.assertEqual(
            "20140607-150910-123456-UTC",
            base.timestamp(base.parse_timestamp_str("20140607-080910-123456-PDT"))
        )

    def test_est_timestamp_to_utc(self):
        os.environ["TZ"] = "UTC+00"
        time.tzset()
        self.assertEqual(
            "20141211-201314-654320-UTC",
            base.timestamp(base.parse_timestamp_str("20141211-151314-654320-EST"))
        )
        self.assertEqual(
            "20140607-150910-123456-UTC",
            base.timestamp(base.parse_timestamp_str("20140607-110910-123456-EDT"))
        )

    def test_error_with_timestamp_from_europe_to_utc(self):
        os.environ["TZ"] = "UTC+00"
        time.tzset()
        # we only handle US timezones, so a french timezone should fail in conversion
        self.assertRaisesRegex(
            base.Error,
            "unknown GMT offset for timezone CET",
            base.parse_timestamp_str,
            "20140607-080910-123456-CET",
        )

    def test_error_with_timestamp_from_est_to_pst(self):
        # cannot convert from two non-UTC timezones
        self.assertRaisesRegex(
            base.Error,
            "unable to convert between two non-GMT timezones: EST and PST",
            base.parse_timestamp_str,
            "20140607-080910-123456-EST",
        )

# ------------------------------------------------------------------------------


if __name__ == '__main__':
    base.run(unittest.base_main)
