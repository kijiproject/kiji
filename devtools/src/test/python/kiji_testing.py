#!/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Kiji testing base tools.

Usage:

    $ TEST_ARGS="--log-console-level=debug" python3 -m unittest test_x

For unit-test help:

    $ python3 -m unittest -h
"""

import os
import sys

from base import base


FLAGS = base.FLAGS
FLAGS.parse(os.environ.get("TEST_ARGS", "").split())


_INITIALIZED = False


def init_logging():
    global _INITIALIZED
    if _INITIALIZED:
        return
    _INITIALIZED = True

    try:
        log_level = base.parse_log_level_flag(FLAGS.log_level)
        log_console_level = base.parse_log_level_flag(FLAGS.log_console_level)
        log_file_level = base.parse_log_level_flag(FLAGS.log_file_level)
    except base.Error as err:
        print(err)
        return os.EX_USAGE

    base.setup_logging(
        level=log_level,
        console_level=log_console_level,
        file_level=log_file_level,
    )


init_logging()
