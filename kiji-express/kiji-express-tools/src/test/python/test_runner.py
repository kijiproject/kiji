#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

import argparse
import express
import logging
import sys
import tempfile
import unittest


def make_arg_parser():
    parser = argparse.ArgumentParser(
        description="Unit-test command-line interface.",
        add_help=False,
    )
    parser.add_argument('--log-level', default="info", help='Logging level.')
    return parser


def main(args):
    parser = make_arg_parser()
    (flags, unparsed_args) = parser.parse_known_args(args[1:])

    try:
        log_level = express.parse_log_level(flags.log_level)
        express.setup_logging(log_level=log_level)
    except Error as err:
        print(err)
        return os.EX_USAGE

    # Run tests:
    unparsed_args.insert(0, args[0])
    logging.debug('Running unittest.main(%r)', unparsed_args)
    sys.exit(unittest.main(module=None, argv=unparsed_args))


if __name__ == "__main__":
    main(sys.argv)
