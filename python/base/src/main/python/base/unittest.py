#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

# Holds an abstract subclass of unittest.TestCase that interoperates with the base environment.
# This allows the unittest to be invoked from the command-line and also as a parameter to
# unittest (which plays better with IntelliJ).

import os
import sys
import unittest

from base import base


class BaseTestCase(unittest.TestCase):
    """Subclass of a unittest TestCase which allows for interoperation of the base environment and
    running tests as a parameter to unittest.
    """

    @classmethod
    def setUpClass(cls):
        """Called once for each suite.  This does the minimal setup of base which
        requires parsing FLAGS with no arguments and setting up logging.
        If invoked directly rather than through unittest, the FLAGS will already be parsed
        and logging will already be set up, so this should be a no-op.
        """

        if not base.FLAGS.has_parsed:
            base.FLAGS.parse(os.environ.get("TEST_ARGS", "").split())
        # these are the log levels that are set by the default global flag values
        # so if logging hasn't yet been setup, it'll be setup as if base.run were called with
        # no args.
        base.setup_logging(
            level=base.LogLevel.DEBUG,
            console_level=base.LogLevel.INFO,
            file_level=base.LogLevel.ALL,
        )


def base_main(args):
    """When called by the command-line, this method may be used as the target of base.run. It just
    adds a dummy value for the script name before calling unittest.main.

    A simple handler for __main__ would be:
        if __name__ == '__main__':
            base.run(base.unittest.base_main)

    Args:
        args: Unparsed arguments from base.run to pass onto unittest.

    Return: The exit code for the unittest.
    """
    args = list(args)
    args.insert(0, sys.argv[0])
    return unittest.main(argv=args)
