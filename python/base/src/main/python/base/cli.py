#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""CLI tool actions."""

import abc
import logging
import os

from base import base


HELP_FLAG = base.make_tuple(
    name='HelpFlag',
    ADD_HANDLE=0,
    ADD_NO_HANDLE=1,
    NO_ADD=2,
)

HelpFlag = HELP_FLAG  # Deprecated - for compatibility only


class Error(Exception):
    """Errors used in this module."""
    pass


class Action(object, metaclass=abc.ABCMeta):
    """Abstract base class for CLI actions."""

    # Name of this action, optionally overridden by sub-classes.
    # Default behavior is to use the un-camel-cased class name.
    NAME = None

    @classmethod
    def get_name(cls):
        """Returns this action's name."""
        name = cls.NAME
        if name is None:
            name = cls.__name__
        return base.un_camel_case(name, separator='-')

    # Deprecated:
    GetName = get_name

    # Usage of this action, overridden by sub-classes:
    USAGE = """
        |Usage:
        |  %(this)s <flags> ...
        """

    def __init__(
        self,
        parent_flags=None,
        help_flag=HELP_FLAG.ADD_HANDLE,
    ):
        """Initializes the action.

        Most of the initialization relies on command-line flags.

        Args:
            parent_flags: Parent flags to inherit from.
            help_flag: Whether to add and/or handle a --help flag.
                    Default is to add and to handle a --help flag.
        """
        name = 'Flags for %s (%s.%s)' % (
            self.get_name(),
            type(self).__module__,
            type(self).__name__
        )
        self._flags = base.Flags(name=name, parent=parent_flags)
        self._help_flag = help_flag
        if self._help_flag in (HELP_FLAG.ADD_HANDLE, HELP_FLAG.ADD_NO_HANDLE):
            if 'help' not in self._flags:
                self._flags.add_boolean(
                    name='help',
                    default=False,
                    help='Print the help message for this action.',
                )
        self.register_flags()

    def register_flags(self):
        """Sub-classes may override this method to register flags."""
        deprecated = getattr(self, "RegisterFlags", None)
        if deprecated is not None:
            logging.warning(
                "Action.RegisterFlags() is deprecated, update %r and use Action.register_flags().",
                self.__class__)
            self.RegisterFlags()

        # Default implementation declares no flag:
        pass

    @property
    def flags(self):
        """Reports the flags declared in this CLI."""
        return self._flags

    def run(self, args):
        """Sub-classes must override this method to implement the action.

        Args:
            args: Command-line arguments for this action.
        Returns:
            Exit code.
        """
        logging.warning(
            "Action.Run() is deprecated, update %r and use Action.run() instead.",
            self.__class__)
        return self.Run(args)

    # @abc.abstractmethod
    def Run(self, args):
        """Deprecated, use run(args). """
        raise Error('Abstract method')

    def __call__(self, args, config_file=None):
        """Allows invoking an action as a function.

        Args:
            args: Command-line arguments specific to the action.
            config_file: Location of a file containing a json object with base flag values
                (values in args will take precedence over these values).
        Returns:
            Exit code.
        """
        if not self._flags.parse(args, config_file):
            return os.EX_USAGE

        if (self._help_flag == HELP_FLAG.ADD_HANDLE) and self.flags.help:
            print(base.strip_margin(self.USAGE.strip() % {
                'this': '%s %s' % (base.get_program_name(), self.get_name()),
            }))
            print()
            self.flags.print_usage()
            return os.EX_OK

        return self.run(self._flags.get_unparsed())


if __name__ == '__main__':
    raise Error('%r cannot be used as a standalone script.' % __name__)
