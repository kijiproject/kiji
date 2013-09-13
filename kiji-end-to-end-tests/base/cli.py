#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""CLI tool actions."""

import abc
import os

from base import base


class Action(object, metaclass=abc.ABCMeta):
  """Abstract base class for CLI actions."""

  # Name of this action, optionally overridden by sub-classes.
  # Default behavior is to use the un-camel-cased class name.
  NAME = None

  # Usage of this action, overridden by sub-classes:
  USAGE = """
    |Usage:
    |  %(this)s <flags> ...
    """

  def __init__(self):
    """Initializes the action.

    Most of the initialization relies on command-line flags.
    """
    self._flags = base.Flags()
    self._flags.AddBoolean(
        'help',
        default=False,
        help='Print the help message for this action.',
    )
    self.RegisterFlags()

  def RegisterFlags(self):
    """Sub-classes may override this method to register flags."""
    pass

  @property
  def flags(self):
    return self._flags

  @abc.abstractmethod
  def Run(self, args):
    """Sub-classes must override this method to implement the action.

    Args:
      args: Command-line arguments for this action.
    Returns:
      Exit code.
    """
    raise Error('Abstract method')

  def __call__(self, args):
    """Allows invoking an action as a function.

    Args:
      args: Command-line arguments specific to the action.
    Returns:
      Exit code.
    """
    if not self._flags.Parse(args):
      return os.EX_USAGE

    if self.flags.help:
      name = self.NAME
      if name is None:
        name = base.UnCamelCase(type(self).__name__)
      print(base.StripMargin(self.USAGE.strip() % {
          'this': '%s %s' % (base.GetProgramName(), name),
      }))
      print()
      self.flags.PrintUsage()
      return os.EX_OK

    return self.Run(self._flags.GetUnparsed())


if __name__ == '__main__':
  raise Error('%r cannot be used as a standalone script.' % args[0])
