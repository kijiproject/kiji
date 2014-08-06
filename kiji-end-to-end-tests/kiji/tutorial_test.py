#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Common classes and methods for running tutorials."""

import logging
import re

from base import command

# ------------------------------------------------------------------------------


class KijiCommand(command.Command):
  """Shell command assuming a KijiBento environment."""

  def __init__(self, command, **kwargs):
    """Runs a Kiji command line.

    Requires the current working directory to be the KijiBento installation dir.

    Args:
      command: Shell command-line, as a single text string.
      **kwargs: Keyword arguments passed to the underlying Command.
    """
    args = [
        '/bin/bash',
        '-c',
        'source ./bin/kiji-env.sh --override > /dev/null 2>&1 && %s' % command,
    ]
    super(KijiCommand, self).__init__(*args, **kwargs)

# ------------------------------------------------------------------------------


def Expect(expect, actual):
  """Assertion.

  Args:
    expect: Expected value.
    actual: Actual value.
  Raises:
    AssertionError if the actual value does not match the expected value.
  """
  if expect != actual:
    logging.error('Expected %r, got %r', expect, actual)
    raise AssertionError('Expected %r, got %r' % (expect, actual))


# ------------------------------------------------------------------------------

def ExpectRegexMatch(expect, actual):
  """Asserts that a text strings matches a given regular expression.

  Args:
    expect: Regular expression to match.
    actual: Text string to assert the content of.
  Raises:
    AssertionError if the text does not match the regular expression.
  """
  if re.match(expect, actual):
    return True
  else:
    logging.error('%r does not match regex %r.', actual, expect)
    raise AssertionError('%r does not match regex %r.' % (actual, expect))

# ------------------------------------------------------------------------------

