#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-
"""Shared base utilities for Python applications.

Mainly:
 - Configures the logging system with both console and file logging.
 - Command-line flags framework allowing other modules to declare
   their own flags.
 - Program startup procedure.

Template for a Python application:
  from kiji import base

  FLAGS = base.FLAGS

  def Main(args):
    ...
    return os.EX_OK

  if __name__ == '__main__':
    base.Run(Main)
"""


import collections
import datetime
import json
import logging
import os
import random
import re
import subprocess
import sys
import tempfile
import time


class Error(Exception):
  """Errors used in this module."""
  pass


def NowMS():
  """Returns: the current time, in ms since the Epoch."""
  return int(1000 * time.time())


def NowDateTime():
  """Returns: the current time as a date/time object (local timezone)."""
  return datetime.datetime.now()


def Timestamp():
  """Reports the current time as a human-readable timestamp.

  Timestamp has micro-second precision.
  Formatted as 'yyyymmdd-hhmmss-mmmmmm-tz'.

  Returns:
    the current time as a human-readable timestamp.
  """
  now = NowDateTime()
  return '%04d%02d%02d-%02d%02d%02d-%06d-%s' % (
      now.year,
      now.month,
      now.day,
      now.hour,
      now.minute,
      now.second,
      now.microsecond,
      time.tzname[0],
  )


JSON_DECODER = json.JSONDecoder()
JSON_ENCODER = json.JSONEncoder(
  indent=2,
  sort_keys=True,
)


def JsonDecode(json_str):
  """Decodes a JSON encoded string into a Python value.

  Args:
    json_str: JSON encoded string.
  Returns:
    The Python value decoded from the specified JSON string.
  """
  return JSON_DECODER.decode(json_str)


def JsonEncode(py_value):
  """Encodes a Python value into a JSON string.

  Args:
    py_value: Python value to encode as a JSON string.
  Returns:
    The specified Python value encoded as a JSON string.
  """
  return JSON_ENCODER.encode(py_value)


def Truth(text):
  """Parses a human truth value.

  Accepts 'true', 'false', 'yes', 'no', 'y', 'n'.
  Parsing is case insensitive.

  Args:
    text: Input to parse.
  Returns:
    Parsed truth value as a bool.
  """
  lowered = text.lower()
  if lowered in frozenset(['y', 'yes', 'true']):
    return True
  elif lowered in frozenset(['n', 'no', 'false']):
    return False
  else:
    raise Error('Invalid truth value: %r' % text)


def RandomAlphaNumChar():
  """Generates a random character in [A-Za-z0-9]."""
  num = random.randint(0, 26 + 26 + 10)
  if num < 26:
    return chr(num + 65)
  num -= 26
  if num < 26:
    return chr(num + 97)
  return chr(num + 48)


def RandomAlphaNumWord(length):
  """Generates a random word with the specified length.

  Uses characters from the set [A-Za-z0-9].

  Args:
    length: Length of the word to generate.
  Returns:
    A random word of the request length.
  """
  return ''.join([RandomAlphaNumChar() for _ in range(0, length)])


def StripPrefix(string, prefix):
  """Strips a required prefix from a given string.

  Args:
    string: String required to start with the specified prefix.
    prefix: Prefix to remove from the string.
  Returns:
    The given string without the prefix.
  """
  assert string.startswith(prefix)
  return string[len(prefix):]


def StripOptionalPrefix(string, prefix):
  """Strips an optional prefix from a given string.

  Args:
    string: String, potentially starting with the specified prefix.
    prefix: Prefix to remove from the string.
  Returns:
    The given string with the prefix removed, if applicable.
  """
  if string.startswith(suffix):
    string = string[len(prefix):]
  return string


def StripSuffix(string, suffix):
  """Strips a required suffix from a given string.

  Args:
    string: String required to end with the specified suffix.
    suffix: Suffix to remove from the string.
  Returns:
    The given string with the suffix removed.
  """
  assert string.endswith(suffix)
  return string[:-len(suffix)]


def StripOptionalSuffix(string, suffix):
  """Strips an optional suffix from a given string.

  Args:
    string: String required to end with the specified suffix.
    suffix: Suffix to remove from the string.
  Returns:
    The given string with the suffix removed, if applicable.
  """
  if string.endswith(suffix):
    string = string[:-len(suffix)]
  return string


def StripMargin(text, separator='|'):
  """Strips the left margin from a given text block.

  Args:
    text: Multi-line text with a delimited left margin.
    separator: Separator used to delimit the left margin.
  Returns:
    The specified text with the left margin removed.
  """
  lines = text.split('\n')
  lines = map(lambda line: line.split(separator, 1)[-1], lines)
  return '\n'.join(lines)


def GetProgramName():
  """Returns: this program's name."""
  return os.path.basename(sys.argv[0])


def ShellCommandOutput(command):
  """Runs a shell command and returns its output.

  Args:
    command: Shell command to run.
  Returns:
    The output from the shell command (stderr merged with stdout).
  """
  process = subprocess.Popen(
      args=['/bin/bash', '-c', command],
      stdout=subprocess.PIPE,
      stderr=subprocess.STDOUT,
  )
  process.wait()
  output = process.stdout.read().decode()
  assert process.returncode == 0, (
      'Shell command failed: %r : %s' % (command, output))
  return output


# ------------------------------------------------------------------------------


RE_FLAG = re.compile(r'^--?([^=]+)(?:=(.*))?$')


class Flags(object):
  """Wrapper for command-line flags."""

  class StringFlag(object):
    """Basic string flag parser."""

    def __init__(self, name, default=None, help=None):
      self._name = name
      self._value = default
      self._default = default
      self._help = help

    def Parse(self, argument):
      """Parses the command-line argument.

      Args:
        argument: Command-line argument, as a string.
      """
      self._value = argument

    @property
    def name(self):
      return self._name

    @property
    def value(self):
      return self._value

    @property
    def default(self):
      return self._default

    @property
    def help(self):
      return self._help

  class IntegerFlag(StringFlag):
    def Parse(self, argument):
      self._value = int(argument)

  class BooleanFlag(StringFlag):
    def Parse(self, argument):
      if argument is None:
        self._value = True
      else:
        self._value = Truth(argument)

  def __init__(self):
    # Map: flag name -> flag definition
    self._defs = {}

    # After parsing, tuple of unparsed arguments:
    self._unparsed = None

  def Add(self, flag_def):
    assert (flag_def.name not in self._defs), (
        'Flag %r already defined' % flag_def.name)
    self._defs[flag_def.name] = flag_def

  def AddInteger(self, name, **kwargs):
    self.Add(Flags.IntegerFlag(name, **kwargs))

  def AddString(self, name, **kwargs):
    self.Add(Flags.StringFlag(name, **kwargs))

  def AddBoolean(self, name, **kwargs):
    self.Add(Flags.BooleanFlag(name, **kwargs))

  def Parse(self, args):
    """Parses the command-line arguments.

    Args:
      args: List of command-line arguments.
    Returns:
      Whether successful.
    """
    unparsed = []

    skip_parse = False

    for arg in args:
      if arg == '--':
        skip_parse = True
        continue

      if skip_parse:
        unparsed.append(arg)
        continue

      match = RE_FLAG.match(arg)
      if match is None:
        unparsed.append(arg)
        continue

      key = match.group(1)
      value = match.group(2)

      if key not in self._defs:
        unparsed.append(arg)
        continue

      self._defs[key].Parse(value)

    self._unparsed = tuple(unparsed)
    return True

  def __getattr__(self, name):
    assert (self._unparsed is not None), (
        'Flags have not been parsed yet: cannot access flag %r' % name)
    return self._defs[name].value

  def GetUnparsed(self):
    assert (self._unparsed is not None), 'Flags have not been parsed yet'
    return self._unparsed

  def PrintUsage(self):
    print('Flags:')
    for (name, flag) in sorted(self._defs.items()):
      print(' --%-30s\t%s\n    Default: %s\n' % (name, flag.help, flag.default))


FLAGS = Flags()


# ------------------------------------------------------------------------------


def MakeTuple(name, **kwargs):
  """Creates a read-only named-tuple with the specified key/value pair.

  Args:
    name: Name of the named-tuple.
    **kwargs: Key/value pairs in the named-tuple.
  Returns:
    A read-only named-tuple with the specified name and key/value pairs.
  """
  tuple_class = collections.namedtuple(
      typename=name,
      field_names=kwargs.keys(),
  )
  return tuple_class(**kwargs)


LogLevel = MakeTuple('LogLevel',
  FATAL=50,
  ERROR=40,
  WARNING=30,
  INFO=20,
  DEBUG=10,
  DEBUG_VERBOSE=5,
)


def ParseLogLevelFlag(level):
  """Parses a logging level command-line flag.

  Args:
    level: Logging level command-line flag (string).
  Returns:
    Logging level (integer).
  """
  log_level = getattr(LogLevel, level.upper(), None)
  if type(log_level) == int:
    return log_level

  try:
    return int(level)
  except ValueError:
    level_names = sorted(LogLevel._asdict().keys())
    raise Error('Invalid logging-level %r. Use one of %s or an integer.'
                % (level, ', '.join(level_names)))


FLAGS.AddString(
    'log_level',
    default='DEBUG_VERBOSE',
    help=('Root logging level (integer or named level). '
          'Overrides specific logging levels.'),
)

FLAGS.AddString(
    'log_console_level',
    default='INFO',
    help='Console specific logging level (integer or named level).',
)

FLAGS.AddString(
    'log_file_level',
    default='DEBUG_VERBOSE',
    help='Log-file specific logging level (integer or named level).',
)

FLAGS.AddString(
    'log_dir',
    default=None,
    help='Directory where to write logs.',
)


# ------------------------------------------------------------------------------


class _Terminal(object):
  """Map of terminal colors."""

  # Escape sequence to clear the screen:
  clear = '\033[2J'

  # Escape sequence to move the cursor to (y,x):
  move = '\033[%s;%sH'

  def MoveTo(self, x, y):
    """Makes an escape sequence to move the cursor."""
    return _Terminal.move % (y,x)

  normal    = '\033[0m'
  bold      = '\033[1m'
  underline = '\033[4m'
  blink     = '\033[5m'
  reverse   = '\033[7m'

  FG = MakeTuple('ForegroundColor',
    black   = '\033[01;30m',
    red     = '\033[01;31m',
    green   = '\033[01;32m',
    yellow  = '\033[01;33m',
    blue    = '\033[01;34m',
    magenta = '\033[01;35m',
    cyan    = '\033[01;36m',
    white   = '\033[01;37m',
  )

  BG = MakeTuple('BackgroundColor',
    black   = '\033[01;40m',
    red     = '\033[01;41m',
    green   = '\033[01;42m',
    yellow  = '\033[01;43m',
    blue    = '\033[01;44m',
    magenta = '\033[01;45m',
    cyan    = '\033[01;46m',
    white   = '\033[01;47m',
  )

  @property
  def columns(self):
    """Reports the number of columns in the calling shell.

    Returns:
      The number of columns in the terminal.
    """
    return int(ShellCommandOutput('tput cols'))

  @property
  def lines(self):
    """Reports the number of lines in the calling shell.

    Returns:
      The number of lines in the terminal.
    """
    return int(ShellCommandOutput('tput lines'))


Terminal = _Terminal()


# ------------------------------------------------------------------------------


def Run(main):
  """Runs a Python program's Main() function.

  Args:
    main: Main function, with an expected signature like
      exit_code = main(args)
      where args is a list of the unparsed command-line arguments.
  """
  # Parse command-line arguments:
  program_name = GetProgramName()
  if not FLAGS.Parse(sys.argv[1:]):
    FLAGS.PrintUsage()
    return os.EX_USAGE

  # Initialize the logging system:
  try:
    log_root_level = ParseLogLevelFlag(FLAGS.log_level)
    log_console_level = ParseLogLevelFlag(FLAGS.log_console_level)
    log_file_level = ParseLogLevelFlag(FLAGS.log_file_level)
  except Error as err:
    print(err)
    return os.EX_USAGE

  log_formatter = logging.Formatter(
      '%(asctime)s %(levelname)s %(filename)s:%(lineno)s : %(message)s')

  logging.root.setLevel(log_root_level)

  console_handler = logging.StreamHandler()
  console_handler.setFormatter(log_formatter)
  console_handler.setLevel(log_console_level)
  logging.root.addHandler(console_handler)

  # Initialize log dir:
  timestamp = Timestamp()
  pid = os.getpid()

  if FLAGS.log_dir is None:
    tmp_dir = os.path.join('/tmp', program_name)
    if not os.path.exists(tmp_dir): os.makedirs(tmp_dir)
    FLAGS.log_dir = tempfile.mkdtemp(
        prefix='%s.%d.' % (timestamp, pid),
        dir=tmp_dir)
  logging.info('Using log dir: %s', FLAGS.log_dir)
  if not os.path.exists(FLAGS.log_dir):
    os.makedirs(FLAGS.log_dir)

  log_file = os.path.join(FLAGS.log_dir,
                          '%s.%s.%d.log' % (program_name, timestamp, pid))

  file_handler = logging.FileHandler(filename=log_file)
  file_handler.setFormatter(log_formatter)
  file_handler.setLevel(log_file_level)
  logging.root.addHandler(file_handler)

  # Run program:
  sys.exit(main(FLAGS.GetUnparsed()))
