#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wrapper for shell commands."""

import logging
import os
import subprocess
import sys
import time


DEBUG_VERBOSE = 5


class Error(Exception):
  """Errors used in this module."""
  pass


class CommandError(Error):
  """Error while running a shell command."""
  pass


def NowMS():
  """Returns: the current time, in ms since the Epoch."""
  return int(1000 * time.time())


class Command(object):
  """Runs a shell command."""

  def __init__(
    self, *args,
    exit_code=None,
    work_dir=None,
    env=None,
    log_dir=None
  ):
    """Runs a shell command.

    Args:
      args: Command-line, as an array of command-line arguments.
      exit_code: Optional command exit code to require, or None.
      work_dir: Working directory. None means current workding directory.
      env: Optional environment variables for the subprocess, or None.
      log_dir: Optional directory where to write files capturing the
          command output/error streams.
          Defaults to the current working directory.
    Raises:
      CommandError: if the subprocess exit code does not match exit_code.
    """
    self._args = tuple(args)
    self._required_exit_code = exit_code
    self._work_dir = work_dir or os.getcwd()
    self._env = env
    log_dir = log_dir or os.getcwd()

    name = os.path.basename(self._args[0])
    timestamp = NowMS()

    self._input_path = '/dev/null'
    self._output_path = (
        os.path.join(log_dir, '%s.%d.%d.out' % (name, timestamp, os.getpid())))
    self._error_path = (
        os.path.join(log_dir, '%s.%d.%d.err' % (name, timestamp, os.getpid())))

    if logging.getLogger().level <= DEBUG_VERBOSE:
      logging.debug('Running command in %r:\n%s\nWith environment:\n%s' % (
          self._work_dir,
          ' \\\n\t'.join(map(repr, self._args)),
          '\n'.join(map(lambda kv: '\t%r: %r' % kv, sorted(self._env.items())))
      ))
    else:
      logging.debug('Running command in %r:\n%s' % (
          self._work_dir,
          ' \\\n\t'.join(map(repr, self._args)),
      ))

    with open(self._input_path, 'r') as input_file:
      with open(self._output_path, 'w') as output_file:
        with open(self._error_path, 'w') as error_file:
          self._process = subprocess.Popen(
              args=self._args,
              stdin=input_file,
              stdout=output_file,
              stderr=error_file,
              cwd=self._work_dir,
              env=self._env,
          )
          self._process.wait()
    with open(self._output_path, 'rb') as f:
      self._output_bytes = f.read()
    with open(self._error_path, 'rb') as f:
      self._error_bytes = f.read()

    if ((self._required_exit_code is not None)
        and (self.exit_code != self._required_exit_code)):
      raise CommandError(
          'Exit code %d does not match required code %d for command:\n%s' % (
          self.exit_code,
          self._required_exit_code,
          ' \\\n\t'.join(self._args),
      ))

  @property
  def output_bytes(self):
    """Returns: the command output stream as an array of bytes."""
    return self._output_bytes

  @property
  def output_text(self):
    """Returns: the command output stream as a text string."""
    return self.output_bytes.decode()

  @property
  def output_lines(self):
    """Returns: the command output stream as an array of text lines."""
    return self.output_text.split('\n')

  @property
  def error_bytes(self):
    """Returns: the command error stream as an array of bytes."""
    return self._error_bytes

  @property
  def error_text(self):
    """Returns: the command error stream as a text string."""
    return self.error_bytes.decode()

  @property
  def error_lines(self):
    """Returns: the command error stream as an array of text lines."""
    return self.error_text.split('\n')

  @property
  def exit_code(self):
    """Returns: the command exit code."""
    return self._process.returncode

