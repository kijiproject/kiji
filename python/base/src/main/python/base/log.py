#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Python logging system that supports keyword format parameters."""

import enum
import logging
import os
import sys
import traceback

from base import base


class Error(Exception):
    """Errors raised in this module."""
    pass


class Level(enum.IntEnum):
    """Log levels.

    Log statements and loggers are labeled with a log level.
    A logger ignores log messages whose level are below the logger's configured level.
    """

    ALL = 0
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    FATAL = 50


# --------------------------------------------------------------------------------------------------


def list_call_stack():
    """Lists the call stack entries in the current frame.

    Entries are ordered from the inner-most call to the outer-most main call.

    Yields:
        Call stack entry: (module name, file path, line number, function name).
    """
    frame = sys._getframe(3)
    try:
        while True:
            filepath = frame.f_code.co_filename
            module = frame.f_globals["__name__"]
            line = frame.f_lineno
            function = frame.f_code.co_name

            yield (module, filepath, line, function)

            frame = frame.f_back
    except AttributeError as err:
        pass


def list_stack_trace():
    """Lists the stack trace frames, from the most nested to the main entry point invocation.

    Yields:
        Formatted stack trace entries, including depth (0 is deepest), module, function,
        file and line.
    """
    for (depth, (module, filepath, line, function)) in enumerate(list_call_stack()):
        yield "frame[{}]\t{:<30s}\t{:<30s}\t{:s}:{:d}" \
            .format(depth, module, function, os.path.abspath(filepath), line)


def get_caller_location():
    """Returns: the code coordinate of the call site, as a tuple:
        (module name, file path, line number, function name).
    """
    for (module, filepath, line, function) in list_call_stack():
        if module != "base.log":
            return (module, filepath, line, function)

    return ("<unknown>", "<unknown>", None, "<unknown>")


# --------------------------------------------------------------------------------------------------


base.FLAGS.add_list(
    name="log-module-level",
    default=tuple(),
    delimiter=",",
    help=("Absolute, per-module log level. Overrides all other forms of log-level.\n"
          "Example: --log-module-level=base.base:info,base.cli:debug"),
)


class Logger(object):
    """Simple logger interface."""

    def __init__(self, level=Level.ALL, handlers=None):
        """Initializes a new logger.

        Args:
            level: Global fallback level filter.
                Any log statement below this level is discarded,
                unless a module-specific level overrides this level.
            handlers: Optional explicit collection of log handlers.
                Default is to use the handlers currently set in logging.root.
        """
        # Minimum level under which log messages are discarded:
        self._level = level

        # Map: module name -> Module configuration
        self._module_map = dict()

        for entry in base.FLAGS.log_module_level:
            (module, level) = entry.split("=", )
            try:
                level = int(level)
            except ValueError:
                try:
                    level = Level[level.upper()]
                except ValueError:
                    raise Error("Invalid module level specification: {!r}".format(entry))

            self.set_module_level(module, level)

        self._entry_format = \
            "{time!s} {level!s} {filepath:s}:{line!s} [{module:s}] {function:s}() : {message:s}\n"

        if handlers is None:
            handlers = logging.root.handlers
        self._handlers = tuple(handlers)

    def set_module_level(self, module, level):
        """Sets the log level for the specified Python module.

        Args:
            module: Python module to affect the logging level of.
            level: New logging level to set.
        """
        self._module_map[module] = level

    def log(self, level, fmt, *args, **kwargs):
        """Appends the specified log entry.

        Args:
            level: Level of the log entry to append (see the Level enum).
            fmt: Format of the log entry message.
            *args, **kwargs: Positional and named arguments of the log entry message format.
        """
        (module, filepath, line, function) = get_caller_location()
        if level < self._module_map.get(module, self._level):
            return

        # Normalize well-known log levels:
        try:
            level_str = Level(level).name
        except ValueError:
            level_str = "L{}".format(level)

        message = fmt.format(*args, **kwargs)
        entry = self._entry_format.format(
            time=base.timestamp(),
            level=level_str,
            filepath=os.path.abspath(filepath),
            line=line,
            module=module,
            function=function,
            message=message,
        )

        for handler in self._handlers:
            if level >= handler.level:
                handler.stream.write(entry)
                handler.stream.flush()

    def debug(self, fmt, *args, **kwargs):
        self.log(Level.DEBUG, fmt, *args, **kwargs)

    def info(self, fmt, *args, **kwargs):
        self.log(Level.INFO, fmt, *args, **kwargs)

    def warning(self, fmt, *args, **kwargs):
        self.log(Level.WARNING, fmt, *args, **kwargs)

    def error(self, fmt, *args, **kwargs):
        self.log(Level.ERROR, fmt, *args, **kwargs)

    def fatal(self, fmt, *args, **kwargs):
        self.log(Level.FATAL, fmt, *args, **kwargs)


# --------------------------------------------------------------------------------------------------


# Global, default logger:
_LOGGER = None

# Slots for global logger shortcuts:
log = debug = info = warning = error = fatal = None


def set_logger(logger):
    ns = globals()
    ns["_LOGGER"] = logger

    for method in ("log", "debug", "info", "warning", "error", "fatal"):
        ns[method] = getattr(logger, method)
