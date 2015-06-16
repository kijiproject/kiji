#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Manages devtools configuration."""

import logging
import os
import sys

from base import base


FLAGS = base.FLAGS


class Error(Exception):
    """Errors used in this module."""
    pass


# Default path of the configuration file:
DEFAULT_CONFIG_PATH = os.path.join(os.environ['HOME'], '.config', 'devtools-conf.json')


FLAGS.add_string(
    name='config_file',
    default=DEFAULT_CONFIG_PATH,
    help='Path of the persistent configuration file.',
)


Keys = base.make_tuple('Keys',
    REVIEWBOARD = 'reviewboard_servers',
    JIRA = 'jira_servers',
    CONFLUENCE = 'confluence_servers',
)


class Configuration(object):
    """Persistent configuration.

    Configuration is persisted as a JSON serialized Avro record.
    The configuration record is defined in 'avro/devtools.avdl'.
    """

    # Singleton instance:
    INSTANCE = None

    @classmethod
    def get_global(cls):
        """Returns: the global configuration singleton."""
        if cls.INSTANCE is None:
            cls.INSTANCE = Configuration(config_file=FLAGS.config_file)
        return cls.INSTANCE

    def __init__(self, config_file=None):
        """Initializes a new configuration with the specified file."""
        if config_file is None:
            config_file = CONFIG_PATH

        self._config_file = config_file
        self._config = self._read_from_file(path=self._config_file)
        if self._config is None:
            # New configuration
            self._config = {}

        self._jira_server_map = None
        self._reviewboard_server_map = None
        self._confluence_server_map = None

    @classmethod
    def _read_from_file(cls, path):
        """Reads the configuration from the config file.

        Args:
            path: Path of the file to read from.
        Returns:
            The decoded DevToolsConfiguration Avro record (JSON object).
            None if the configuration file does not exist.
        """
        logging.debug('Reading configuration from file: %r', path)
        if not os.path.exists(path): return None
        with open(path, 'rt') as f:
            return base.json_decode(f.read())

    @staticmethod
    def _write_to_file(config, path):
        """Writes the configuration to the config file.

        Args:
            config: The configuration dictionary.
            path: Path of the file to write to.
        """
        if os.path.exists(path):
            backup = '%s.backup.%d' % (path, base.now_ms())
            os.rename(path, backup)
            os.chmod(path=backup, mode=0o600)
        logging.debug('Writing configuration to file: %r', path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wt') as f:
            json = base.json_encode(config)
            f.write(json)
        os.chmod(path=path, mode=0o600)

    def write(self):
        """Writes the configuration to disk."""
        self._write_to_file(self._config, self._config_file)

    @property
    def jira(self):
        """Returns: a map of the JIRA server configurations."""
        if self._jira_server_map is None:
            self._jira_server_map = dict(map(
                lambda srv: (srv['id'], srv),
                self._config.get(Keys.JIRA, ())))
        return self._jira_server_map

    @property
    def reviewboard(self):
        """Returns: a map of the ReviewBoard server configurations."""
        if self._reviewboard_server_map is None:
            self._reviewboard_server_map = dict(map(
                lambda srv: (srv['id'], srv),
                self._config.get(Keys.REVIEWBOARD, ())))
        return self._reviewboard_server_map

    @property
    def confluence(self):
        """Returns: a map of the Confluence server configurations."""
        if self._confluence_server_map is None:
            self._confluence_server_map = dict(map(
                lambda srv: (srv['id'], srv),
                self._config.get(Keys.CONFLUENCE, ())))
        return self._confluence_server_map

    def add_jira(self, jira_server_conf):
        self._jira_server_map = None
        self._config.setdefault(Keys.JIRA, []).append(jira_server_conf)

    def add_reviewboard(self, rb_server_conf):
        self._reviewboard_server_map = None
        self._config.setdefault(Keys.REVIEWBOARD, []).append(rb_server_conf)

    def add_confluence(self, confluence_server_conf):
        self._confluence_server_map = None
        self._config.setdefault(Keys.CONFLUENCE, []).append(confluence_server_conf)

    def __getitem__(self, key):
        return self._config.get(key)

    def __setattr__(self, key, value):
        if key.startswith("_"):
            self.__dict__[key] = value
        else:
            self._config[key] = value

    def __getattr__(self, key):
        return self._config.get(key, base.UNDEFINED)

    def __delattr__(self, key):
        try:
            del self._config[key]
        except KeyError as err:
            raise AttributeError(key)

    def __contains__(self, key):
        return key in self._config

    def __setitem__(self, key, value):
        self._config[key] = value

    def __delitem__(self, key):
        del self._config[key]

    def __getitem__(self, key):
        return self._config[key]

    def get(self, key, default=base.UNDEFINED):
      return self._config.get(key, default)

    # Deprecated API:
    Get = get_global
    Write = write
    AddJira = add_jira
    AddReviewBoard = add_reviewboard
    AddConfluence = add_confluence


def get():
    """Returns: the global wibi-devtools configuration."""
    return Configuration.get_global()


# ------------------------------------------------------------------------------

def main(args):
    conf = Configuration.Get()
    print(base.json_encode(conf._config))
    conf.Write()


if __name__ == '__main__':
    base.run(main)
