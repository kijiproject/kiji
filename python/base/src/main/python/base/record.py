#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

import pickle

from base import base


class Record(object):
    """Record that behaves like a dictionary and an object."""

    def __init__(self, params=None):
        """Initializes a new record."""
        if params is None:
            params = dict()
        self.__dict__['_params'] = params

    def __setattr__(self, key, value):
        self._params[key] = value

    def __getattr__(self, key):
        return self._params.get(key, base.UNDEFINED)

    def __delattr__(self, key):
        try:
            del self._params[key]
        except KeyError:
            raise AttributeError(key)

    def __contains__(self, key):
        return key in self._params

    def __setitem__(self, key, value):
        self._params[key] = value

    def __delitem__(self, key):
        del self._params[key]

    def __getitem__(self, key):
        return self._params[key]

    def get(self, key, default=base.UNDEFINED):
        return self._params.get(key, default)

    def copy(self):
        """Returns: a copy of this record."""
        return self.update()

    def update(self, **kwargs):
        """Creates a copy of this record with some parameters updated.

        Args:
            kwargs: Collection of parameters to set.
        Returns:
            A copy of this record with the specified parameters updated.
        """
        params = dict(self._params)
        params.update(kwargs)
        return Record(params)

    def __dir__(self):
        """Lists the fields in this record.

        Overrides the operator dir(record).

        Returns:
            List of the fields in this record.
        """
        return self._params.keys()

    def __iter__(self):
        """Iterator over the field names in this record.

        Overrides the operator iter(record) and the behavior of 'for...in record'.
        This should be used over dir(record) for most iteration algorithms.

        Returns:
            Iterator over the fields in this record.
        """
        return iter(self._params)

    def __str__(self):
        return (
            'Record{%s}' % ','.join(
                map(lambda item: '%s=%s' % item, sorted(self._params.items()))
            )
        )

    def __repr__(self):
        return (
            'Record(%s)' % ','.join(
                map(lambda item: '%s=%r' % item, sorted(self._params.items()))
            )
        )

    def __getnewargs__(self):
        """Customize deserialization through pickle."""
        return ()

    def __getstate__(self):
        """Customize serialization through pickle."""
        return dict(_params=self._params)

    def __setstate__(self, state):
        """Customize deserialization through pickle."""
        self.__dict__['_params'] = state['_params']

    def write_to_file(self, file_path):
        with open(file_path, 'wb') as f:
            pickler = pickle.Pickler(f)
            pickler.dump(self)


def load_from_file(file_path):
    """Loads a record from a file.

    Args:
        file_path: Path of the file to load the record from.
    Returns:
        The record loaded from the given file.
    """
    with open(file_path, 'rb') as f:
        pickler = pickle.Unpickler(f)
        return pickler.load()
