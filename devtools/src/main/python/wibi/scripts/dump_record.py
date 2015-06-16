#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tools to dump pickled records."""

import logging
import pickle

from base import base
from base import cli
from base import record


class Tool(cli.Action):
    def register_flags(self):
        pass

    def run(self, args):
        for filepath in args:
            logging.info("Dumping pickled record %r", filepath)
            with open(filepath, mode="rb") as f:
                rec = pickle.load(f)
            for name in sorted(rec):
                print("{0} = {1!r}".format(name, getattr(rec, name)))


def main(args):
    tool = Tool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == "__main__":
    base.run(main)
