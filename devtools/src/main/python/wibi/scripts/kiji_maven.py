#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

"""Wraps Maven to work within a workspace."""

import logging
import os
import subprocess

from base import base
from base import cli

from wibi.build import workspace


class MavenTool(cli.Action):
    def register_flags(self):
        self.flags.add_string(
            name="local-repo",
            default=None,
            help="Maven local repository to use. Defaults to the workspace repository.",
        )

    def run(self, args):
        local_repo = self.flags.local_repo
        if local_repo is None:
            wkspc_root = workspace.find_workspace_root(os.getcwd())
            if wkspc_root is None:
                logging.warn("Not running within a workspace?")
                local_repo = os.path.join(os.environ["HOME"], ".m2", "repository")
            else:
                logging.info("Running in workspace %r", wkspc_root)
                local_repo = os.path.join(wkspc_root, "output", "maven_repository")

        command = [
            "mvn",
            "-Dmaven.repo.local=%s" % local_repo,
        ] + list(args)

        return subprocess.call(args=command)


def main(args):
    tool = MavenTool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == '__main__':
    base.run(main)
