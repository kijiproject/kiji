#!/usr/bin/env python3
# -*- mode: python; coding: utf-8 -*-

"""Wrapper around the Maven CLI."""

import os
import sys
import tempfile

from base import base
from base import cli
from base import command
from base import log

from wibi.build import workflow_task
from wibi.maven import artifact
from wibi.maven import maven_repo


FLAGS = base.FLAGS


class Error(Exception):
  """Errors used in this module."""
  pass


# --------------------------------------------------------------------------------------------------


def log_maven_output(lines):
    for line in lines:
        line = line.strip()
        if line.startswith("[WARNING]"):
            line = line[10:].rstrip()
            if len(line) > 0:
                log.warning("{!r}", line)
        elif line.startswith("[ERROR]"):
            line = line[8:].rstrip()
            if len(line) > 0:
                log.error("{!r}", line)
        else:
            pass


# --------------------------------------------------------------------------------------------------


def list_classpath_entries(
    coordinates,
    scope="runtime",
    local_repo=None,
):
    """Lists the classpath entries.

    Args:
        coordinates: Collection of Artifact to examine.
        scope: Optional Maven scope to determine the classpath for. Default is runtime.
        local_repo: Optional explicit Maven local repository to use.
    Returns:
        An iterable of classpath entries.
    """
    with tempfile.NamedTemporaryFile(prefix="pom.", suffix=".xml") as pom_file, \
        tempfile.NamedTemporaryFile(prefix="maven-classpath.", suffix=".txt") as tfile:

        with open(pom_file.name, "wt") as file:
            file.write(workflow_task.format_pom_file(
                artf=artifact.Artifact("unknown", "unknown", "unknown"),
                compile_deps=coordinates,
            ))

        args = [
            "mvn",
            "--batch-mode",
            "--file={}".format(pom_file.name),
        ]
        if local_repo is not None:
            args += ["-Dmaven.repo.local={}".format(local_repo)]

        args += [
            "dependency:build-classpath",
            "-DincludeScope={}".format(scope),
            "-Dmdep.cpFile={}".format(tfile.name),
        ]
        cmd = command.Command(args=args)
        log_maven_output(cmd.output_lines)

        with open(tfile.name, "rt") as file:
            return file.read().split(":")


# --------------------------------------------------------------------------------------------------


def list_dependencies(
    coordinates,
    scope="runtime",
    local_repo=None,
):
    """Lists the resolved dependencies.

    Args:
        coordinates: Collection of Artifact to examine.
        scope: Optional Maven scope to determine the classpath for. Default is runtime.
        local_repo: Optional explicit Maven local repository to use.
    Returns:
        An iterable of Maven artifact coordinates.
    """
    with tempfile.NamedTemporaryFile(prefix="pom.", suffix=".xml") as pom_file, \
        tempfile.NamedTemporaryFile(prefix="maven-dependency-resolved.", suffix=".txt") as tfile:

        with open(pom_file.name, "wt") as file:
            file.write(workflow_task.format_pom_file(
                artf=artifact.Artifact("unknown", "unknown", "unknown"),
                compile_deps=coordinates,
            ))

        args = [
            "mvn",
            "--batch-mode",
            "--file={}".format(pom_file.name),
        ]
        if local_repo is not None:
            args += ["-Dmaven.repo.local={}".format(local_repo)]

        args += [
            "dependency:list",
            "-DoutputFile={}".format(tfile.name),
            "-Dsort=true",
            "-DincludeScope={}".format(scope),
        ]
        cmd = command.Command(args=args)
        log_maven_output(cmd.output_lines)

        with open(tfile.name, "rt") as file:
            lines = file.readlines()
            lines = map(str.strip, lines)
            lines = filter(None, lines)
            lines = filter(lambda line: (line != "The following files have been resolved:"), lines)
            lines = map(artifact.parse_artifact, lines)
            return sorted(lines)


# --------------------------------------------------------------------------------------------------


def get_dependency_tree(
    coordinates,
    scope=None,
    local_repo=None,
):
    """Reports the dependency tree as a text representation.

    Args:
        coordinates: Collection of Artifact to examine.
        scope: Optional Maven scope to determine the classpath for.
        local_repo: Optional explicit Maven local repository to use.
    Returns:
        A text representation of the Maven dependency tree.
    """
    with tempfile.NamedTemporaryFile(prefix="pom.", suffix=".xml") as pom_file, \
        tempfile.NamedTemporaryFile(prefix="maven-dependency-tree.", suffix=".txt") as tfile:

        with open(pom_file.name, "wt") as file:
            file.write(workflow_task.format_pom_file(
                artf=artifact.Artifact("root", "root", "0"),
                compile_deps=coordinates,
            ))

        args = [
            "mvn",
            "--batch-mode",
            "--file={}".format(pom_file.name),
        ]
        if local_repo is not None:
            args += ["-Dmaven.repo.local={}".format(local_repo)]

        args += [
            "dependency:tree",
            "-DoutputFile={}".format(tfile.name),
            "-Dverbose",
        ]

        if (scope is not None) and (len(scope) > 0):
            args.append("-Dscope={}".format(scope))

        cmd = command.Command(args=args)
        log_maven_output(cmd.output_lines)

        with open(tfile.name, "rt") as file:
            return file.read()


# --------------------------------------------------------------------------------------------------


class MavenAction(cli.Action):
    """Base class for Maven helper tools."""

    def __init__(self, repo):
        super(MavenAction, self).__init__()
        self._repo = repo

    @property
    def repo(self):
        """Returns: the default Maven repository to interact with."""
        return self._repo


# ------------------------------------------------------------------------------


class FetchArtifact(MavenAction):
    """Fetches a specific Maven artifact."""

    USAGE = """
        |Fetches a specific Maven artifact.
        |
        |Usage: %(this)s [--artifact=]group:artifact:version[[:classifier]:type]
    """

    def register_flags(self):
        self.flags.add_string(
            name="artifact",
            default=None,
            help=("The coordinate of the artifact to download.\n"
                  "e.g. 'group:artifact:version[[:classifier]:type]'.\n"
                  "classifier defaults to nothing.\n"
                  "type defaults to 'jar'."),
        )

    def run(self, args):
        coord = self.flags.artifact
        if (coord is None) and (len(args) > 0):
            coord, args = args[0], args[1:]
        assert (coord is not None), "Must specify an artifact to download."

        assert (len(args) == 0), "Unexpected command-line arguments: {!r}".format(args)

        artf = artifact.parse_artifact(coord)
        log.info("Looking up artifact {!s}", artf)

        print(self.repo.get(artf))
        return os.EX_OK


# --------------------------------------------------------------------------------------------------


class ListArtifactVersions(MavenAction):
    """Lists the versions of a given Maven artifact."""

    USAGE = """
        |Lists the versions of a given Maven artifact.
        |
        |Usage: %(this)s [--artifact=]group:artifact[[:classifier]:type]
    """

    def register_flags(self):
        self.flags.add_string(
            name="artifact-key",
            default=None,
            help=("Key of the artifact to search for.\n"
                  "e.g. 'group:artifact[[:classifier]:type]'.\n"
                  "classifier defaults to nothing.\n"
                  "type defaults to 'jar'."),
        )

    def run(self, args):
        coord = self.flags.artifact_key
        if (coord is None) and (len(args) > 0):
            coord, args = args[0], args[1:]
        assert (coord is not None), "Must specify the artifact to list versions of."

        assert (len(args) == 0), "Unexpected command-line arguments: {!r}".format(args)

        artf_key = artifact.parse_artifact_key(coord)
        log.info("Scanning artifacts matching key: {}", artf_key)

        versions = list()
        for remote in self.repo.remotes:
            versions.extend(
                remote.list_versions(group=artf_key.group_id, artifact=artf_key.artifact_id))
        print("\n".join(sorted(frozenset(versions))))
        return os.EX_OK


# --------------------------------------------------------------------------------------------------


class BuildClasspath(MavenAction):
    def register_flags(self):
        self.flags.add_string(
            name="artifacts",
            default=None,
            help=("Comma-separated list of Maven artifact coordinates.\n"
                  "e.g. 'group:artifact:version[[:classifier]:type]'.\n"
                  "classifier defaults to nothing.\n"
                  "type defaults to 'jar'."),
        )
        self.flags.add_string(
            name="scope",
            default="compile",
            help="Dependency scope.",
        )

    def run(self, args):
        coords = list()
        if self.flags.artifacts is not None:
            coords.extend(self.flags.artifacts.split(","))
        coords.extend(args)

        coords = filter(None, coords)
        artfs = map(artifact.parse_artifact, coords)
        artfs = tuple(artfs)

        for artf in artfs:
            log.info("Examining artifact: {}", artf)

        cp_entries = list(list_classpath_entries(
            coordinates=artfs,
            scope=self.flags.scope,
            local_repo=base.strip_optional_prefix(self.repo.local.path, "file://"),
        ))
        for cp_entry in cp_entries:
            print(cp_entry)
        return os.EX_OK



# --------------------------------------------------------------------------------------------------


class ListDependencies(MavenAction):
    def register_flags(self):
        self.flags.add_string(
            name="artifacts",
            default=None,
            help=("Comma-separated list of Maven artifact coordinates.\n"
                  "e.g. 'group:artifact:version[[:classifier]:type]'.\n"
                  "classifier defaults to nothing.\n"
                  "type defaults to 'jar'."),
        )
        self.flags.add_string(
            name="scope",
            default="compile",
            help="Dependency scope.",
        )

    def run(self, args):
        coords = list()
        if self.flags.artifacts is not None:
            coords.extend(self.flags.artifacts.split(","))
        coords.extend(args)

        coords = filter(None, coords)
        artfs = map(artifact.parse_artifact, coords)
        artfs = tuple(artfs)

        for artf in artfs:
            log.info("Examining artifact: {}", artf)

        cp_entries = list(list_dependencies(
            coordinates=artfs,
            scope=self.flags.scope,
            local_repo=base.strip_optional_prefix(self.repo.local.path, "file://"),
        ))
        for cp_entry in cp_entries:
            print(cp_entry)
        return os.EX_OK


# --------------------------------------------------------------------------------------------------


class GetDependencyTree(MavenAction):
    def register_flags(self):
        self.flags.add_string(
            name="artifacts",
            default=None,
            help=("Comma-separated list of Maven artifact coordinates.\n"
                  "e.g. 'group:artifact:version[[:classifier]:type]'.\n"
                  "classifier defaults to nothing.\n"
                  "type defaults to 'jar'."),
        )
        self.flags.add_string(
            name="scope",
            default="compile",
            help="Dependency scope.",
        )

    def run(self, args):
        coords = list()
        if self.flags.artifacts is not None:
            coords.extend(self.flags.artifacts.split(","))
        coords.extend(args)

        coords = filter(None, coords)
        artfs = map(artifact.parse_artifact, coords)
        artfs = tuple(artfs)

        for artf in artfs:
            log.info("Examining artifact: {}", artf)

        print(get_dependency_tree(
            coordinates=artfs,
            scope=self.flags.scope,
            local_repo=base.strip_optional_prefix(self.repo.local.path, "file://"),
        ))
        return os.EX_OK


# --------------------------------------------------------------------------------------------------


_ACTIONS = {
    "fetch-artifact": FetchArtifact,
    "list-artifact-versions": ListArtifactVersions,
    "build-classpath": BuildClasspath,
    "list-dependencies": ListDependencies,
    "dependency-tree": GetDependencyTree,
}


class MavenTool(cli.Action):
    def register_flags(self):
        self.flags.add_string(
            name="do",
            default=None,
            help="Action to perform. One of {}.".format(",".join(sorted(_ACTIONS))),
        )
        self.flags.add_string(
            name="remotes",
            default=",".join([
                maven_repo.CLOUDERA_REPO,
                maven_repo.MAVEN_CENTRAL_REPO,
            ]),
            help="Comma-separated list of remote Maven repositories.",
        )
        self.flags.add_string(
            name="local",
            default="file://" \
                + os.path.abspath(os.path.join(os.environ["HOME"], ".m2", "repository")),
            help="Path of the local Maven repository.",
        )

    def run(self, args):
        action = self.flags.do
        if (action is None) and (len(args) >= 1) and (args[0] in _ACTIONS):
            action = args[0]
            args = args[1:]

        if action is None:
            print("Must specify an action to perform with {} [--do=]action"
                  .format(base.get_program_name()))
            return os.EX_USAGE

        repo = maven_repo.MavenRepository(
            local=self.flags.local,
            remotes=self.flags.remotes.split(","),
        )

        action_class = _ACTIONS[action]
        cli = action_class(repo=repo)
        return cli(args)


def main(args):
    """Program entry point.

    Args:
        args: unparsed/unknown command-line arguments.
    """
    tool = MavenTool(parent_flags=base.FLAGS)
    return tool(args)


# ------------------------------------------------------------------------------


if __name__ == "__main__":
    base.run(main)
