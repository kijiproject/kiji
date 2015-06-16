#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Links a JVM executable."""

import os

from base import base
from base import cli
from base import log

from wibi.build import java_linker
from wibi.build import workspace
from wibi.maven import artifact
from wibi.maven import maven_loader
from wibi.maven import maven_repo
from wibi.maven import maven_wrapper


# --------------------------------------------------------------------------------------------------


MAVEN_REMOTES = (
    maven_repo.MAVEN_CENTRAL_REPO,
)


class JavaLinkerTool(cli.Action):
    """Stripped-down command-line tool to link JVM executables."""

    USAGE = """\
    |Java binary linker.
    |
    |Usage:
    |    java-linker --output=/path/to/executable \
    |        [--main-class=com.foobar.Main] \
    |        --artifacts=group1:artf1:v1,group2:artf2:v2,...
    """

    def register_flags(self):
        self.flags.add_string(
            name="local-repo",
            default=":wkspc:",
            help=("Optional explicit Maven local repository to use.\n"
                  "Use ':wkspc:' to look for the enclosing workspace, if any."),
        )
        self.flags.add_string(
            name="maven-remotes",
            default=",".join(MAVEN_REMOTES),
            help="Optional comma-separated list of Maven remote repositories to use.",
        )

        self.flags.add_string(
            name="main_class",
            default=None,
            help="Optional Java class name to invoke by default.",
        )
        self.flags.add_string(
            name="artifacts",
            default=None,
            help="Comma-separated list of Maven artifact coordinates to link together.",
        )
        self.flags.add_string(
            name="exclusions",
            default=None,
            help="Comma-separated list of Maven artifact coordinates to exclude globally.",
        )
        self.flags.add_string(
            name="scope",
            default="runtime",
            help="Maven scope to resolve dependencies.",
        )

        self.flags.add_string(
            name="output",
            default=None,
            help="Path of the target binary to create.",
        )
        self.flags.add_boolean(
            name="overwrite",
            default=True,
            help="Whether to overwrite the target file if it already exists.",
        )

    def run(self, args):
        artifacts = list()
        if self.flags.artifacts is not None:
            artifacts.extend(self.flags.artifacts.split(","))
        if len(args) > 0:
            artifacts.extend(args)
            args = []
        artifacts = list(map(artifact.parse_artifact, artifacts))

        exclusions = tuple()
        if self.flags.exclusions is not None:
            exclusions = self.flags.exclusions.split(",")
            exclusions = filter(None, exclusions)
            exclusions = map(artifact.parse_artifact, exclusions)
            exclusions = frozenset(exclusions)

        assert (len(args) == 0), "Unexpected command-line arguments: {!r}".format(args)

        # --------------------

        local_repo = self.flags.local_repo
        if local_repo == ":wkspc:":
            wkspc = workspace.find_workspace_root(path=os.getcwd())
            if wkspc is None:
                local_repo = os.path.join(os.environ["HOME"], ".m2", "repository")
                log.info("No workspace found, using global repo {!r}", local_repo)
            else:
                local_repo = os.path.join(wkspc, "output", "maven_repository")
                log.info("Using workspace local repo {!r}", local_repo)
        assert os.path.exists(local_repo), \
            "Maven local repository not found: {!r}".format(local_repo)

        maven_remotes = self.flags.maven_remotes.split(",")
        repo = maven_repo.MavenRepository(local=local_repo, remotes=maven_remotes)

        # --------------------

        assert (self.flags.output is not None) and (len(self.flags.output) > 0), \
            "Must specify output path with --output=/path/to/java-binary"
        if self.flags.overwrite and os.path.exists(self.flags.output):
            os.remove(self.flags.output)

        # --------------------

        scanner = maven_loader.ArtifactScanner(repo=repo, fetch_content=True)
        deps = []
        for artf in artifacts:
            dep = maven_wrapper.Dependency(
                artifact=artf,
                scope=self.flags.scope,
                exclusions=exclusions,
            )
            deps.append(dep)
        scanner.scan(deps)
        assert (len(scanner.errors) == 0), \
            "{:d} errors while resolving Maven dependencies".format(len(scanner.errors))

        artifacts = list()
        for artf, dep_chains in scanner.versions.items():
            dep_chain = dep_chains[0]
            dep = dep_chain[0]
            artifacts.append(dep.artifact)

        # --------------------

        linker = java_linker.JavaLinker(
            output_path=self.flags.output,
            maven_repo=repo,
        )
        try:
            linker.link(
                artifacts=artifacts,
                main_class=self.flags.main_class,
            )
        finally:
            linker.close()

        return os.EX_OK


# --------------------------------------------------------------------------------------------------


def main(args):
    tool = JavaLinkerTool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == "__main__":
    base.run(main)
