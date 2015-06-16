#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Java loader."""

import os

from base import base
from base import cli
from base import log

from wibi.maven import artifact
from wibi.maven import maven_loader
from wibi.maven import maven_repo
from wibi.maven import maven_wrapper

from wibi.build import workspace


# --------------------------------------------------------------------------------------------------


class Error(Exception):
    """Errors used in this module."""
    pass


# --------------------------------------------------------------------------------------------------


class LoaderTool(cli.Action):
    """Tool to help investigating Maven dependencies."""

    def register_flags(self):
        self.flags.add_string(
            name="targets",
            default="",
            help=("Comma-separated list of Maven artifact coordinates to include.\n"
                  "Each target is specified as 'group:artifact[[:type]:qualifier]:version'."),
        )
        self.flags.add_string(
            name="exclusions",
            default="",
            help=("Comma-separated list of Maven artifact names to exclude.\n"
                  "Coordinates are specified as 'group:artifact[[:type]:qualifier]:version'."),
        )

        self.flags.add_string(
            name="maven-local",
            default=":wkspc:",
            help=("Optional path to the local Maven repository.\n"
                  "Specify ':wkspc:' to use the workspace local repository."),
        )
        self.flags.add_string(
            name="maven-remotes",
            default=",".join([
                maven_repo.MAVEN_CENTRAL_REPO,
                maven_repo.CLOUDERA_REPO,
                maven_repo.CONCURRENT_REPO,
                "http://repository.jboss.org/nexus/content/groups/public/",
            ]),
            help="Comma-separated list of the remote Maven repositories to fetch from.",
        )
        self.flags.add_string(
            name="maven-remote-exclusions",
            default=",".join([
                "https://repo.wibidata.com/artifactory/kiji",
                "https://repo.wibidata.com/artifactory/kiji-nightly",
                "https://repo.wibidata.com/artifactory/wibi-nightly",
            ]),
            help="Comma-separated list of the remote Maven repositories to exclude (blacklist).",
        )
        self.flags.add_string(
            name="scope",
            default="runtime",
            help="Scope of the root dependency to scan.",
        )
        self.flags.add_string(
            name="do",
            default=None,
            help="Action to perform, one of: classpath, resolve, explain.",
        )

        self.flags.add_string(
            name="list-file",
            default=None,
            help="Optional file to write the list of resolved dependencies to.",
        )

        self.flags.add_string(
            name="explain",
            default=None,
            help="Name of the artifact to explain.",
        )

        self.flags.add_boolean(
            name="fetch",
            default=True,
            help="Whether to fetch artifact's content.",
        )

    @property
    def repo(self):
        return self._repo

    def run(self, args):
        action = self.flags.do
        if (action is None) and (len(args) > 0):
            action, args = args[0], args[1:]
        if action is None:
            action = "classpath"
        assert (action is not None), ("Must specify an action to perform, eg. 'classpath'.")

        local_repo = self.flags.maven_local
        if local_repo == ":wkspc:":
            wkspc = workspace.find_workspace_root(path=os.getcwd())
            if wkspc is not None:
                local_repo = os.path.join(wkspc, "output", "maven_repository")
                log.info("Using workspace local repo {!r}", local_repo)
            else:
                local_repo = os.path.join(os.environ["HOME"], ".m2", "repository")
                log.info("No workspace found, using global repo {!r}", local_repo)
        assert os.path.exists(local_repo), \
            "Maven local repository not found: {!r}".format(local_repo)

        self._repo = maven_repo.MavenRepository(
            local = local_repo,
            remotes = self.flags.maven_remotes.split(","),
            exclusions = frozenset(self.flags.maven_remote_exclusions.split(",")),
        )

        self._scanner = maven_loader.ArtifactScanner(
            repo=self.repo,
            fetch_content=self.flags.fetch,
            force_unresolved=True,
        )

        targets = list(self.flags.targets.split(",")) + list(args)
        targets = list(filter(None, targets))
        target_artifacts = map(artifact.parse_artifact, targets)

        exclusions = self.flags.exclusions.split(",")
        exclusions = filter(None, exclusions)
        exclusion_artifacts = frozenset(map(artifact.parse_artifact_name, exclusions))

        deps = list()
        for target_artifact in target_artifacts:
            dep = maven_wrapper.Dependency(
                artifact=target_artifact,
                scope=self.flags.scope,
                exclusions=exclusion_artifacts,
            )
            deps.append(dep)

        log.debug("Scanning dependencies: {!r}", deps)
        classpath = self._scanner.scan(deps)

        list_file_path = self.flags.list_file
        if list_file_path is None:
            list_file_path = '/dev/null'
        with open(list_file_path, "wt") as ofile:
            self._scanner.write_dep_list(output=ofile)

        if action == "classpath":
            print(":".join(sorted(classpath)))

        elif action == "resolve":
            for artf, dep_chains in sorted(self._scanner.versions.items()):
                chain = dep_chains[0]
                selected = chain[0]
                print(selected.artifact)

        elif action == "explain":
            self._scanner.explain(artifact.parse_artifact_name(self.flags.explain))

        else:
            raise Error("Invalid action: {!r}".format(action))


# --------------------------------------------------------------------------------------------------


def main(args):
    tool = LoaderTool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == "__main__":
    base.run(main)
