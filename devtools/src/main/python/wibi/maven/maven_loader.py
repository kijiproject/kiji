#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Maven artifact dependency resolver."""

import collections
import logging
import os
import queue
import threading
import traceback

from base import base

from wibi.maven import artifact
from wibi.maven import maven_repo
from wibi.maven import maven_wrapper


LOG_LEVEL = base.LOG_LEVEL


# --------------------------------------------------------------------------------------------------


class Error(Exception):
    """Errors used in this module."""
    pass


# --------------------------------------------------------------------------------------------------


def format_dep_chain(chain):
    """Formats a dependency chain as a single line.

    Args:
        chain: Dependency chain, as a list of Maven dependencies.
    Returns:
        Textual representation of the Maven dependency chain.
    """
    return " from ".join(map(str, chain))


# --------------------------------------------------------------------------------------------------

# Exclusion of all Maven dependencies:
ALL_EXCLUSION_WILDCARD = artifact.ArtifactName(group_id="*", artifact_id="*")


class ArtifactScanner(object):
    """Scans an artifact's dependency tree."""

    def __init__(self, repo, fetch_content=True, force_unresolved=False):
        """Initializes a new artifact dependency scanner.

        Args:
            repo: Maven repository to use.
            fetch: Whether to fetch artifact content, in addition to the POMs.
        """
        self._repo = repo
        self._wkspc = maven_wrapper.MavenWorkspace(repo=self.repo)

        self._fetch_content = fetch_content

        # Queue of artifacts yet to process:
        self._queue = queue.Queue()

        # Set of artifact coordinates processed already:
        self._done = set()

        # Map: unversioned Maven artifact -> list of dependency chains
        self._versions = None

        # Accumulated list of errors (raised exceptions):
        self._errors = None

        # Whether to force the use of unresolved dependencies (comments in POM files):
        self._force_unresolved = force_unresolved

    @property
    def repo(self):
        """Returns: the Maven repository from which to fetch artifacts."""
        return self._repo

    @property
    def wkspc(self):
        """Returns: the Maven workspace used to resolve dependencies."""
        return self._wkspc

    @property
    def versions(self):
        """Returns: the version map: unversioned Artifact -> list of dependency chain."""
        return self._versions

    @property
    def errors(self):
        return self._errors

    def _visit_dep(self, dep_chain):
        """Processes the specified dependency.

        Args:
            dep_chain: Dependency chain to visit.
                First dependency is the one to visit.
        Returns:
            The path of the artifact on the local file system.
        """
        if logging.getLogger().isEnabledFor(LOG_LEVEL.DEBUG_VERBOSE):
            logging.debug("Processing dependency: %s", "\n\tfrom ".join(map(str, dep_chain)))

        dep = dep_chain[0]
        artf = dep.artifact

        if len(dep_chain) >= 2:
            sdep = dep_chain[1]
            if (sdep.scope in ("compile", "provided", "runtime")
                and dep.scope in ("compile", "runtime")):
                pass
            elif (sdep.scope == "test") and (dep.scope in ("runtime", "compile")):
                pass
            else:
                logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Discarding dependency: %s -> %s", sdep, dep)
                if dep.scope == "provided":
                    self._provided.append(dep_chain)
                return None

        # Do not fetch POM file unless the dependency is active:
        # Note: this means the referenced remote repositories are not included.
        project_artf = artf.update(classifier=None)
        project = self.wkspc.GetOrFetchForArtifact(artifact=project_artf)

        # Ignore dependencies on artifact that were already resolved:
        # Note that because the dependency chain is different, the transitive
        # dependencies on this artifact could be different.
        unversioned_artf = artf.update(version=None)
        self._versions[unversioned_artf].append(dep_chain)
        if len(self._versions[unversioned_artf]) > 1:
            return

        if self._fetch_content:
            fetched = self.repo.Get(
                group=artf.group_id,
                artifact=artf.artifact_id,
                version=artf.version,
                type=artf.packaging,
                classifier=artf.classifier,
            )
            if fetched is None:
                logging.error("Artifact not found: %s", artf)
                raise Error("Artifact not found: %s", artf)
            logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Fetched %s as %r", artf, fetched)
        else:
            fetched = None

        # Set of artifact names being excluded:
        exclusions = set()
        for dep in dep_chain:
            if dep.exclusions is not None:
                exclusions.update(dep.exclusions)
        exclusions = frozenset(exclusions)

        # Maven-style exclusions: 'group:artifact'
        maven_exclusions = filter(lambda exc: isinstance(exc, artifact.ArtifactName), exclusions)
        maven_exclusions = frozenset(maven_exclusions)

        # Full exclusions: 'group:artifact:packaging:classifier:version'
        full_exclusions = filter(lambda exc: isinstance(exc, artifact.Artifact), exclusions)
        full_exclusions = frozenset(full_exclusions)

        if ALL_EXCLUSION_WILDCARD in maven_exclusions:
            # Skip transitive dependencies if there is a global exclusion:
            pass
        else:
            def is_excluded(dep):
                artifact = dep.artifact
                for exclusion in maven_exclusions:
                    if (exclusion.group_id != "*") and (exclusion.group_id != artifact.group_id):
                        continue
                    if (exclusion.artifact_id != "*") and (exclusion.artifact_id != artifact.artifact_id):
                        continue
                    return True
                for exclusion in full_exclusions:
                    if (exclusion.group_id != "*") and (exclusion.group_id != artifact.group_id):
                        continue
                    if (exclusion.artifact_id != "*") and (exclusion.artifact_id != artifact.artifact_id):
                        continue
                    if (exclusion.version != "*") and (exclusion.version != artifact.version):
                        continue
                    if (exclusion.packaging != "*") and (exclusion.packaging != artifact.packaging):
                        continue
                    if (exclusion.classifier != "*") and (exclusion.classifier != artifact.classifier):
                        continue
                    return True
                return False

            deps = project.list_all_immediate_dependencies(
                include_optional=False,
                force_unresolved=self._force_unresolved,
            )
            for dep in deps:
                if is_excluded(dep):
                    logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Excluding dependency %s", dep)
                else:
                    logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Queuing dependency %s", dep)
                    transitive = list(dep_chain)
                    transitive.insert(0, dep)
                    self._queue.put(transitive)

        return fetched

    def scan(
        self,
        deps,
        nworkers=1,
    ):
        """Scan the specified dependencies.

        Args:
            deps: Collection of dependencies to scan.
            nworkers: Number of concurrent workers to fetch dependencies.
                Default is one (no parallelism).
                Note: this may affect the effective versions of artifact dependencies.
        Returns:
            The classpath inferred from the specified dependencies, as a set of JAR files.
        """
        # Set of artifacts already resolved (without version):
        self._done = set()

        # Map: unversioned Artifact -> list of dependency chains
        self._versions = collections.defaultdict(list)

        # List of ignored provided dependencies:
        self._provided = list()

        # Bootstrap the fetch:
        for dep in deps:
            stack = [dep]
            self._queue.put(stack)

        # Counter for the # of active workers
        self._active_worker_counter = 0

        # Accumulates the resolved dependencies:
        self._classpath = set()

        # Accumulates errors from the concurrent workers:
        self._errors = list()

        self._lock = threading.Lock()
        self._cond = threading.Condition(lock=self._lock)

        workers = [threading.Thread(target=self._worker_thread)
                   for _ in range(nworkers)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

        return self._classpath

    def _worker_thread(self):
        """Worker thread processing dependencies."""
        try:
            self._worker_thread_unsafe()
        except BaseException as err:
            logging.error("Error in Maven dependency scanner worker thread: %r", err)
            traceback.print_exc()
            self._errors.append(err)

    def _worker_thread_unsafe(self):
        while True:
            with self._lock:
                while self._queue.empty() and (self._active_worker_counter > 0):
                    self._cond.wait()
                if self._queue.empty() and (self._active_worker_counter == 0):
                    return

                # Pick an artifact to scan:
                dep_chain = self._queue.get_nowait()

                # This worker is now active:
                self._active_worker_counter += 1

            entry = self._visit_dep(dep_chain)

            with self._lock:
                if entry is not None:
                    self._classpath.add(entry)

                # This worker is no longer active:
                self._active_worker_counter -= 1
                self._cond.notify_all()

    def write_dep_list(self, output):
        ruler = '-' * 100
        logging.debug(ruler)
        for artf, version in sorted(self._versions.items()):
            logging.debug(ruler)
            resolved_dep = "%s:%s" % (artf.name, version[0][0].artifact.version)

            output.write("%d\t%s\n" % (len(version), resolved_dep))

            logging.debug("Resolved dependency %-80s%d ways", resolved_dep, len(version))
            for ichain, dep_chain in enumerate(version):
                logging.log(base.LOG_LEVEL.DEBUG_VERBOSE,
                            "Dependency chain #%d: %s",
                            ichain, "\n\tfrom ".join(map(str, dep_chain)))

        logging.debug(ruler)
        for unversioned, dep_chains in sorted(self._versions.items()):
            versions = set()
            for dep_chain in dep_chains:
                dep = dep_chain[0]
                versions.add(dep.artifact.version)
            if len(versions) > 1:
                logging.warn("Collision on artifact %s (available versions: %s)",
                             dep_chains[0][0].artifact, " - ".join(sorted(versions)))
                for dep_chain in dep_chains:
                    logging.debug("Dependency reference: %s", "\n\tfrom ".join(map(str, dep_chain)))
                logging.debug(ruler)

        logging.debug(ruler)
        provided = list(self._provided)
        provided = sorted(provided, key=lambda dc: dc[0].artifact)
        for dep_chain in provided:
            dep = dep_chain[0]
            if dep.artifact.update(version=None) not in self._versions:
                logging.info("Externally provided dependency: %s", format_dep_chain(dep_chain))

        logging.debug(ruler)

    def explain(self, artifact_name):
        """Explains where a dependency come from.

        Args:
            artifact_name: Name of the Maven artifact dependency to explain.
        """
        unversioned = artifact.Artifact(
            group_id=artifact_name.group_id,
            artifact_id=artifact_name.artifact_id,
            version=None,
        )
        chains = self._versions[unversioned]

        ruler = '-' * 100
        print("Resolved dependency %s in %d ways:" % (artifact_name, len(chains)))
        for ichain, dep_chain in enumerate(chains):
            print(ruler)
            print("Dependency chain #%d: %s" % (ichain, "\n\tfrom ".join(map(str, dep_chain))))


# --------------------------------------------------------------------------------------------------
