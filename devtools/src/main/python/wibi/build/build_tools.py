#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Tools to build and test libraries and binaries."""

import glob
import hashlib
import itertools
import logging
import os
import re
import shutil
import subprocess
import tempfile
import zipfile

from base import base
from base import command
from base import log

from wibi.build import build_defs
from wibi.maven import artifact
from wibi.maven import maven_loader
from wibi.maven import maven_wrapper


FLAGS = base.FLAGS
FLAGS.add_string(
    name='maven_args',
    default=None,
    help='Extra arguments for Maven commands.',
)


# --------------------------------------------------------------------------------------------------


class Error(Exception):
    """Errors used in this module."""
    pass


# --------------------------------------------------------------------------------------------------


def list_dirs(path):
    for root_path, dir_names, file_names in os.walk(path):
        for dir_name in dir_names:
            yield os.path.join(root_path, dir_name)


def extglob(path):
    """Evaluates an extended glob.

    In addition to wildcards recognized by the glob module,
    extended globs also recognize the path component '**' to match all nested subdirectory.
    The '**' path component cannot be prefixed or suffixed. For example,
    the glob: "/path/a**b/*.py" does not recognize the '**' token specially.

    Note: the '**' token is also expanded as no directory. In other words,
    the pattern /a/**/b also matches /a/b.

    Args:
        path: File path potentially including extended globs.
    Yields:
        File paths matching the specified glob pattern.
    """

    def _list_matches(root, comps):
        if len(comps) == 1:
            yield from glob.iglob(os.path.join(root, comps[0]))
        else:
            for resolved in glob.iglob(os.path.join(root, comps[0])):
                yield from _list_matches(resolved, comps[1:])
                for resolved_dir in list_dirs(resolved):
                    yield from _list_matches(resolved_dir, comps[1:])

    comps = path.split("/**/")
    yield from _list_matches("", comps)


# --------------------------------------------------------------------------------------------------


def clone_maven_repo(target, source):
    """Clones a Maven local repository deeply.

    Clones the entire source Maven local repository into the specified target
    directory. The clone is entirely isolated from the source repository, ie.
    new artifacts downloaded into the clone are not visible in the source
    repository.

    Args:
        target: Where to clone the source Maven repository.
        source: Source Maven repository to clone.
    """
    assert os.path.exists(source), ("Source Maven local repository %r does not exists.", source)

    assert not os.path.exists(target), ("Target Maven local repository %r already exists.", target)
    base.make_dir(target)
    assert os.path.exists(target), ("Could not create target directory %r" % target)

    for name in os.listdir(source):
        source_path = os.path.join(source, name)
        if os.path.isfile(source_path):
            os.symlink(src=source_path, dst=os.path.join(target, name))
        elif os.path.isdir(source_path):
            clone_maven_repo(target=os.path.join(target, name), source=source_path)
        else:
            logging.error("Unable to handle file: %s", source_path)


def backport_updates_from_cloned_repo(clone, source):
    """Back-port updates in a cloned Maven repository to its source.

    Contributes new artifacts from the cloned repository back to the source
    repository, in a way that is thread-safe.
    This operation destroys the cloned repository.

    Args:
        clone: Maven repository to back-port from.
        source: Maven repository source of the clone.
    """
    assert os.path.exists(clone), ("Source Maven local repository %r does not exists.", clone)

    base.make_dir(source)

    for name in os.listdir(clone):
        clone_path = os.path.join(clone, name)
        source_path = os.path.join(source, name)
        if os.path.islink(clone_path):
            # Skip all symlinks
            continue
        elif os.path.isfile(clone_path):
            # Copy file instead of move?
            logging.debug("Back-porting %s as %s", clone_path, source_path)
            os.rename(src=clone_path, dst=source_path)
        elif os.path.isdir(clone_path):
            backport_updates_from_cloned_repo(source=source_path, clone=clone_path)
        else:
            logging.error("Unable to handle file: %s", clone_path)


# --------------------------------------------------------------------------------------------------


# This fingerprint salt must be incremented/updated in order to invalidate previous builds.
# Such updates are required when, for example:
#  - the format of a trace file changes in an incompatible way;
#  - the process by which an artifact is produced is changed.

FINGERPRINT_SALT = "kiji-build revision #9 2015-05-06"
#
# "kiji-build revision #9 2015-05-06"
# - Reset after several changes.
#
# "kiji-build revision #8 2015-02-11"
# - Added support for all build tasks with packrat. Edits content in output_keys of output records.
#
# "kiji-build revision #7 2015-02-10"
# - Added support for packrat. Adds output_keys to output records.
#
# "kiji-build revision #6 2015-02-09" :
#  - Add the field output_files to the output records of python binary tasks.
#
# "kiji-build revision #5 2015-02-07" :
#  - Add support for import-scope Maven dependencies.
#    This may affect the resulting dependency trees.
#
# "kiji-build revision #4 2015-02-07" :
#  - JAR files now include a minimal META-INF/MANIFEST.MF file
#
# "kiji-build revision #3 2015-02-02" :
#  - moved all build specification data structures into a separate module:
#    existing pickled data becomes invalid.
#
# "kiji-build revision #2 2015-01-26" :
#  - generated *.pom files also include the ordered list of direct dependencies (as comments)
#    with kiji-build style exclusions.
#  - output records for Java tasks are slightly extended.
#
# "kiji-build revision #1 2015-01-15" :
#  - introduction of the fingerprint salt
#


INIT_PY = b"""\
import pkg_resources
pkg_resources.declare_namespace(__name__)
"""


def get_executable_path(name):
    """Reports the path of a given executable.

    Args:
        name: Name of the executable to report the path for.
    Returns:
        The absolute path for the specified executable, or None if it cannot be found.
    """
    try:
        output = subprocess.check_output(["which", name]).decode().strip()
        path = os.path.abspath(output)
        assert os.path.exists(path)
        return path
    except subprocess.CalledProcessError:
        return None


class BuildTools(object):
    """Aggregates a collection of build tools."""

    def __init__(
        self,
        workspace,
        config=None,
        avro_version=None,
        scala_version=None,
        checkstyle_version=None,
        scalastyle_version=None,
    ):
        self._workspace = workspace

        if config is None:
            config = self.workspace.config
        self._config = config

        if avro_version is None:
            avro_version = self.config.avro_version
        if scala_version is None:
            scala_version = self.config.scala_version
        if checkstyle_version is None:
            checkstyle_version = self.config.checkstyle_version
        if scalastyle_version is None:
            scalastyle_version = self.config.scalastyle_version

        self._jar_path = get_executable_path("jar")
        self._java_path = get_executable_path("java")
        self._javac_path = get_executable_path("javac")
        self._mvn_path = get_executable_path("mvn")
        self._python_path = get_executable_path("python")
        self._npm_path = get_executable_path("npm")
        self._bower_path = get_executable_path("bower")
        self._grunt_path = get_executable_path("grunt")

        assert (self.java_path is not None), "Required 'java' executable not found."
        assert (self.javac_path is not None), "Required 'javac' executable not found."
        assert (self.python_path is not None), "Required 'python' executable not found."

        logging.debug("Path for 'java' executable: %r", self.java_path)
        logging.debug("Path for 'javac' executable: %r", self.javac_path)
        logging.debug("Path for 'python' executable: %r", self.python_path)
        logging.debug("Path for 'npm' executable: %r", self.npm_path)
        logging.debug("Path for 'bower' executable: %r", self.bower_path)
        logging.debug("Path for 'grunt' executable: %r", self.grunt_path)

        self._avro_version = avro_version
        self._scala_version = scala_version
        self._checkstyle_version = checkstyle_version
        self._scalastyle_version = scalastyle_version

        self._avro_lib_classpath = None
        self._avro_ipc_classpath = None
        self._avro_tools_classpath = None
        self._scalac_classpath = None
        self._checkstyle_classpath = None
        self._scalastyle_classpath = None

        self._timestamp = base.timestamp()

        self._maven_cloned_repo_dir = os.path.join(
            self.workspace.temp_dir,
            "maven_repository",
            "%s.%s" % (self._timestamp, os.getpid()),
        )

        logging.debug("KijiBuild builtin fingerprint salt: %r", FINGERPRINT_SALT)
        logging.debug("Java version: %r", self.java_version_text)
        logging.debug("Javac version: %r", self.javac_version_text)
        logging.debug("Python version: %r", self.python_version_text)

        # Compute the fingerprint salt:
        self._fingerprint_salt = "\n".join([
            FINGERPRINT_SALT,
            self.java_version_text,
            self.javac_version_text,
            self.python_version_text,
        ])
        logging.debug("KijiBuild composite fingerprint: %r", self._fingerprint_salt)

    @property
    def workspace(self):
        return self._workspace

    @property
    def config(self):
        """Returns: the workspace configuration."""
        return self._config

    def validate(self):
        """Validates this tooling against requirements.

        Raises:
            Error: if the tooling does not meet KijiBuild requirements.
        """
        if self.java_version not in self.config.java_versions:
            raise Error(
                "Unsupported Java version: {!r}. "
                "Please use a supported Java version: {!s}."
                .format(self.java_version, self.config.java_versions)
            )

        if self.javac_version not in self.config.java_versions:
            raise Error(
                "Unsupported Java version: {!r}. "
                "Please use a supported Java version: {!s}."
                .format(self.javac_version, self.config.java_versions)
            )

        if self.python_version not in self.config.python_versions:
            raise Error(
                "Unsupported Python version: {!r}. "
                "Please use a supported Python version: {!s}."
                .format(self.python_version, self.config.python_versions)
            )

    @property
    def jar_path(self):
        """Returns: the path of the 'jar' executable."""
        return self._jar_path

    @property
    def java_path(self):
        """Returns: the path of the 'java' executable."""
        return self._java_path

    @property
    def javac_path(self):
        """Returns: the path of the 'javac' executable."""
        return self._javac_path

    @property
    def mvn_path(self):
        """Returns: the path of the 'mvn' executable."""
        return self._mvn_path

    @property
    def python_path(self):
        """Returns: the path of the 'python' executable."""
        return self._python_path

    @property
    def npm_path(self):
        """Returns: the path of the 'npm' executable."""
        return self._npm_path

    @property
    def bower_path(self):
        """Returns: the path of the 'bower' executable."""
        return self._bower_path

    @property
    def grunt_path(self):
        """Returns: the path of the 'grunt' executable."""
        return self._grunt_path

    @property
    def scala_library_version(self):
        # Removes the last version digit from the scala version (by convention scala dependencies
        # append the scala minor version on their artifact id).
        return ".".join(self._scala_version.split(".")[:-1])

    @base.memoized_property
    def java_version_text(self):
        """Returns: the text emitted by the command 'java -version'."""
        cmd = command.Command(args=[self.java_path, "-version"], exit_code=0)
        return (cmd.output_text + cmd.error_text)

    @base.memoized_property
    def java_version(self):
        """Returns: the Java version ID, eg. '1.7.0_60'."""
        java_version_line = self.java_version_text.splitlines()[0]
        match = re.match(r"""^java version "(.*)"$""", java_version_line)
        return match.group(1)

    @base.memoized_property
    def javac_version_text(self):
        """Returns: the text emitted by the command 'java -version'."""
        cmd = command.Command(args=[self.javac_path, "-version"], exit_code=0)
        return (cmd.output_text + cmd.error_text)

    @base.memoized_property
    def javac_version(self):
        """Returns: the Java version ID, eg. '1.7.0_60'."""
        javac_version_line = self.javac_version_text.splitlines()[0]
        match = re.match(r"""^javac (.*)$""", javac_version_line)
        return match.group(1)

    @base.memoized_property
    def python_version_text(self):
        """Returns: the text emitted by the command 'python --version'."""
        cmd = command.Command(args=[self.python_path, "--version"], exit_code=0)
        return (cmd.output_text + cmd.error_text)

    @base.memoized_property
    def python_version(self):
        """Returns: the Python version ID, eg. '3.4.2'."""
        python_version_line = self.python_version_text.splitlines()[0]
        match = re.match(r"""^Python (.*)$""", python_version_line)
        return match.group(1)

    def avro_lib_classpath(self, exclusions=tuple()):
        """Returns: the list of classpath entries to run the Avro command-line tools."""
        return self.get_classpath(
            artifacts=[self.avro_ipc_artifact],
            scope="compile",
            exclusions=exclusions,
        )

    @property
    def avro_lib_artifact(self):
        return artifact.Artifact("org.apache.avro", "avro", self._avro_version)

    def avro_ipc_classpath(self, exclusions=tuple()):
        return self.get_classpath(
            artifacts=[self.avro_lib_artifact, self.avro_ipc_artifact],
            scope="compile",
            exclusions=exclusions,
        )

    @property
    def avro_ipc_artifact(self):
        return artifact.Artifact("org.apache.avro", "avro-ipc", self._avro_version)

    @property
    def avro_tools_classpath(self):
        """Returns: the list of classpath entries to run the Avro command-line tools."""
        if self._avro_tools_classpath is None:
            self._avro_tools_classpath = self._get_avro_tools_classpath()
        return self._avro_tools_classpath

    def _get_avro_tools_classpath(self):
        """Computes the Java classpath necessary to run the command-line Avro tools."""
        avro_artf = artifact.Artifact("org.apache.avro", "avro-tools", self._avro_version)
        return self.get_classpath(artifacts=[avro_artf], scope="runtime")

    @property
    def scalac_classpath(self):
        if self._scalac_classpath is None:
            self._scalac_classpath = self._get_scalac_classpath()
        return self._scalac_classpath

    @property
    def scala_lib_artifact(self):
        return artifact.Artifact("org.scala-lang", "scala-library", self._scala_version)

    def _get_scalac_classpath(self):
        """Computes the Java classpath necessary to run the command-line Avro tools."""
        scala_artf = artifact.Artifact("org.scala-lang", "scala-compiler", self._scala_version)
        return self.get_classpath(artifacts=[scala_artf], scope="runtime")

    @property
    def checkstyle_classpath(self):
        if self._checkstyle_classpath is None:
            self._checkstyle_classpath = self._get_checkstyle_classpath()
        return self._checkstyle_classpath

    def _get_checkstyle_classpath(self):
        """Computes the Java classpath necessary to run the checkstyle command-line tool."""
        checkstyle_artf = \
            artifact.Artifact("com.puppycrawl.tools", "checkstyle", self._checkstyle_version)
        return self.get_classpath(artifacts=[checkstyle_artf], scope="runtime")

    @property
    def scalastyle_classpath(self):
        if self._scalastyle_classpath is None:
            self._scalastyle_classpath = self._get_scalastyle_classpath()
        return self._scalastyle_classpath

    def _get_scalastyle_classpath(self):
        """Computes the Java classpath necessary to run the scalastyle command-line tool."""
        scalastyle_artf = \
            artifact.Artifact(
                group_id="org.scalastyle",
                artifact_id="scalastyle_" + self.scala_library_version,
                version=self._scalastyle_version,
                # classifier="batch",
            )
        return self.get_classpath(artifacts=[scalastyle_artf], scope="runtime")

    # TODO: get_java_deps is too big and should be refactored.
    def get_java_deps(
        self,
        spec_name,
        deps,
        maven_exclusions,
        provided_slots,
        inputs,
        workflow,
    ):
        """Resolves the Java dependencies from the given specifications.

        Args:
            spec_name: String name of the specification.
            deps: Iterable of dependencies to resolve.
            maven_exclusions: Iterable of maven dependencies (along with their subtrees) to
                expliclty exclude.
            provided_slots: Iterable of dynamic dependency slots provided explicitly.
            inputs: Dictionary of build task inputs.
            workflow: Workflow the task belongs to.
        Returns:
            A tuple with:
             - the list of resolved build dependencies
             - the list of resolved declared dependencies
             - set of Maven exclusions (global)
             - map of dynamic dependencies: slot -> dependency
             - slots directly or transitively provided by this library
        """
        # Compute the set of Maven Artifact coordinate (patterns) to exclude:
        maven_exclusions = map(artifact.parse_artifact, maven_exclusions)
        maven_exclusions = frozenset(maven_exclusions)

        # ------------------------------------------------------------------------------------------
        # Extract dynamic dependency slots:

        # dynamic slot name -> dependency name
        dynamic_deps = dict()

        # Transitively inherit required slots for dynamic dependencies (but not the providers!):
        for dep in deps:
            if isinstance(dep, artifact.Artifact):
                continue
            if isinstance(dep, build_defs.DynamicDep):
                dep = dep.provider
                if dep is None:
                    # Skip this if the provided dependency is specified as None.
                    continue
            name = dep
            input = inputs.get(name)
            assert (input is not None), \
                "Internal error: no input matching dependency {!r}".format(name)

            slots = input.get("dynamic_deps", dict()).keys()
            if len(slots) > 0:
                logging.debug("%r inherits dynamic slots %r from %r", spec_name, slots, name)
            for slot in slots:
                dynamic_deps[slot] = base.UNDEFINED

        # Fulfill dynamic slots with specified dynamic dependencies:
        for dep in deps:
            if not isinstance(dep, build_defs.DynamicDep):
                continue
            existing = dynamic_deps.get(dep.slot, base.UNDEFINED)
            if existing is not base.UNDEFINED:
                raise Error("{!r} has duplicate dynamic dependency for slot {!r}: {!r} vs {!r}"
                            .format(spec_name, dep.slot, existing, dep.provider))
            dynamic_deps[dep.slot] = dep.provider

        logging.debug("Dynamic slots for %r: %r", spec_name, dynamic_deps)

        # ------------------------------------------------------------------------------------------
        # Processes dependencies in the declared order, and determine:
        #  - compile-time dependency list (build_deps);
        #  - published dependency list (declared_deps);
        #  - the dynamic slots provided (provided_slots).
        #
        # Note: we cannot iterate on inputs directly as this looses the ordering.

        build_deps = list()
        declared_deps = list()

        # Set of provided slots:
        provided_slots = set(provided_slots)

        backfill_dynamic_deps = list()

        for dep in deps:
            logging.debug("Processing dependency %r", dep)

            if isinstance(dep, artifact.Artifact):
                build_deps.append(dep)
                declared_deps.append(dep)
                continue

            dynamic = False
            if isinstance(dep, build_defs.DynamicDep):
                dynamic = True
                logging.debug("Processing dynamic dependency: %r", dep)
                dep = dep.provider
                if dep is None:
                    # User explicitly indicates there should be no provider for dynamic dependency:
                    continue

            # Output record of the build task that created the dependency:
            dep_output = inputs[dep]

            maven_deps = dep_output.get("maven_artifacts", tuple())
            build_deps.extend(maven_deps)

            if dynamic:
                # Dynamic dependencies are not exported in the POM,
                # ie. not included in declared_deps:
                pass
            else:
                # Static dependencies are exported in the POM:
                declared_deps.extend(maven_deps)

                # Transitively inherit slots provided by static dependencies:
                provided_slots.update(dep_output.get("provided_slots", tuple()))

            backfill_dynamic_deps.extend(dep_output.get("dynamic_deps", dict()).items())

        # Prioritize first instances over secondary ones:
        backfill_dynamic_deps.reverse()
        backfill_dynamic_deps = dict(backfill_dynamic_deps)

        # ------------------------------------------------------------------------------------------
        # Backfilling dynamic dependencies if necessary:

        for slot, provider in dynamic_deps.items():
            if provider is not base.UNDEFINED:
                continue

            provider = backfill_dynamic_deps.get(slot, base.UNDEFINED)
            if provider is base.UNDEFINED:
                raise Error("Dynamic slot {!r} not fulfilled for {!r}".format(slot, spec_name))

            logging.debug("Backfilling dynamic slot %r for %r with %r", slot, spec_name, provider)
            dynamic_deps[slot] = provider
            if provider is not None:
                provider_task = workflow.tasks[provider]
                dep_output = provider_task.output
                build_deps.extend(dep_output.get("maven_artifacts", tuple()))

        return (build_deps, declared_deps, maven_exclusions, dynamic_deps, provided_slots)

    def get_classpath(
        self,
        artifacts,
        exclusions=None,
        scope="runtime",
    ):
        """Generates a classpath for the specified collection of Maven artifacts.

        Args:
            artifacts: Collection of Maven Artifact to generate a classpath from.
            exclusions: Global exclusions.
            scope: Maven scope for the dependencies to derive a classpath from.
        Returns:
            Collection of classpath entries (sorted to ensure a determinism ordering).
        """
        if exclusions is not None:
            exclusions = frozenset(exclusions)

        scanner = maven_loader.ArtifactScanner(
            repo=self.workspace.maven_repo,
            fetch_content=True,
        )
        deps = []
        for artifact in artifacts:
            dep = maven_wrapper.Dependency(artifact=artifact, scope=scope, exclusions=exclusions)
            deps.append(dep)
        deps = sorted(scanner.scan(deps))
        assert (len(scanner.errors) == 0), \
            ("%d errors while resolving Maven dependencies" % len(scanner.errors))
        return deps

    def resolve_deps(
        self,
        artifacts,
        exclusions=None,
        scope="runtime",
    ):
        """Resolves the specified collection of Maven dependencies into a flat list.

        Args:
            artifacts: Collection of Maven Artifact to resolve.
            exclusions: Global exclusions.
            scope: Maven scope for the dependencies to resolve.
        Yields:
            Flat, resolved ordered list of Maven artifact coordinates.
        """
        if exclusions is not None:
            exclusions = frozenset(exclusions)

        scanner = maven_loader.ArtifactScanner(
            repo=self.workspace.maven_repo,
            fetch_content=True,
        )
        deps = []
        for artifact in artifacts:
            dep = maven_wrapper.Dependency(artifact=artifact, scope=scope, exclusions=exclusions)
            deps.append(dep)

        scanner.scan(deps)
        assert (len(scanner.errors) == 0), \
            ("%d errors while resolving Maven dependencies" % len(scanner.errors))

        for artf, dep_chains in sorted(scanner.versions.items()):
            dep_chain = dep_chains[0]
            dep = dep_chain[0]
            yield dep.artifact

    def compile_avro_idl(self, avdl_source_path, avpr_target_path):
        """Compiles an Avro IDL file (file.avdl) into Avro protocol files (file.avpr)."""
        os.makedirs(os.path.dirname(avpr_target_path), exist_ok=True)
        self.avro_tools("idl", avdl_source_path, avpr_target_path)

    def compile_avro_protocol_to_java(self, avpr_source_path, source_dir):
        """Compiles an Avro protocol (file.avpr) into a collection of Java source files."""
        self.avro_tools("compile", "-string", "protocol", avpr_source_path, source_dir)

    def compile_avro_schema_to_java(self, avsc_source_path, source_dir):
        """Compiles an Avro schema (file.avsc) into a collection of Java source files."""
        self.avro_tools("compile", "-string", "schema", avsc_source_path, source_dir)

    def avro_tools(self, *args):
        """General Avro tools command-line invocation.

        Args:
            *args: List of command-line arguments for the avro-tools command-line interface.
        Returns:
            The Command instance for the specified avro-tools command-line invocation.
        """
        cmd_args = [
            self.java_path,
            "-classpath", ":".join(self.avro_tools_classpath),
            "org.apache.avro.tool.Main",
        ]
        cmd_args.extend(args)

        cmd = command.Command(
            args=cmd_args,
            work_dir=self.workspace.path,
            exit_code=base.EXIT_CODE.SUCCESS,
        )
        return cmd

    def maven(self, clone_repo=False, local_repo=None, **kwargs):
        """Invokes the Maven command-line interface.

        Note: when cloning the Maven local repository, the command must run synchronously.

        Args:
            clone_repo: Whether to clone the Maven local repository.
            local_repo: Optional explicit Maven local repository. Default to the workspace's repo.
            args: List of command-line arguments for the Maven invocation.
            recursive: Whether to enable the Maven reactor. Default is non-recursive.
            snapshot_updates: Whether to update snapshot artifacts. Default is no.
        Returns:
            The Command instance for the specified Maven invocation.
        """
        if local_repo is None:
            local_repo = self.workspace.maven_local_repo

        if clone_repo:
            source_local_repo = local_repo
            os.makedirs(self._maven_cloned_repo_dir, exist_ok=True)
            local_repo = os.path.join(self._maven_cloned_repo_dir, base.random_alpha_num_word(20))
            logging.debug("Cloning Maven local repo %s into %s", source_local_repo, local_repo)
            clone_maven_repo(source=source_local_repo, target=local_repo)

        try:
            return self._maven(local_repo=local_repo, **kwargs)
        finally:
            if clone_repo:
                logging.debug("Back-porting cloned Maven repo %s to %s",
                              local_repo, source_local_repo)
                backport_updates_from_cloned_repo(source=source_local_repo, clone=local_repo)
                shutil.rmtree(local_repo)

    def _maven(
        self,
        args,
        recursive=False,
        snapshot_updates=False,
        local_repo=None,
        **kwargs
    ):
        if local_repo is None:
            local_repo = self.workspace.maven_local_repo
        cmd_args = [
            self.mvn_path,
            "--batch-mode",
            "-Dmaven.repo.local=%s" % local_repo,
        ]
        if not snapshot_updates:
            cmd_args.append("--no-snapshot-updates")
        if not recursive:
            cmd_args.append("--non-recursive")
        if FLAGS.maven_args is not None:
            cmd_args.extend(FLAGS.maven_args.split())
        cmd_args.extend(args)
        return command.Command(
            args=cmd_args,
            exit_code=base.EXIT_CODE.SUCCESS,
            **kwargs
        )

    def get_java_build_classpath_from_maven_project(self, project):
        with tempfile.NamedTemporaryFile() as classpath_file:
            self.maven(
                args=[
                    "dependency:build-classpath",
                    "-DincludeScope=compile",
                    "-Dmdep.cpFile=%s" % classpath_file.name,
                ],
                work_dir=project.path,
            )
            return classpath_file.read().decode().split(":")

    def resolve_wpath(self, path):
        """Resolves a workspace path into an absolute path.

        Args:
            path: Workspace path (starting with //path/to/file, where // indicates workspace root).
        Returns:
            Resolved absolute file path.
        """
        if path.startswith("//"):
            path = os.path.join(self.workspace.path, path[2:])
        return path

    def wkspc_relpath(self, path):
        """Relativize an absolute path within the workspace.

        Args:
            Absolute file path within the workspace.
        Returns:
            Workspace file path (with a leading //).
        """
        assert path.startswith(self.workspace.path), \
            "Path {path} does not belong to workspace {wkspc}" \
                .format(path=path, wkspc=self.workspace.path)

        return "//" + os.path.relpath(path, self.workspace.path)

    def compile_java(
        self,
        sources,
        build_classpath,
        classes_dir,
        processor_classpath=None,
        processors=None,
        deprecation_warnings=False,
        java_source_version=None,
        jvm_target_version=None,
    ):
        """Compiles a collection of Java source files into Java class files.

        Args:
            sources: Collection of Java source file paths to compile.
            build_classpath: Classpath (ordered list of classpath entries) to use while compiling.
            classes_dir: Path of the directory where to write Java class files.
            processor_classpath: Classpath (ordered list) where to find processors.
            processors: Collection of processor class names.
            deprecation_warnings: Whether to enable deprecation warnings.
            java_source_version: Optional explicit version of the Java source code to accept.
            jvm_target_version: Optional explicit version of the JVM bytecode to produce.
        Returns:
            The Command instance for the javac invocation.
        """
        # Force remove classes_dir first
        os.makedirs(classes_dir, exist_ok=True)

        processor_args = []
        if (processors is not None) and (len(processors) > 0):
            processor_args = [
                "-processorpath", ":".join(processor_classpath),
                "-processor", ",".join(processors),
            ]

        if java_source_version is None:
            java_source_version = self.config.java_source_version
        if jvm_target_version is None:
            jvm_target_version = self.config.jvm_target_version

        args = [
            self.javac_path,
        ] + processor_args + [
            "-classpath", ":".join(build_classpath),
            "-d", classes_dir,
            "-source", java_source_version,
            "-target", jvm_target_version,
        ]
        if deprecation_warnings:
            args.append("-Xlint:deprecation")
        args.extend(map(self.resolve_wpath, sources))

        cmd = command.Command(
            args=args,
            work_dir=self.workspace.path,
        )
        return cmd

    def extglob(self, path):
        """Evaluates a workspace glob.

        Args:
            path: Workspace-relative path, potentially including globs.
                Being relative to the workspace, glob must start with "//".
                Recognized patterns include:
                    - //path/to/file : selector for a single file;
                    - //path/t*/*o/Test*.py : traditional '*' and '?' globs;
                    - //path/**/*.java : recursive folder '**' glob.
        Yields:
            Resolved workspace paths (globs expanded).
        """
        return map(self.workspace.wpath, extglob(self.workspace.apath(path)))

    def glob(self, *patterns):
        """Evaluates a collection of workspace glob patterns.

        Recognized patterns are:
            //path/to/file
            //path/*/to/f*le
            //path/to/**.py
        Patterns must start with // which refers to the root of the workspace.

        Yields:
            Workspace file paths matched by the pattern.
        """
        apatterns = map(self.workspace.apath, patterns)
        resolved = itertools.chain(*map(self._glob, apatterns))
        return map(self.workspace.wpath, resolved)

    def _glob(self, pattern):
        """Evaluates a glob pattern.

        Args:
            pattern: Absolute file pattern to match.
                Recognizes the pattern 'path/to/**.extension'.
        Yields:
            Absolute file paths matched by the pattern.
        """
        name = os.path.basename(pattern)
        if name.startswith("**"):
            yield from self.list_files_with_suffix(
                path=os.path.dirname(pattern),
                suffix=name[2:],
            )
        else:
            yield from glob.glob(pattern)

    def compile_scala(
        self,
        sources,
        build_classpath,
        classes_dir,
        jvm_target_version=None,
    ):
        """Compiles a collection of Scala source files into Java class files.

        Args:
            sources: Collection of Scala source file paths to compile.
            build_classpath: Classpath (ordered list of classpath entries) to use while compiling.
            classes_dir: Path of the directory where to write Java class files.
            jvm_target_version: Optional explicit version of the JVM bytecode to produce.
        Returns:
            The Command instance for the Scala compiler invocation.
        """
        # Force remove classes_dir first
        os.makedirs(classes_dir, exist_ok=True)

        if jvm_target_version is None:
            jvm_target_version = self.config.jvm_target_version

        args = [
            "-classpath", ":".join(build_classpath),
            "-d", classes_dir,
            "-target:jvm-{}".format(jvm_target_version),
        ]
        args.extend(map(self.resolve_wpath, sources))

        return self.scalac(args)

    def scalac(self, args):
        """Invokes the Scala compiler command-line interface.

        Args:
            args: Command-line arguments to send to the Scala compiler.
        Returns:
            The Command instance for the Scala compiler invocation.
        """
        cmd_args = [
            self.java_path,
            "-classpath", ":".join(self.scalac_classpath),
            "scala.tools.nsc.Main",
        ]
        cmd_args.extend(args)
        cmd = command.Command(
            args=cmd_args,
            work_dir=self.workspace.temp_dir,
        )
        return cmd

    def build_jar(self, *dirs, jar_file_path, mapping=None):
        """Constructs a JAR file from the content specified through a list of directories.

        Args:
            *dirs: collection of directories whose content is to include in the resulting JAR file.
            jar_file_path: Path of the JAR file to construct.
            mapping: Explicit file mapping: archive path -> local FS path to include.
        """
        logging.debug("Creating JAR file %r", jar_file_path)

        manifest = dict()
        manifest["Manifest-Version"] = "1.0"
        manifest["Built-By"] = "kiji-build"

        def format_entry(item):
            (key, value) = item
            item_bytes = "{}: {}".format(key, value).encode("utf-8")
            assert (len(item_bytes) < 72), "Not supported yet"  # See specs for MANIFEST files.
            yield item_bytes

        manifest_content = b"\n".join(itertools.chain(*map(format_entry, manifest.items())))

        with zipfile.ZipFile(file=jar_file_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(zinfo_or_arcname="META-INF/MANIFEST.MF", data=manifest_content)
            for dir_path in dirs:
                for root, dir_names, file_names in os.walk(dir_path):
                    dir_names.sort()
                    for file_name in sorted(file_names):
                        file_path = os.path.join(root, file_name)
                        archive_name = os.path.relpath(file_path, dir_path)
                        zf.write(filename=file_path, arcname=archive_name)
            if mapping is not None:
                for archive_path, file_path in mapping.items():
                    zf.write(filename=file_path, arcname=archive_path)

    def artifact_wpath(self, artifact):
        """Reports the workspace path for the specified Maven artifact.

        Args:
            artifact: Maven artifact coordinate to report the path of.
        Returns:
            Absolute file path for the specified Maven artifact.
        """
        file_name = "%(artifact_id)s-%(version)s.%(packaging)s" % dict(
            artifact_id=artifact.artifact_id,
            version=artifact.version,
            packaging=artifact.packaging,
        )
        path_comps = [
            self.wkspc_relpath(self.workspace.maven_local_repo),
        ] + artifact.group_id.split(".") + [
            artifact.artifact_id,
            artifact.version,
            file_name,
        ]
        return os.path.join(*path_comps)

    def artifact_abspath(self, artifact):
        """Reports the absolute file path for the specified Maven artifact.

        Args:
            artifact: Maven artifact coordinate to report the path of.
        Returns:
            Absolute file path for the specified Maven artifact.
        """
        file_name = "%(artifact_id)s-%(version)s.%(packaging)s" % dict(
            artifact_id=artifact.artifact_id,
            version=artifact.version,
            packaging=artifact.packaging,
        )
        path_comps = [
            self.workspace.maven_local_repo,
        ] + artifact.group_id.split(".") + [
            artifact.artifact_id,
            artifact.version,
            file_name,
        ]
        return os.path.join(*path_comps)

    def maven_repo_install(self, artifact, path=None, text=None, bytes=None):
        """Installs the specified artifact.

        Args:
            artifact: Artifact coordinates to install.
            path, text, bytes: Content of the Artifact to install.
                Exactly one of path, text and bytes must be set.
        Returns:
            The path of the installed artifact.
        """
        assert (len(list(filter(lambda x: x is not None, [path, text, bytes]))) == 1)

        install_path = self.artifact_abspath(artifact)
        os.makedirs(os.path.dirname(install_path), exist_ok=True)
        logging.debug("Installing %s into %s", artifact, install_path)

        if path is not None:
            shutil.copyfile(
                src=path,
                dst=install_path,
            )
        elif text is not None:
            with open(install_path, mode="wt", encoding="UTF-8") as f:
                f.write(text)
        elif bytes is not None:
            with open(install_path, mode="wb") as f:
                f.write(bytes)
        else:
            raise Error("Unexpected case")

        return install_path

    def list_files_with_suffix(self, path, suffix):
        """Lists all the files whose name ends with the specified suffix.

        Yields:
            Absolute paths of files within the specified directory
            and whose name ends with the specified suffix.
        """
        path = os.path.abspath(path)
        for root, dir_names, file_names in os.walk(path):
            for file_name in file_names:
                if file_name.endswith(suffix):
                    yield os.path.join(root, file_name)

    def list_java_files(self, path):
        """Lists all the Java files (*.java) within the specified directory."""
        return self.list_files_with_suffix(path=path, suffix=".java")

    def list_scala_files(self, path):
        """Lists all the Java files (*.java) within the specified directory."""
        return self.list_files_with_suffix(path=path, suffix=".scala")

    def link_python_binary(self, path, main_module, content_map):
        """Links a Python binary.

        Args:
            path: Path of the executable file to produce.
            main_module: Name of the main module to invoke.
            content_map: Mapping of the files to include in the binary:
                python path -> content (bytes).
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)

        wfile = open(path, "wb")
        wfile.write(b"#!/usr/bin/env python3.4\n")
        zf = zipfile.ZipFile(file=wfile, mode="w", compression=zipfile.ZIP_STORED)

        main_py = base.strip_margin("""
            |#!/usr/bin/env python3.4
            |# -*- coding: utf-8; mode: python -*-
            |
            |import runpy
            |import sys
            |
            |module_name = "{module_name}"
            |runpy.run_module(module_name, run_name="__main__", alter_sys=False)
            |"""
        ).format(module_name=main_module)
        zf.writestr(zinfo_or_arcname="__main__.py", data=main_py)

        for python_path, content_bytes in sorted(content_map.items()):
            zf.writestr(zinfo_or_arcname=python_path, data=content_bytes)
        zf.close()
        wfile.close()

        # Set execution permission:
        os.chmod(path=path, mode=0o755)

        return path

    def resolve_binary_name(self, binary_name):
        """Resolves the specified binary (identified by its string name) to its output absolute
            path.

        Args:
            binary_name: String name of the binary ("//devtools:checkstyle").
        Returns:
            Absolute file path of the specified executable.
        """
        return os.path.join(self.workspace.output_dir, "bin", binary_name[2:].replace(":", "/"))

    def resolve_maven_artifact(self, maven_artifact):
        """Resolves the maven artifact to an absolute path in the workspace's maven repository.

        This will fetch the artifact if not already present in the local maven repository.

        Args:
            maven_artifact: Artifact object to resolve.
        Returns:
            Absolute file path of the specified executable.
        """
        return self.workspace.maven_repo.get(maven_artifact)

    def resolve_package_name(self, package_name):
        """Resolves the specified package (identified by its string name) to its output absolute
            path.

        Args:
            package_name: String name of the package ("//package/group:package-name").
        Returns:
            Absolute file path of the specified package.
        """
        return os.path.join(
            self.workspace.output_dir,
            "packages",
            package_name[2:].replace(":", "/")
        )

    def run_checkstyle(
        self,
        build_classpath,
        config_path,
        sources,
        suppressions_path=None,
        header_path=None,
        jvm_args=tuple(),
    ):
        """General checkstyle command-line invocation.

        Args:
            build_classpath: Classpath to use when running checkstyle. Checkstyle classpath itself
                will be prepended to this.
            jvm_args: Arguments to pass directly to the jvm.
            args: List of command-line arguments
        Returns:
            The command object from running checkstyle (the result of command.Command()).
        """
        # Drop other classpath entries.
        cmd_env = dict(os.environ)
        cmd_env["CLASSPATH"] = ":".join(self.checkstyle_classpath + build_classpath)

        cmd_args = [self.java_path]
        cmd_args.extend(jvm_args)
        # Add flag specifying the checkstyle suppressions file.
        if suppressions_path is not None:
            cmd_args.append(
                "-Dcheckstyle.suppressions.file=" + self.resolve_wpath(suppressions_path)
            )
        # Add flag specifying the checkstyle header file.
        if header_path is not None:
            cmd_args.append("-Dcheckstyle.header.file=" + self.resolve_wpath(header_path))
        cmd_args.append("com.puppycrawl.tools.checkstyle.Main")
        cmd_args.append("-c")
        cmd_args.append(self.resolve_wpath(config_path))
        cmd_args.extend(map(self.resolve_wpath, sources))

        cmd = command.Command(
            args=cmd_args,
            env=cmd_env,
            work_dir=self.workspace.path,
        )
        return cmd

    def run_scalastyle(self, config_path, sources):
        """Scalastyle command-line invocation.

        Args:
            config_path: Path to the scalastyle configuration.
            sources: Search paths for sources that scalastyle should check.
        Returns:
            The command object from running scalastyle (the restult of command.Command()).
        """
        cmd_env = dict(os.environ)
        cmd_env["CLASSPATH"] = ":".join(self.scalastyle_classpath)

        cmd_args = [self.java_path, "org.scalastyle.Main", "-c", self.resolve_wpath(config_path)]
        cmd_args.extend(map(self.resolve_wpath, sources))

        cmd = command.Command(
            args=cmd_args,
            env=cmd_env,
            work_dir=self.workspace.path,
        )
        return cmd

    def new_fingerprint(self):
        """Construct a new fingerprint for this tooling.

        Returns:
            New Fingerprint instance, salted according to this tooling.
        """
        fp = Fingerprint()
        fp.add_text(self._fingerprint_salt)
        return fp


# --------------------------------------------------------------------------------------------------


class Fingerprint(object):
    """Helper class to fingerprint a collection of files or other inputs."""

    def __init__(self):
        """Initializes a new fingerprint accumulator."""
        self._fingerprint = hashlib.md5()

    def add_text(self, text):
        """Appends the given text string to the fingerprint.

        Args:
            text: Text to append to the fingerprint.
        """
        self._fingerprint.update(text.encode())

    def add_bytes(self, bytes):
        """Appends the given bytes to the fingerprint.

        Args:
            bytes: Byte array to append to the fingerprint.
        """
        self._fingerprint.update(bytes)

    def add_file(self, path, missing_ok=False):
        """Appends the content of the specified file to the fingerprint.

        Note: the path or the name of the file is not included in the fingerprint.

        Args:
            path: Path of the file whose content is to be appended to the fingerprint.
            missing_ok: Whether to ignore missing files or to throw FileNotFoundError.
        """
        try:
            with open(path, mode="rb") as f:
                self.add_bytes(f.read())
        except FileNotFoundError:
            if not missing_ok:
                raise

    def add_dir(self, path, missing_ok=True):
        """Appends the content of an entire directory to the fingerprint.

        Note: This recursively scans through subdirectories.
            This includes file paths (relative to the root directory path) in the fingerprint.

        Args:
            path: Path of a directory to include to the fingerprint.
        """
        path = os.path.abspath(path)
        if not missing_ok and not os.path.exists(path):
            raise FileNotFoundError(path)

        for root, dir_names, file_names in os.walk(path):
            # Ensure we recurse in sub-directories in a deterministic order (see os.walk() doc):
            dir_names.sort()

            for file_name in sorted(file_names):
                full_path = os.path.join(root, file_name)
                # Include a portable representation of the file path:
                self.add_text(os.path.relpath(full_path, path))
                self.add_file(full_path)

    def get(self):
        """Returns: the fingerprint digest, as an hexadecimal string."""
        return self._fingerprint.hexdigest()


# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    raise Exception("Not a standalone module.")
