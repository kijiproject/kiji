#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Links a Java application."""

import collections
import os
import pkgutil
import tempfile
import zipfile

from base import base
from base import command
from base import log


# --------------------------------------------------------------------------------------------------


def list_jar_file_entries(jar_path):
    """Scans a JAR file and lists the file paths it contains.

    Args:
        jar_path: Path of the JAR file to scan.
    Yield:
        Path of the files included in the JAR file.
    """
    if os.path.isdir(jar_path):
        return

    try:
        with zipfile.ZipFile(jar_path) as zf:
            for info in zf.infolist():
                yield info.filename
    except zipfile.BadZipFile:
        log.error("{!r} is not a valid JAR file.", jar_path)


def class_name_from_path(path):
    """Parses the name of a Java class from its file path.

    Args:
        path: Relative path of a Java class file.
    Returns:
        The fully-qualified name of the Java class.
    """
    suffix = ".class"
    assert path.endswith(suffix)
    return path[:-len(suffix)].replace("/", ".")


def report_overlapping_jars(cp_entries):
    """Scans a collection of JAR files and reports classes where shadowing occurs.

    Args:
        cp_entries: Collection of classpath entries to validate.
    Yields:
        Pairs (class name, set of JAR file paths containing the class).
    """

    # Map: class name -> list of JAR files containing one version of that class
    class_map = collections.defaultdict(set)

    for cp_entry in cp_entries:
        if not os.path.exists(cp_entry):
            log.warn("Classpath entry does not exist: {!r}", cp_entry)
            continue

        if not cp_entry.endswith(".jar") or not os.path.isfile(cp_entry):
            continue

        log.debug("Processing classpath entry: {!r}", cp_entry)
        for zip_entry in list_jar_file_entries(cp_entry):
            if not zip_entry.endswith(".class"):
                continue

            class_name = class_name_from_path(zip_entry)
            class_map[class_name].add(cp_entry)

    for class_name, cp_entries in sorted(class_map.items()):
        if len(cp_entries) > 1:
            yield (class_name, frozenset(cp_entries))


def log_jar_conflicts(cp_entries, maven_repo, name=None):
    conflicting_jar_sets = set()
    for class_name, jars in report_overlapping_jars(cp_entries):
        jars = frozenset(map(maven_repo.local.artifact_for_path, jars))
        jars = tuple(sorted(jars))
        conflicting_jar_sets.add(jars)

    if len(conflicting_jar_sets) > 0:
        def list_conflicts():
            for jar_set in conflicting_jar_sets:
                yield "   ".join(map(str, jar_set))
        if name is None:
            name = ""
        else:
            name = " for {} ".format(name)
        log.warning("Conflicting JAR sets{}:\n\t{}", name, "\n\t".join(list_conflicts()))


# --------------------------------------------------------------------------------------------------


# Shell header for a JVM executable:
JAVA_EXE_HEADER = """\
#!/bin/sh
java -jar "$0" "$@"
"""


# Shell header for a Python executable (PEX):
PYTHON_EXE_HEADER = """\
#!/usr/bin/env python3.4
"""


def _get_python_module_source(module_path):
    """Loads and returns the specified Python module source.

    Args:
        module_path: Module path, eg. "foo.bar".
    Returns:
        The source code of the specified module.
    """
    loader = pkgutil.get_loader(module_path)
    return loader.get_source(module_path)


JAVA_EXE_PYTHON_LAUNCHER = _get_python_module_source("wibi.build.java_exe_maven_repo_stub")
BASE_BASE_PY = _get_python_module_source("base.base")
BASE_CLI_PY = _get_python_module_source("base.cli")
BASE_LOG_PY = _get_python_module_source("base.log")


# --------------------------------------------------------------------------------------------------


class JavaLinker(object):
    """Helper to construct Java executables."""

    def __init__(
        self,
        output_path,
        maven_repo,
        compression=zipfile.ZIP_STORED,
    ):
        """Initializes a new JVM executable linker.

        Args:
            output_path: Path of the executable to construct.
            maven_repo: Maven repository wrapper to load artifacts.
            compression: ZIP compression algorithm to use.
        """
        self._output_path = output_path
        self._output_file = open(self._output_path, mode="wb")
        self._output_zip = \
            zipfile.ZipFile(file=self._output_file, mode="w", compression=compression)
        self._zip_entry_names = set()
        self._maven_repo = maven_repo

    @property
    def output_path(self):
        """Returns: the path of the executable being created."""
        return self._output_path

    @property
    def maven_repo(self):
        """Returns: the Maven repository used to resolve artifacts."""
        return self._maven_repo

    def close(self):
        """Closes this linker."""
        self._output_zip.close()
        self._output_file.close()
        self._output_file = None
        os.chmod(path=self._output_path, mode=0o755)

    def write_header(self, text):
        self._output_file.write(text.encode())

    def append_file_entry(self, name, path):
        """Appends the specified file to the ZIP archive.

        Args:
            name: Name of the entry in the ZIP file.
            path: Path of the file on the local file system.
        """
        assert (name not in self._zip_entry_names), "Duplicate ZIP entry: {}".format(name)
        self._zip_entry_names.add(name)
        self._output_zip.write(arcname=name, filename=path)

    def append_entry(self, name, data):
        """Appends the specified file to the ZIP archive.

        Args:
            name: Name of the entry in the ZIP file.
            data: Data (bytes or string) to associate to the name in the ZIP archive.
        """
        assert (name not in self._zip_entry_names), "Duplicate ZIP entry: {}".format(name)
        self._zip_entry_names.add(name)
        self._output_zip.writestr(zinfo_or_arcname=name, data=data)

    def merge_zip(self, path):
        """Merges an existing ZIP archive (eg. a JAR file) into this ZIP archive.

        Args:
            path: Path of the existing ZIP archive to merge.
        """
        with zipfile.ZipFile(file=path, mode="r") as zf:
            for entry in zf.infolist():
                name = entry.filename
                self.append_entry(name=name, data=zf.read(name))

    def write_python_launcher(self, config):
        python_launcher = JAVA_EXE_PYTHON_LAUNCHER
        self.append_entry(name="__main__.py", data=JAVA_EXE_PYTHON_LAUNCHER)
        self.append_entry(name="base/__init__.py", data="")
        self.append_entry(name="base/base.py", data=BASE_BASE_PY)
        self.append_entry(name="base/cli.py", data=BASE_CLI_PY)
        self.append_entry(name="base/log.py", data=BASE_LOG_PY)
        self.append_entry(name="java/__init__.py", data="")
        self.append_entry(name="java/config", data=repr(config))

    def write_entries(self, entry_map):
        for archive_name, file_path in sorted(entry_map.items()):
            self.append_file_entry(name=archive_name, path=file_path)

    def archive_relpath_for_artifact(self, artifact):
        """Returns: the archive relative path for the specified artifact.

        Args:
            artifact: Artifact coordinate to report the archive path for.
        Returns:
            The archive relative path for the specified artifact.
        """
        return os.path.join("libs", self.maven_repo.local.get_path(artifact))

    def make_profile(self, artifacts, main_class=None, jvm_args=tuple()):
        """Prepares the configuration of the specified JVM executable.

        Args:
            artifacts: Ordered list of Maven artifact coordinates to include in the executable.
            main_class: Optional default Java class to invoke.
            jvm_args: Optional default JVM arguments to include.
        Returns:
            A tuple with:
             - the configuration descriptor for the JVM executable.
             - the JAR archive entry map for the executable,
               containing a mapping: archive relative path -> absolute file path.
        """
        artifacts = tuple(artifacts)

        # Map: archive relative file path -> absolute file path
        entry_map = collections.OrderedDict()
        for artf in artifacts:
            entry_map[self.archive_relpath_for_artifact(artf)] = self.maven_repo.get(artf)

        log_jar_conflicts(
            cp_entries=entry_map.values(),
            maven_repo=self.maven_repo,
            name=self.output_path,
        )

        config = dict(
            classpath=tuple(entry_map.keys()),  # Maintain ordering of artifacts = entry_map keys
            artifacts=tuple(map(str, artifacts)),
            main_class=main_class,
            jvm_args=tuple(jvm_args),
        )

        return (config, entry_map)

    def link(
        self,
        artifacts,
        main_class=None,
        jvm_args=tuple(),
    ):
        """Links a Java executable from a flat list of resolved Maven dependencies.

        Args:
            artifacts: Ordered list of Maven artifact coordinates to include in the executable.
            main_class: Optional default Java class to invoke.
            jvm_args: Optional default JVM arguments to include.
        """
        (profile, entry_map) = \
            self.make_profile(artifacts=artifacts, main_class=main_class, jvm_args=jvm_args)

        profile_name = os.path.basename(self.output_path)

        config = dict(
            artifacts=profile["artifacts"],
            default_profile=profile_name,
            profiles={profile_name: profile},
        )

        self.write_header(PYTHON_EXE_HEADER)
        self.write_python_launcher(config=config)
        self.write_entries(entry_map)

        return config

# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    raise Exception("Not a standalone module.")
