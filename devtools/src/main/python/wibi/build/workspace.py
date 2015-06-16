#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Interface of a workspace."""

import collections
import logging
import multiprocessing
import os

from base import base
from base import record

from wibi.build import build_defs
from wibi.build import build_tools
from wibi.build import workflow_task
from wibi.maven import artifact
from wibi.maven import maven_repo
from wibi.scripts import git_wrapper

from workflow import workflow


# Name of the directory present in any workspace root directory:
# Testing the existence of this directory allows to detect the root of a workspace.
WORKSPACE_CONFIG = ".workspace_config"


class Error(Exception):
    """Errors used in this module."""
    pass


class StaleDependencyError(Error):
    """Raised when a project has a stale dependency on a snapshot."""
    pass


# --------------------------------------------------------------------------------------------------


def find_workspace_root(path):
    """Locates the enclosing workspace containing a given file or directory.

    Args:
        path: Path of the file or directory to identify the enclosing workspace of.
    Returns:
        The path of the enclosing workspace, if any, or None.
    """
    path = os.path.abspath(path)
    while path != "/":
        if os.path.exists(os.path.join(path, WORKSPACE_CONFIG)):
            logging.debug("Found workspace root %r", path)
            return path
        path = os.path.dirname(path)
    return None


# --------------------------------------------------------------------------------------------------


def remove_tasks_of_type(flow, task_type):
    """Prunes all tasks of the specified type from the build graph.

    Args:
        flow: The workflow to prune.
        task_type: The task type to prune.
    """
    root_tasks = flow.tasks.values()
    root_tasks = filter(lambda task: not isinstance(task, task_type), root_tasks)
    flow.prune(root_tasks, direction=workflow.UPSTREAM)


def remove_tasks_not_of_types(flow, task_types):
    """Prunes all tasks not of the specified type from the build graph.

    Args:
        flow: The workflow to prune.
        task_types: The task types to keep.
    """
    def is_a_task_type(task_internal, task_types_internal):
        return any(map(lambda task_type: isinstance(task_internal, task_type), task_types_internal))
    root_tasks = flow.tasks.values()
    root_tasks = filter(lambda task: is_a_task_type(task, task_types), root_tasks)
    flow.prune(root_tasks, direction=workflow.UPSTREAM)


# --------------------------------------------------------------------------------------------------
# Workspace


class Workspace(object):
    """Representation of a workspace content, of its configuration and of its actions.

    The workspace directory contains a configuration directory used as a sentinel to detect the
    workspace root (see find_workspace_root()).
    """

    def __init__(
        self,
        path,
        maven_repository=None,
    ):
        """Initializes a new workspace object.

        Args:
            path: Root path of the workspace.
            maven_repository: Optional explicit Maven repository where to search for artifacts.
        """
        self._path = os.path.abspath(path)
        assert os.path.exists(self.path), \
            "Workspace root directory does not exist: {!r}".format(self.path)

        assert os.path.isdir(self.config_dir), \
            "Workspace configuration directory missing: {!r}".format(self.config_dir)

        self._build_defs = build_defs.BuildDefs()

        # Process workspace configuration file:
        conf_file = os.path.join(self.config_dir, "conf.py")
        if os.path.exists(conf_file):
            logging.info("Loading configuration %r", conf_file)
            with open(conf_file, mode="rt", encoding="UTF-8") as f:
                conf_py = f.read()
            self._build_defs.eval(conf_py)

        # Process master BUILD file:
        build_file = os.path.join(self.path, "BUILD")
        if os.path.exists(build_file):
            logging.info("Loading build file %r", build_file)
            with open(build_file, mode="rt", encoding="UTF-8") as f:
                build_py = f.read()
            self._build_defs.eval(build_py)

        else:
            logging.info("BUILD file missing: %r", build_file)

        self._config = record.Record(self._build_defs.exec_locals)
        self.config.definitions = self._build_defs.definitions

        logging.debug("Using configuration: %r", self._config)

        # ------------------------------------------------------------------------------------------

        base.make_dir(self.output_dir)
        base.make_dir(self.temp_dir)
        base.make_dir(self.maven_local_repo)
        base.make_dir(os.path.join(self.temp_dir, "workflow"))
        logging.debug("Generating artifacts in: %s", self.maven_local_repo)

        # ------------------------------------------------------------------------------------------

        self._git = git_wrapper.Git(self.path)

        if maven_repository is None:
            remotes = self.config.get("maven_repositories", tuple())
            maven_repository = maven_repo.MavenRepository(
                local=self.maven_local_repo,
                remotes=remotes,
            )
            logging.debug("Using Maven repositories:\n%s",
                          base.add_margin("\n".join(remotes), "\t- "))
        self._maven_repo = maven_repository

        # Build toolkit:
        self._tools = build_tools.BuildTools(workspace=self)
        self.tools.validate()

        # Workflow generated after the build graph:
        self._workflow = None

    def __str__(self):
        return "Workspace(path={})".format(self.path)

    @property
    def path(self):
        """Returns: the absolute path for this workspace."""
        return self._path

    @property
    def config_dir(self):
        """Returns: the workspace configuration directory."""
        return os.path.join(self.path, WORKSPACE_CONFIG)

    @property
    def config(self):
        """Returns: the workspace configuration.

        The configuration includes properties declared in the workspace configuration file,
        typically in //.workspace_config/conf.py), overlaid by those declared in the BUILD file.

        Other configuration properties are finally set via command-line flags.
        """
        return self._config

    @property
    def maven_repo(self):
        """Returns: the Maven repository used to search for artifacts."""
        return self._maven_repo

    @property
    def output_dir(self):
        """Returns: the directory where to write outputs."""
        return os.path.join(self.path, "output")

    @property
    def maven_local_repo(self):
        """Returns: the Maven local repository path where to produce artifacts."""
        return os.path.join(self.output_dir, "maven_repository")

    @property
    def temp_dir(self):
        """Returns: the directory where to write temporary outputs."""
        return os.path.join(self.output_dir, "temp")

    @property
    def git(self):
        """Returns: the workspace top-level git project."""
        return self._git

    @property
    def tools(self):
        """Returns: the toolkit (BuildTools instance) for this workspace."""
        return self._tools

    # ----------------------------------------------------------------------------------------------

    def wpath(self, apath):
        """Converts an absolute file path into a workspace path.

        A workspace path is an absolute file path relative to the workspace root.
        Workspace paths are formatted "//path/to/file",
        where the leading '//' indicates the root of the workspace.

        Args:
            apath: Absolute file path to convert into a workspace path.
        Returns:
            Workspace path corresponding to the specified absolute path.
        Raises:
            Error: if the specified absolute path does not belong to the workspace.
        """
        if not apath.startswith(self.path):
            raise Error("Path {!r} does not belong to workspace {!r}".format(apath, self.path))
        return "//" + os.path.relpath(apath, self.path)

    def apath(self, wpath):
        """Converts a workspace path into an absolute file path.

        Args:
            wpath: Workspace path. Must start with '//'.
        Returns:
            Absolute file path corresponding to the specified workspace path.
        """
        if not wpath.startswith("//"):
            raise Error("Invalid workspace path {!r}".format(wpath))
        return os.path.join(self.path, wpath[2:])

    # ----------------------------------------------------------------------------------------------

    def _make_workflow(self, config, force_build=False):
        """Builds the workflow tasks after the build graph defined by the workspace.

        Args:
            config: Workspace configuration record.
            force_build: When true, force a rebuild irrespective of incremental changes.
        Returns:
            The workflow builder.
        """
        workflow_name = "build.commit={commit}.timestamp={timestamp}".format(
            commit=self.git.get_commit_hash(),
            timestamp=base.timestamp(),
        )
        flow = workflow.Workflow(name=workflow_name)

        TASK_CLASS_MAP = dict(
            avro_java_library=workflow_task.AvroJavaLibraryTask,
            generated_pom=workflow_task.GeneratePomTask,
            java_binary=workflow_task.JavaBinaryTask,
            java_library=workflow_task.JavaLibraryTask,
            java_test=workflow_task.JavaTestTask,
            java_super_binary=workflow_task.JavaSuperBinaryTask,

            js_app=workflow_task.JSAppTask,
            npm_install=workflow_task.NpmInstallTask,
            bower_install=workflow_task.BowerInstallTask,

            python_binary=workflow_task.PythonBinaryTask,
            python_library=workflow_task.PythonLibraryTask,
            python_test=workflow_task.PythonTestTask,
            run_checkstyle=workflow_task.RunCheckstyleTask,
            run_java_test=workflow_task.RunJavaTestTask,
            run_python_test=workflow_task.RunPythonTestTask,
            run_scala_test=workflow_task.RunJavaTestTask,
            run_scalastyle=workflow_task.RunScalastyleTask,
            scala_library=workflow_task.ScalaLibraryTask,
            scala_test=workflow_task.ScalaTestTask,
        )

        for name, definition in self._build_defs.definitions.items():
            task_class = TASK_CLASS_MAP[definition.kind]
            task = task_class(
                workflow=flow,
                workspace=self,
                name=name,
                spec=definition,
                force=force_build,
            )
            for dep in definition.get("deps", tuple()):
                if isinstance(dep, artifact.Artifact):
                    # Direct Maven artifact dependencies are not reified as workflow tasks:
                    continue

                if isinstance(dep, build_defs.PythonPyPIDep):
                    # Python PyPI dependencies are external dependencies:
                    continue

                if isinstance(dep, build_defs.DynamicDep):
                    dep = dep.provider
                    if dep is None:
                        continue

                task.bind_input_to_task_output(input_name=dep, task=dep)

        return flow

    def make_workflow(self, force_build=False):
        """Generates the workflow after the build graph.

        Args:
            force_build: Whether to force all build tasks to re-run.
        Returns:
            The generated workflow.
        """
        self._workflow = self._make_workflow(
            config=self.config,
            force_build=force_build,
        )
        return self._workflow

    @property
    def workflow(self):
        """Returns: the build workflow modeled after the build graph."""
        return self._workflow

    def process(
        self,
        http_monitor=None,
        nworkers=base.DEFAULT,
    ):
        """Processes the selected tasks from the BUILD.

        Args:
            http_monitor: Optional workflow.WorkflowHTTPMonitor to use for monitoring.
            nworkers: Number of concurrent tasks to process in parallel.
        """
        if nworkers is base.DEFAULT:
            nworkers = multiprocessing.cpu_count()

        # Freeze the workflow, done building the dependency graph
        self.workflow.build()

        if http_monitor is not None:
            http_monitor.set_workflow(self.workflow)

        # Execute the tasks with maximum parallelism:
        self.workflow.process(
            nworkers=nworkers,
            monitor_thread=False,
            sync=True,
        )

        logging.info("Build summary:\n{!s}".format(self.workflow.dump_summary_as_text()))

        if len(self.workflow.failed_tasks) > 0:
            logging.error("Build failed")
        else:
            logging.info("Build successful")

        return self.workflow


# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    raise Exception("Not a standalone module.")
