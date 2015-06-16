#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Kiji build command-line tool."""

import getpass
import logging
import multiprocessing
import os
import shutil
import tempfile

from base import base
from base import cli

from wibi.build import workflow_task
from wibi.build import workspace
from wibi.maven import artifact
from wibi.maven import maven_repo

from workflow import workflow


FLAGS = base.FLAGS
EXIT_CODE = base.EXIT_CODE


class Error(Exception):
    """Errors used in this module."""
    pass


class StaleDependencyError(Error):
    """Raised when a project has a stale dependency on a snapshot."""
    pass


# --------------------------------------------------------------------------------------------------


# Build ID (ms since the Epoch):
BUILD_ID = base.now_ms()


# Path to the local Maven repository:
MAVEN_LOCAL_REPO = os.path.join(os.getenv('HOME'), '.m2', 'repository')


OUTPUT_PACKAGES = frozenset(['org/kiji', 'com/wibidata'])

# --------------------------------------------------------------------------------------------------


FLAGS.add_string(
    name='build_workspace_dir',
    default=('/tmp/%s/kiji_build_workspaces/%d.%d'
             % (getpass.getuser(), os.getpid(), BUILD_ID)),
    help='Local temporary directory to use for isolated build workspaces.',
)


# --------------------------------------------------------------------------------------------------


class Action(cli.Action):
    """CLI action that applies within a Kiji workspace."""

    def __init__(self, workspace, **kwargs):
        """Initializes a CLI action on the specified Kiji workspace.

        Args:
            workspace: Kiji workspace this action applies to.
        """
        super().__init__(**kwargs)
        self._workspace = workspace

    @property
    def workspace(self):
        """Returns: the Kiji workspace the action is being applied to."""
        return self._workspace


# --------------------------------------------------------------------------------------------------


class List(Action):
    """Build graph exploration tool."""

    USAGE = """
        |Lists the BUILD targets.
        |
        |Usage:
        |    kiji-build list [--filter-kind=<kind>] <target1> ...
        |
        |Example:
        |- List all the targets in the workspace:
        |    $ kiji-build list
        |
        |- List all the java_binary targets in the workspace:
        |    $ kiji-build list --filter-kind=java_binary
        |
        |- List the direct dependencies of "//org/kiji/schema:kiji-schema":
        |    $ kiji-build list --deps=upstream //org/kiji/schema:kiji-schema
        |
        |- List the build targets that depend on "//org/kiji/schema:kiji-schema":
        |    $ kiji-build list --deps=downstream-transitive //org/kiji/schema:kiji-schema
        |
        |Note:
        |   Targets are always formatted as follows: "//path/to/package:name".
        |   In order to select a specific task, you may specify a task ID with "id:<task-id>".
        """

    def register_flags(self):
        self.flags.add_string(
            name="filter-kind",
            default=None,
            help="Filters BUILD targets by kind (eg. 'java_library').",
        )
        self.flags.add_string(
            name="deps",
            default="none",
            help=("Whether to include direct or transitive dependencies, forward or reverse.\n"
                  "Accepted values are: 'none' (the default), 'upstream', 'downstream', "
                  "'upstream-transitive', 'downstream-transitive'.\n"
                  " - upstream selects the dependencies of the specified tasks.\n"
                  " - downstream selects the tasks that depend on the specified tasks."),
        )

    def run(self, args):
        flow = self.workspace.make_workflow()

        # Initial target/task ID selection:
        args = frozenset(args)
        if len(args) == 0:
            # If no target is specified, default to all targets:
            task_ids = frozenset(flow.tasks.keys())
        else:
            task_ids = set()
            targets = set()
            for arg in args:
                if arg.startswith("id:"):
                    task_ids.add(arg[3:])
                else:
                    targets.add(arg)
            # Resolve target names into task IDs:
            for task in flow.tasks.values():
                if task.spec.target in targets:
                    task_ids.add(task.task_id)
        tasks = frozenset(task_ids)

        # Apply dependency selector, if any:
        if self.flags.deps == "none":
            pass

        elif self.flags.deps == "upstream":
            direct = set()
            for task_id, task in flow.tasks.items():
                if len(tasks.intersection(task.runs_before)) > 0:
                    direct.add(task_id)
            tasks = frozenset(direct)

        elif self.flags.deps == "upstream-transitive":
            tasks = list(map(lambda tid: flow.tasks[tid], tasks))
            flow.prune(tasks=tasks, direction=workflow.UPSTREAM)
            tasks = list(flow.tasks.keys())

        elif self.flags.deps == "downstream":
            reverse = set()
            for task_id, task in flow.tasks.items():
                if len(tasks.intersection(task.runs_after)) > 0:
                    reverse.add(task_id)
            tasks = frozenset(reverse)

        elif self.flags.deps == "downstream-transitive":
            tasks = list(map(lambda tid: flow.tasks[tid], tasks))
            flow.prune(tasks=tasks, direction=workflow.DOWNSTREAM)
            tasks = list(flow.tasks.keys())

        else:
            raise Error("Invalid value for flag --deps={!r}".format(self.flags.deps))

        def list_tasks():
            for task_id in tasks:
                task = flow.tasks[task_id]
                if self.flags.filter_kind is not None:
                    if task.spec.kind != self.flags.filter_kind:
                        continue
                yield (task.spec.target, task_id)

        for (target, task_id) in sorted(list_tasks()):
            print("{target!s:40s}\t{task_id!s}".format(target=target, task_id=task_id))


# --------------------------------------------------------------------------------------------------


class Build(Action):
    """Builds and tests the workspace."""

    USAGE = """
        |Builds and tests the workspace.
        |
        |Usage:
        |  kiji-build build [--targets=]<target> ...
        """

    def register_flags(self):
        self.flags.add_string(
            name="targets",
            default=None,
            help=("Comma separated list of Maven artifacts to build.\n"
                  "None, empty or 'all' means build all artifacts.\n"),
        )

        self.flags.add_boolean(
            name="force-build",
            default=False,
            help="When set, force a rebuild irrespective of incremental status.",
        )

        self.flags.add_integer(
            name="nworkers",
            default=multiprocessing.cpu_count(),
            help="Number of workers to process build tasks."
        )

        self.flags.add_string(
            name="http-monitor",
            default="0.0.0.0:1646",
            help="Optional HTTP address to monitor the build flow."
        )

        self.flags.add_boolean(
            name="enable-incremental-testing",
            default=True,
            help=("When set, tests that have been run before successfully will be skipped if no "
                  "changes to its dependencies have been made."),
        )

        # TODO(DEV-429): Refactor these flags, they are redundant and unwieldy
        self.flags.add_string(
            name="output-dot",
            default=None,
            help="When set writes the final build graph as a dot file out to the specified file."
        )

        self.flags.add_string(
            name="output-svg",
            default=None,
            help="When set writes the final build graph as a svg file out to the specified file."
        )

        self.flags.add_string(
            name="output-table",
            default=None,
            help="When set writes the final build table as a text file to the specified file."
        )

        # Test logs:
        self.flags.add_boolean(
            name="separate-test-logs",
            default=True,
            help=("When set logs from tests will be written to files separate from the main log.\n"
                  "When unset, logs from tests will be appended to the kiji-build main log file."),
        )
        self.flags.add_string(
            name="test-log-prefix",
            default=None,
            help="Prefix to apply to printed log directory paths.",
        )

        # Workspace cloning:
        self.flags.add_boolean(
            name="clone-workspace",
            default=False,
            help=("Build the workspace in a temporary directory.\n"
                  "This creates a copy of the workspace in a temporary directory."),
        )
        self.flags.add_boolean(
            name="delete-cloned-workspace",
            default=True,
            help="Whether to delete the workspace copy, if --clone-workspace is set.",
        )

    @property
    def verb(self):
        return "build"

    def run(self, args):
        # Generate a unique ID for this build.
        build_id = "build-%d" % base.now_ms()

        # Determine which workspace to build in:
        workspace = self.workspace
        if self.flags.clone_workspace:
            base.make_dir(FLAGS.build_workspace_dir)
            temp_workspace = os.path.join(tempfile.mkdtemp(dir=FLAGS.build_workspace_dir), build_id)
            workspace = self.workspace.Clone(temp_workspace)

        targets = []
        if (self.flags.targets is not None) and (len(self.flags.targets) > 0):
            targets.extend(self.flags.targets.split(","))
        targets.extend(args)
        targets = frozenset(targets)
        self._targets = targets

        http_monitor = None
        if (self.flags.http_monitor is not None) and (len(self.flags.http_monitor) > 0):
            (interface, port) = base.parse_host_port(self.flags.http_monitor)
            try:
                http_monitor = workflow.WorkflowHTTPMonitor(interface=interface, port=port)
                http_monitor.start()
            except OSError as os_error:
                if (os_error.errno == 48) or (os_error.errno == 98):
                    raise Error(
                        "Address {!s}:{!s} already in use. Specify a different address "
                        "for the workflow http monitor with the --http-monitor flag."
                        .format(interface, port)
                    )
                else:
                    raise

        # Pass flags through the workspace config object.
        workspace.config.enable_incremental_testing = self.flags.enable_incremental_testing
        workspace.config.separate_test_logs = self.flags.separate_test_logs
        workspace.config.test_log_prefix = self.flags.test_log_prefix

        # Run the build:
        try:
            flow = workspace.make_workflow(force_build=self.flags.force_build)
            self._flow = flow

            self.adjust_workflow()
            flow = workspace.process(
                nworkers=self.flags.nworkers,
                http_monitor=http_monitor,
            )

            # TODO(DEV-429): refactor these flags, they are unwieldy
            if self.flags.output_dot is not None:
                with open(self.flags.output_dot, "wt", encoding="UTF-8") as dot_file:
                    dot_file.write(flow.dump_as_dot())

            if self.flags.output_svg is not None:
                with open(self.flags.output_svg, "wt", encoding="UTF-8") as svg_file:
                    svg_file.write(flow.dump_as_svg())

            if self.flags.output_table is not None:
                with open(self.flags.output_table, "wt", encoding="UTF-8") as table_file:
                    table_file.write(flow.dump_state_as_table())

            return len(flow.failed_tasks)  # 0 means success, non 0 means failure

        finally:
            if self.flags.clone_workspace and self.flags.delete_cloned_workspace:
                logging.info("Deleting cloned workspace %s", temp_workspace)
                shutil.rmtree(path=temp_workspace)

    @property
    def flow(self):
        """Returns: the workflow generated after the build graph."""
        return self._flow

    @property
    def targets(self):
        """Returns: the set of build target names selected by the user."""
        return self._targets

    def select_tasks(self, fun):
        """Retains the tasks matching the specified criteria."""
        root_tasks = filter(fun, self.flow.tasks.values())
        self.flow.prune(root_tasks, direction=workflow.UPSTREAM)

    def adjust_workflow(self):
        """Subclasses should override to configure the build graph."""
        if len(self.targets) > 0:
            self.select_tasks(lambda task: task.name in self.targets)

        # Selects build tasks only:
        # TODO(DEV-430): this is too magical, tasks should expose searchable attributes.
        def select_build_tasks(task):
            return isinstance(task, workflow_task.AvroJavaLibraryTask) or \
                isinstance(task, workflow_task.JavaBinaryTask) or \
                isinstance(task, workflow_task.JavaLibraryTask) or \
                isinstance(task, workflow_task.JavaSuperBinaryTask) or \
                isinstance(task, workflow_task.PythonBinaryTask) or \
                isinstance(task, workflow_task.PythonLibraryTask) or \
                isinstance(task, workflow_task.ScalaLibraryTask) or \
                isinstance(task, workflow_task.NpmInstallTask) or \
                isinstance(task, workflow_task.BowerInstallTask) or \
                isinstance(task, workflow_task.JSAppTask)
        self.select_tasks(select_build_tasks)


# --------------------------------------------------------------------------------------------------


class Run(Build):
    """Runs the specified workspace tasks."""

    USAGE = """
        |Performs the specified build tasks.
        |
        |Usage:
        |    kiji-build run [<target-or-id> <target-or-id> ...]
        """

    def register_flags(self):
        super().register_flags()

    @property
    def verb(self):
        return "task"

    def adjust_workflow(self):
        """Simply select those tasks that were manually given on the command-line."""
        self.select_tasks(lambda task: task.name in self.targets)


# --------------------------------------------------------------------------------------------------


class Test(Build):
    """Runs tests on the specified workspace targets."""

    USAGE = """
        |Builds and tests the specified workspace targets.
        |
        |Usage:
        |    kiji-build test [<target> <target> ...]
        """

    @property
    def verb(self):
        return "test"

    def adjust_workflow(self):
        self.select_tasks(lambda task: task.name.startswith("run_test"))
        if len(self.targets) > 0:
            targets = frozenset(map(lambda target: "run_test({})".format(target), self.targets))
            self.select_tasks(lambda task: task.name in targets)


# --------------------------------------------------------------------------------------------------


class GeneratePom(Build):
    """Generates pom.xml files for IDE."""

    USAGE = """
        |Generates pom.xml files for IDE.
        |
        |Usage:
        |    kiji-build generate-pom [<target> <target> ...]
        """

    @property
    def verb(self):
        return "generate-pom"

    def adjust_workflow(self):
        self.select_tasks(lambda task: isinstance(task, workflow_task.GeneratePomTask))


# --------------------------------------------------------------------------------------------------


class Checkstyle(Build):
    """Runs checkstyle on the projects configured to use it."""

    USAGE = """
        |Runs checkstyle on the projects configured to use checkstyle.
        |
        |Usage:
        |    kiji-build checkstyle [<target> <target> ...]
        """

    @property
    def verb(self):
        return "checkstyle"

    def adjust_workflow(self):
        self.select_tasks(lambda task: isinstance(task, workflow_task.RunCheckstyleTask))
        if len(self.targets) > 0:
            targets = frozenset(map(lambda target: "run_checkstyle({})".format(target), self.targets))
            self.select_tasks(lambda task: task.name in targets)


# --------------------------------------------------------------------------------------------------


class Scalastyle(Build):
    """Runs scalastyle on the projects configured to use it."""

    USAGE = """
        |Runs scalastyle on the projects configured to use scalastyle.
        |
        |Usage:
        |    kiji-build scalastyle [<target> <target> ...]
        """

    @property
    def verb(self):
        return "scalastyle"

    def adjust_workflow(self):
        self.select_tasks(lambda task: isinstance(task, workflow_task.RunScalastyleTask))
        if len(self.targets) > 0:
            targets = frozenset(
                map(lambda target: "run_scalastyle({})".format(target), self.targets))
            self.select_tasks(lambda task: task.name in targets)


# --------------------------------------------------------------------------------------------------


class Lint(Build):
    """Runs linters (currently: checkstyle, scalastyle) on the projects configured to use it."""

    USAGE = """
        |Runs linters (currently: checkstyle, scalastyle) on the projects configured to use it.
        |
        |Usage:
        |    kiji-build lint [<target> <target> ...]
        """

    @property
    def verb(self):
        return "lint"

    def adjust_workflow(self):
        def is_lint_task(task):
            return isinstance(task, workflow_task.RunCheckstyleTask) \
                or isinstance(task, workflow_task.RunScalastyleTask)
        self.select_tasks(is_lint_task)


# --------------------------------------------------------------------------------------------------

XML_HEADER = ("""<?xml version="1.0" encoding="UTF-8"?>""" + "\n")

ECLIPSE_PROJECT_XML = """
<projectDescription>
  <name>{project_name}</name>
  <comment></comment>
  <projects>
  </projects>
  <buildSpec>
    <buildCommand>
      <name>org.eclipse.jdt.core.javabuilder</name>
      <arguments>
      </arguments>
    </buildCommand>
  </buildSpec>
  <natures>
    <nature>org.eclipse.jdt.core.javanature</nature>
  </natures>
</projectDescription>
"""

ECLIPSE_CLASSPATH_XML = """
<classpath>
  <classpathentry kind="src" path="src/test/java"/>
  <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER"/>
  <classpathentry combineaccessrules="false" kind="src" path="/kiji-delegation"/>
  <classpathentry kind="lib" path="/W/wibi/output/maven_repository/junit/junit/4.11/junit-4.11.jar"/>
  <classpathentry kind="output" path="bin"/>

  <classpathentry kind="src" path=""/>
  <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER"/>
  <classpathentry exported="true" kind="lib" path="/W/wibi/output/maven_repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar"/>
  <classpathentry combineaccessrules="false" kind="src" path="/annotations"/>
  <classpathentry kind="output" path=""/>
</classpath>
"""


def eclipse_classpath_entry(
    path,
    kind="src",  # src, lib, con (container), output
    exported=None,
    source_path=None,
    combine_access_rules=None,
):
    elts = [
        "kind=\"%s\"" % kind,
        "path=\"%s\"" % path
    ]
    if exported is not None:
        elts.append("exported=\"%s\"" % ("true" if exported else "false"))
    if source_path is not None:
        elts.append("sourcepath=\"%s\"" % source_path)
    if combine_access_rules is not None:
        elts.append("combineaccessrules=\"%s\"" % ("true" if combine_access_rules else "false"))
    return "<classpathentry %s />" % " ".join(elts)


class Eclipse(Action):
    """Generates Eclipse projects."""

    def register_flags(self):
        self.flags.add_string(
            name="eclipse-workspace",
            default=None,
            help="",
        )

        self.flags.add_string(
            name="targets",
            default=None,
            help=("Comma separated list of Maven artifacts to build.\n"
                  "None, empty or 'all' means build all artifacts.\n"),
        )
        self.flags.add_string(
            name="from-targets",
            default=None,
            help=("Comma separated list of Maven artifacts to build from.\n"
                  "None or empty means build from all artifacts.\n"),
        )

    def run(self, args):
        targets = []
        if (self.flags.targets is not None) and (len(self.flags.targets) > 0):
            targets.extend(self.flags.targets.split(","))
        targets.extend(args)
        targets = frozenset(targets)

        if "all" in targets:
            targets = frozenset()

        from_targets = []
        if (self.flags.from_targets is not None) and (len(self.flags.from_targets) > 0):
            from_targets.extend(self.flags.from_targets.split(","))
        from_targets = frozenset(from_targets)

        if "all" in from_targets:
            from_targets = frozenset()

        flow = self.workspace.make_workflow(config=self.workspace.config)

        # Restrict the build to the requested targets:
        targets = frozenset(targets)
        if len(targets) > 0:
            targets = map(lambda target: flow.GetTask(target), targets)
            flow.prune(targets, direction=workflow.UPSTREAM)

        from_targets = frozenset(from_targets)
        if len(from_targets) > 0:
            from_targets = map(lambda target: flow.GetTask(target), from_targets)
            flow.prune(from_targets, direction=workflow.DOWNSTREAM)

        ewkspc = self.flags.eclipse_workspace
        assert os.path.exists(os.path.join(ewkspc, ".metadata"))

        for name, task in flow.tasks.items():
            logging.info("Generating Eclipse project for %r", name)
            eproject_dir = os.path.join(ewkspc, name[2:].replace(":", "/"))
            os.makedirs(eproject_dir, exist_ok=True)
            eproject_path = os.path.join(eproject_dir, ".project")
            with open(eproject_path, mode="wt", encoding="UTF-8") as file:
                file.write(XML_HEADER)
                file.write(ECLIPSE_PROJECT_XML.format(project_name=name[2:].replace("/", ".")))

            eclasspath_path = os.path.join(eproject_dir, ".classpath")
            output_path = os.path.join(self.workspace.output_dir, "eclipse")

            with open(eclasspath_path, mode="wt", encoding="UTF-8") as file:
                file.write(XML_HEADER)
                file.write("<classpath>\n")
                file.write(eclipse_classpath_entry(kind="con", path="org.eclipse.jdt.launching.JRE_CONTAINER") + "\n")
                file.write(eclipse_classpath_entry(kind="output", path=output_path) + "\n")
                for source in task.spec.sources:
                    if source.endswith("**.java"):
                        file.write(eclipse_classpath_entry(
                            kind="src",
                            path=self.workspace.tools.resolve_wpath(source[:-7]),
                        ))
                        file.write("\n")

                for dep in task.spec.deps:
                    file.write(eclipse_classpath_entry(
                        kind="src",
                        path="/" + dep[2:].replace("/", "."),
                        combine_access_rules=False,  # Project-Project dependency
                    ))
                    file.write("\n")
                for maven_dep in task.spec.maven_deps:
                    artf = artifact.parse_artifact(maven_dep)
                    jar_path = self.workspace.tools.artifact_abspath(artf)

                    file.write(eclipse_classpath_entry(
                        kind="lib",
                        path=jar_path,
                        # source_path=...
                    ))
                    file.write("\n")

                file.write("</classpath>\n")


# --------------------------------------------------------------------------------------------------


USAGE = """\
Usage:
    kiji-build [--do=]command [--flags ...] arguments...

Available commands:
    build                   Builds the workspace targets (libraries, binaries, tests).
    test                    Runs the tests defined in the workspace.
    checkstyle              Runs checkstyle on the workspace targets.
    scalastyle              Runs scalastyle on the workspace targets.
    lint                    Runs linters (scalastyle, checkstyle) on the workspace targets.
    generate-pom            Generates maven pom.xml files for the workspace targets.

    help                    displays this help

Command-specific help may be obtained with:
    kiji-build <command> --help

Wiki documentation:
    https://wiki.wibidata.com/display/ENG/kiji-build+v2
"""


class Help(cli.Action):
    def run(self, args):
        this = base.get_program_name()
        if (len(args) == 1) and (args[0] in ACTIONS):
            action = args[0]
            print('Usage for %s %s' % (this, action))
            ACTIONS[action](workspace=None)(['--help'])
        else:
            print(USAGE % {'this': this})
            FLAGS.print_usage()


# --------------------------------------------------------------------------------------------------


# Map: action-name (dash-separated) -> CLI Action class :
ACTIONS = dict(
    map(lambda cls: (base.un_camel_case(cls.__name__, separator='-'), cls),
        Action.__subclasses__()
        + [Run, Test, Checkstyle, Scalastyle, Lint, GeneratePom, Help])
)


class KijiBuildTool(cli.Action):
    """Command-line tool dispatcher for kiji-build."""

    def __init__(self, **kwargs):
        super().__init__(
            help_flag=cli.HelpFlag.ADD_NO_HANDLE,
            **kwargs
        )

    def register_flags(self):
        self.flags.add_string(
            name='do',
            default=None,
            help=('Action to perform:\n%s.'
                  % ',\n'.join(map(lambda name: ' - ' + name, sorted(ACTIONS)))),
        )
        self.flags.add_string(
            name='workspace',
            default=None,
            help=('Workspace base directory.\n'
                  'The workspace contains all the projects.\n'
                  'None means use the enclosing Kiji workspace.'),
        )
        self.flags.add_string(
            name="maven-repositories",
            default=None,
            help=("Optional explicit list of remote Maven repositories to use.\n"
                  "Overrides the 'maven_repositories' list in .workspace_config/conf.py"),
        )

    def run(self, args):
        # Identify action to run:
        action_name = self.flags.do
        if (action_name is None) and (len(args) >= 1) and (args[0] in ACTIONS):
            action_name, args = args[0], args[1:]
        action_class = ACTIONS.get(action_name, Help)

        # Short-circuit action that do not require a workspace:
        if not issubclass(action_class, Action):
            action = action_class(parent_flags=self.flags)
            return action(args)

        # Locate workspace:
        wkspc = self.flags.workspace
        if wkspc is None:
            wkspc = os.getcwd()
        resolved_wkspc = workspace.find_workspace_root(path=wkspc)
        assert (resolved_wkspc is not None), ('Cannot locate workspace for %s.' % wkspc)
        wkspc = resolved_wkspc
        logging.info('Using workspace: %r' % wkspc)

        repo = None
        if self.flags.maven_repositories is not None:
            remotes = self.flags.maven_repositories.split(",")
            wkspc_maven_repo = os.path.join(wkspc, "output", "maven_repository")
            remotes.insert(0, "file://" + wkspc_maven_repo)

            # Remote Maven repositories:
            repo = maven_repo.MavenRepository(
                local=wkspc_maven_repo,
                remotes=remotes,
            )

        # Run action in configured workspace:
        wkspc = workspace.Workspace(
            path=wkspc,
            maven_repository=repo,
        )

        tool = action_class(workspace=wkspc, parent_flags=self.flags)
        return tool(args)


# --------------------------------------------------------------------------------------------------


def main(args):
    """Program entry point.

    Args:
        args: unparsed command-line arguments.
    """
    tool = KijiBuildTool(parent_flags=base.FLAGS)
    return tool(args)


if __name__ == "__main__":
    base.run(main)
