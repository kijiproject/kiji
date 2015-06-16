#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Collection of BUILD definitions."""


import collections
import logging


from base import record
from wibi.maven import artifact


# --------------------------------------------------------------------------------------------------


class Error(Exception):
    """Errors used in this module."""
    pass


# --------------------------------------------------------------------------------------------------


SourceSpec = collections.namedtuple(
    typename="SourceSpec",
    field_names=(
        # Root directory, as a workspace path (wpath):
        "root",

        # File selector.
        #  - "path/to/file" selects a single file
        #  - "path/to/*.py" selects all files in relative folder path/to with the .py extension.
        #  - "path/to/**.java" selects all .java files recursively in the folder path/to.
        "selector",
    ),
)


DynamicDep = collections.namedtuple(
    typename="DynamicDep",
    field_names=(
        # Name of the slot this dependency provides:
        "slot",

        # Name of the provider for this slot:
        "provider",
    ),
)


PythonPyPIDep = collections.namedtuple(
    typename="PythonPyPIDep",
    field_names=(
        # Python package name, eg. "flake8"
        "name",

        # Version requirement, eg. "2.3.0"
        "version",
    ),
)


CheckstyleConfig = collections.namedtuple(
    typename="CheckstyleConfig",
    field_names=(
        # Workspace path to the checkstyle rules file that defines the checks to perform:
        "config",

        # Workspace path to the checkstyle rules to suppress:
        "suppressions",

        # Workspace path to the expected header file for this slot:
        "header",
    ),
)


# --------------------------------------------------------------------------------------------------


class BuildDefs(object):
    """Processes a BUILD file and registers the definitions it contains."""

    def __init__(self):
        # Map: definition name -> spec
        self._definitions = dict()

        self._exec_locals = dict()

    def eval(self, build_source):
        """Evaluates the given BUILD definitions.

        Args:
            build_source: BUILD definitions to evaluate.
        """
        logging.debug("Processing BUILD source: %r", build_source)
        exec(build_source, dict(self.exec_globals), self._exec_locals)

    @property
    def exec_globals(self):
        return dict(
            avro_java_library=self.avro_java_library,
            checkstyle=self.checkstyle_config,
            dynamic=self.dynamic_dep,
            generated_pom=self.generated_pom,
            java_binary=self.java_binary,
            java_library=self.java_library,
            java_test=self.java_test,
            java_super_binary=self.java_super_binary,

            js_app=self.js_app,
            npm_install=self.npm_install,
            bower_install=self.bower_install,

            maven=self.maven_dep,
            pypi=self.pypi,
            python_binary=self.python_binary,
            python_library=self.python_library,
            python_test=self.python_test,
            scala_library=self.scala_library,
            scala_test=self.scala_test,
            source=self.source,
        )

    @property
    def exec_locals(self):
        return self._exec_locals

    @property
    def definitions(self):
        return self._definitions

    def declare(self, name, kind, deps=tuple(), target=None, **kwargs):
        """Declares a new target.

        Args:
            name: Target task name (unique task ID).
            kind: Target kind, eg. "java_library" or "python_test".
            deps: Dependencies on other targets.
            target: Target this task contributes to.
                Different tasks make contribute to the same target.
            **kwargs: Target-specific parameters.
        Returns:
            Target specification record.
        """
        if target is None:
            target = name

        kwargs["name"] = name
        kwargs["kind"] = kind
        kwargs["deps"] = deps
        kwargs["target"] = target

        spec = record.Record(kwargs)
        if name in self._definitions:
            raise Error("Duplicate definition for {name}:\n{existing}\n{duplicate}"
                        .format(name=name, existing=self._definitions[name], duplicate=spec))
        self._definitions[name] = spec
        return spec

    # ----------------------------------------------------------------------------------------------
    # Below are the BUILD definition macros:

    def avro_java_library(
        self,
        name,
        sources,
        deps=tuple(),
        maven_exclusions=tuple(),
        version=None,
        ipc=False,
        target=None,
    ):
        return self.declare(
            kind="avro_java_library",
            name=name,
            sources=sources,
            deps=deps,
            maven_exclusions=maven_exclusions,
            version=version,
            ipc=ipc,
            target=target,
        )

    def java_library(
        self,
        name,
        sources=tuple(),
        resources=tuple(),
        deps=tuple(),
        maven_exclusions=tuple(),
        version=None,
        processors=tuple(),
        processor_deps=tuple(),
        provides=tuple(),
        checkstyle=None,
        target=None,
        library_name=None,
    ):
        if target is None:
            target = name

        if checkstyle is not None:
            self.run_checkstyle(
                name="run_checkstyle({})".format(target),
                deps=[name],
                sources=sources,
                checkstyle_config=checkstyle,
                target=target,
            )

        return self.declare(
            kind="java_library",
            name=name,
            sources=sources,
            resources=resources,
            deps=list(deps) + list(processor_deps),
            maven_exclusions=maven_exclusions,
            version=version,
            processors=processors,
            processor_deps=processor_deps,
            provides=provides,
            target=target,
            library_name=library_name,
        )

    def java_binary(
        self,
        name,
        main_class,
        deps=tuple(),
        maven_exclusions=tuple(),
        target=None,
        binary_name=None,
        jvm_args=tuple(),
    ):
        return self.declare(
            kind="java_binary",
            name=name,
            main_class=main_class,
            deps=deps,
            maven_exclusions=maven_exclusions,
            target=target,
            binary_name=binary_name,
            jvm_args=jvm_args,
        )

    def java_test(
        self,
        name,
        sources=tuple(),
        resources=tuple(),
        deps=tuple(),
        maven_exclusions=tuple(),
        jvm_args=tuple(),
        checkstyle=None,
        target=None,
    ):
        if target is None:
            target = name

        java_test_lib = self.java_library(
            name="java_library({})".format(target),
            sources=sources,
            resources=resources,
            deps=deps,
            maven_exclusions=maven_exclusions,
            processors=["testing.TestAnnotationCollector"],
            processor_deps=["//testing:test-annotation-collector"],
            target=target,
            library_name=name,
        )
        if checkstyle is not None:
            self.run_checkstyle(
                name="run_checkstyle({})".format(java_test_lib.name),
                deps=[java_test_lib.name],
                sources=sources,
                checkstyle_config=checkstyle,
                target=target,
            )
        java_test_bin = self.java_binary(
            name="java_binary({})".format(target),
            binary_name=name,
            deps=[java_test_lib.name],
            main_class="org.junit.runner.JUnitCore",
            target=target,
            jvm_args=jvm_args,
        )

        self.declare(
            kind="run_java_test",
            name="run_test({})".format(target),
            deps=[name],
            target=target,
        )

        return self.declare(
            kind="java_test",
            name=name,
            deps=[java_test_lib.name, java_test_bin.name],
            target=target,
        )

    def java_super_binary(
        self,
        name,
        deps=tuple(),
        target=None,
        default_profile=None,
        binary_name=None,
    ):
        return self.declare(
            kind="java_super_binary",
            name=name,
            deps=deps,
            target=target,
            default_profile=default_profile,
            binary_name=None,
        )

    def scala_library(
        self,
        name,
        sources=tuple(),
        resources=tuple(),
        deps=tuple(),
        maven_exclusions=tuple(),
        version=None,
        provides=tuple(),
        checkstyle=None,
        scalastyle=None,
        target=None,
        library_name=None,
    ):
        if target is None:
            target = name

        if checkstyle is not None:
            self.run_checkstyle(
                name="run_checkstyle({!s})".format(name),
                deps=[name],
                sources=sources,
                checkstyle_config=checkstyle,
                target=target,
            )
        if scalastyle is not None:
            self.run_scalastyle(
                name="run_scalastyle({!s})".format(name),
                deps=tuple(),
                sources=sources,
                scalastyle_config=scalastyle,
                target=target,
            )

        return self.declare(
            kind="scala_library",
            name=name,
            sources=sources,
            resources=resources,
            deps=deps,
            maven_exclusions=maven_exclusions,
            version=version,
            provides=provides,
            target=target,
            library_name=library_name,
        )

    def scala_test(
        self,
        name,
        sources=tuple(),
        resources=tuple(),
        deps=tuple(),
        maven_exclusions=tuple(),
        test_name_pattern="^Test.*",
        jvm_args=tuple(),
        checkstyle=None,
        scalastyle=None,
        target=None,
    ):
        if target is None:
            target = name

        scala_test_lib = self.scala_library(
            name="java_library({})".format(name),  # Use java_library() to help with generate_pom()
            library_name=name,
            sources=sources,
            resources=resources,
            deps=deps,
            maven_exclusions=maven_exclusions,
            target=target,
        )
        if checkstyle is not None:
            self.run_checkstyle(
                name="run_checkstyle({!s})".format(scala_test_lib.name),
                deps=[scala_test_lib.name],
                sources=sources,
                checkstyle_config=checkstyle,
                target=target,
            )
        if scalastyle is not None:
            self.run_scalastyle(
                name="run_scalastyle({!s})".format(scala_test_lib.name),
                deps=tuple(),
                sources=sources,
                scalastyle_config=scalastyle,
                target=target,
            )
        scala_test_bin = self.java_binary(
            name="java_binary({})".format(name),
            binary_name=name,
            deps=[scala_test_lib.name],
            main_class="org.junit.runner.JUnitCore",
            target=target,
            jvm_args=jvm_args,
        )

        self.declare(
            kind="run_scala_test",
            name="run_test({})".format(name),
            deps=[name],
            target=target,
        )

        return self.declare(
            kind="scala_test",
            name=name,
            deps=[scala_test_bin.name],
            sources=sources,
            test_name_pattern=test_name_pattern,
            target=target,
        )

    def python_library(
        self,
        name,
        sources=tuple(),
        deps=tuple(),
        target=None,
    ):
        return self.declare(
            kind="python_library",
            name=name,
            sources=sources,
            deps=deps,
            target=target,
        )

    def python_binary(
        self,
        name,
        main_module,
        sources=tuple(),
        deps=tuple(),
        target=None,
    ):
        return self.declare(
            kind="python_binary",
            name=name,
            main_module=main_module,
            sources=sources,
            deps=deps,
            target=target,
        )

    def python_test(
        self,
        name,
        main_module="unittest",
        sources=tuple(),
        deps=tuple(),
        target=None,
    ):
        if target is None:
            target = name

        self.declare(
            kind="run_python_test",
            name="run_test({})".format(name),
            deps=[name],
            target=target,
        )

        return self.declare(
            kind="python_test",
            name=name,
            main_module=main_module,
            sources=sources,
            deps=deps,
            target=target,
        )

    def npm_install(
        self,
        name,
        source,
        deps=tuple(),
    ):
        self.declare(
            kind="npm_install",
            name=name,
            source=source,
            deps=deps,
            target=name,
        )

    def bower_install(
        self,
        name,
        source,
        deps=tuple(),
    ):
        self.declare(
            kind="bower_install",
            name=name,
            source=source,
            deps=deps,
            target=name,
        )

    def js_app(
        self,
        name,
        sources,
        outputs=tuple(),
        deps=tuple(),
        target=None,
    ):
        self.declare(
            kind="js_app",
            name=name,
            sources=sources,
            outputs=outputs,
            deps=deps,
            target=target,
        )

    def generated_pom(
        self,
        name,
        pom_name,
        pom_file,
        pom_template=None,
        main_deps=tuple(),
        test_deps=tuple(),
        main_maven_exclusions=tuple(),
        test_maven_exclusions=tuple(),
        target=None,
    ):
        # Use the java_library() dependency because the test task dependency doesn't propagate the right
        # information in its output record (input_fingerprint, maven_deps).
        test_lib_deps = []
        for test_dep in test_deps:
            test_lib_deps.append("java_library({})".format(test_dep))

        return self.declare(
            kind="generated_pom",
            name=name,
            pom_name=pom_name,
            pom_file=pom_file,
            pom_template=pom_template,
            main_deps=main_deps,
            test_deps=test_lib_deps,
            main_maven_exclusions=main_maven_exclusions,
            test_maven_exclusions=test_maven_exclusions,
            deps=list(main_deps) + test_lib_deps,
            target=target,
        )

    def run_checkstyle(
        self,
        name,
        checkstyle_config,
        sources=tuple(),
        deps=tuple(),
        target=None,
    ):
        return self.declare(
            kind="run_checkstyle",
            name=name,
            sources=sources,
            deps=deps,
            checkstyle_config=checkstyle_config,
            target=target,
        )

    def run_scalastyle(
        self,
        name,
        scalastyle_config,
        sources=tuple(),
        deps=tuple(),
        target=None,
    ):
        return self.declare(
            kind="run_scalastyle",
            name=name,
            sources=sources,
            deps=deps,
            scalastyle_config=scalastyle_config,
            target=target,
        )

    # ----------------------------------------------------------------------------------------------

    @staticmethod
    def source(root, selector):
        return SourceSpec(root=root, selector=selector)

    @staticmethod
    def dynamic_dep(**kwargs):
        assert (len(kwargs) == 1), "Invalid dynamic dependency specification: {}".format(kwargs)
        (slot, provider) = next(iter(kwargs.items()))
        return DynamicDep(slot=slot, provider=provider)

    @staticmethod
    def maven_dep(
        coord=None,
        group=None, id=None, version=None, classifier=None, type=None,
    ):
        assert (
            (coord is None)
            ^ (frozenset({group, id, version, classifier, type}) == frozenset({None}))), \
            "Invalid Maven dependency."

        if coord is not None:
            return artifact.parse_artifact(coord)

        assert not ((group is None) or (id is None) or (version is None)), \
            "Invalid Maven artifact dependency: incomplete coordinate."

        return artifact.Artifact(
            group_id=group,
            artifact_id=id,
            version=version,
            classifier=classifier,
            packaging=type,
        )

    @staticmethod
    def pypi(name, version):
        return PythonPyPIDep(name=name, version=version)

    @staticmethod
    def checkstyle_config(config, suppressions, header):
        return CheckstyleConfig(
            config=config,
            suppressions=suppressions,
            header=header,
        )
