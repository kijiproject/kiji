#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Wrapper for Maven projects."""

import collections
import itertools
import logging
import os
import re
import sys
import tempfile
import threading
import xml.dom.minidom as minidom

from base import base
from base import command
from base import record

from wibi.maven import artifact
from wibi.maven import maven_repo
from wibi.util import xml_util


FLAGS = base.FLAGS
LOG_LEVEL = base.LOG_LEVEL
DEFAULT = base.DEFAULT

Artifact = artifact.Artifact
ArtifactName = artifact.ArtifactName
get_or_default = artifact.get_or_default


class Error(Exception):
    """Errors used in this module."""
    pass


class DependencyResolutionError(Error):
    pass


class PluginResolutionError(Error):
    pass


# --------------------------------------------------------------------------------------------------


SCOPE = base.make_tuple(
    'Scope',
    COMPILE = 'compile',
    RUNTIME = 'runtime',
    PROVIDED = 'provided',
    TEST = 'test',
)


class Dependency(object):
    """Representation of an artifact dependency."""

    def __init__(
        self,
        artifact,
        scope=None,
        optional=None,
        exclusions=None,
        source=None,
    ):
        """Constructs a Dependency on a Maven artifact.

        Args:
            artifact: Artifact depended on.
            scope: Dependency scope.
                None means unset, which evaluates to compile.
            optional: Whether the dependency is optional.
                None means unset, which evaluates to false (ie. required).
            exclusions: Set of artifact exclusions.
            source: Optional source coordinate where the dependency is declared.
        """
        self._artifact = artifact
        self._scope = scope
        self._optional = optional
        if exclusions is not None:
            exclusions = tuple(sorted(frozenset(exclusions)))
        self._exclusions = exclusions
        self._source = source

    @property
    def artifact(self):
        return self._artifact

    @property
    def scope(self):
        return self._scope

    @property
    def optional(self):
        return self._optional

    @property
    def exclusions(self):
        """Returns: ordered set of exclusions (as a tuple)."""
        return self._exclusions

    @property
    def source(self):
        """Coordinate of the POM that defined this dependency."""
        return self._source

    def update(
        self,
        artifact=DEFAULT,
        scope=DEFAULT,
        optional=DEFAULT,
        exclusions=DEFAULT,
        source=DEFAULT,
    ):
        return Dependency(
            artifact=get_or_default(artifact, self._artifact),
            scope=get_or_default(scope, self._scope),
            optional=get_or_default(optional, self._optional),
            exclusions=get_or_default(exclusions, self._exclusions),
            source=get_or_default(source, self._source),
        )

    def __str__(self):
        evaluated = self.eval()
        details = [evaluated.scope]
        if evaluated.optional == True:
            details.append("optional")
        details.extend(list(map(lambda artf: "-" + str(artf), evaluated.exclusions)))
        return "dep(artifact={artifact} [{details}] source={source})".format(
            artifact=evaluated.artifact,
            details=",".join(details),
            source=evaluated.source,
        )

    def __repr__(self):
        return (
            "Dependency("
            "artifact={artifact}, "
            "scope={scope}, "
            "optional={optional}, "
            "exclusions={exclusions}, "
            "source={source})"
        ).format(
            artifact=self._artifact,
            scope=self._scope,
            optional=self._optional,
            exclusions=self._exclusions,
            source=self._source,
        )

    def eval(self):
        """Fully evaluates a dependency definition.

        Convert unspecified fields into their default values.

        Returns:
            The evaluated dependency.
        """
        scope = self._scope
        if scope is None:
            scope = SCOPE.COMPILE

        optional = self._optional
        if optional is None:
            optional = False

        exclusions = self._exclusions
        if exclusions is None:
            exclusions = ()

        return self.update(
            scope=scope,
            optional=optional,
            exclusions=exclusions,
        )

    def __eq__(self, other):
        return (
            (self._artifact == other._artifact)
            and (self._scope == other._scope)
            and (self._optional == other._optional)
            and (self._exclusions == other._exclusions)
        )

    @property
    def key(self):
        """Returns: the key for this dependency."""
        return self._artifact.key


# --------------------------------------------------------------------------------------------------


def dep_set(deps, context=None):
    """Reproduces Maven's behavior on collections of dependencies.

    In a conflict within a dependency set (single XML node), the last dependency wins.

    Returns:
        Ordered set of dependencies (guaranteed unique).
    """
    # Reverse-ordered set of dependencies:
    unique = list()

    # Map: dependency key -> dependency
    deps_map = dict()

    # Process dependencies in reverse order, so that last dependencies win:
    for dep in reversed(list(deps)):
        dep_key = dep.key
        existing = deps_map.get(dep_key)
        if existing is None:
            deps_map[dep_key] = dep
            unique.append(dep)
        elif dep != existing:
            logging.debug("In project %s : ignoring conflicting dependency %s and using %s",
                          context, dep, existing)
        else:
            logging.debug("In project %s : ignoring duplicate dependency declaration: %r",
                          context, dep)

    unique.reverse()
    return unique


# --------------------------------------------------------------------------------------------------


def ArtifactNameFromXML(node):
    return ArtifactName(
        group_id = xml_util.get_child_node_text_value(node, 'groupId'),
        artifact_id = xml_util.get_child_node_text_value(node, 'artifactId'),
    )


def ArtifactFromXML(node, default_packaging='jar'):
    """Creates an Artifact tuple from a dependency XML node.

    Args:
        node: dependency XML node to parse into an Artifact tuple.
        default_packaging: Default packaging to use if none is specified.
    Returns:
        The Artifact tuple parsed from the XML dependency node.
        The artifact version may be None if unspecified.
    """
    version = xml_util.get_child_node_text_value(node, 'version') or None
    classifier = xml_util.get_child_node_text_value(node, 'classifier') or None
    packaging = xml_util.get_child_node_text_value(node, 'type') or default_packaging
    return Artifact(
        group_id=xml_util.get_child_node_text_value(node, 'groupId'),
        artifact_id=xml_util.get_child_node_text_value(node, 'artifactId'),
        packaging=packaging,
        classifier=classifier,
        version=version,
    )


def ArtifactNameFromXML(node):
    return ArtifactName(
        group_id=xml_util.get_child_node_text_value(node, 'groupId'),
        artifact_id=xml_util.get_child_node_text_value(node, 'artifactId'),
    )


def DependencyFromXML(node, default_packaging='jar'):
    artf = ArtifactFromXML(node, default_packaging=default_packaging)
    scope = xml_util.get_child_node_text_value(node, 'scope') or None

    optional = xml_util.get_child_node_text_value(node, 'optional')
    if optional is not None and optional != '':
        optional = base.truth(optional)
    elif optional == '':
        optional = True
    else:
        optional = False

    exclusions = xml_util.get_child(node, 'exclusions')
    if exclusions is None:
        exclusions = ()
    else:
        exclusions = xml_util.list_nodes_with_tag(exclusions.childNodes, tag='exclusion')
        exclusions = map(ArtifactNameFromXML, exclusions)
        exclusions = frozenset(exclusions)

    return Dependency(
        artifact=artf,
        scope=scope,
        optional=optional,
        exclusions=exclusions,
    )


# --------------------------------------------------------------------------------------------------


class MavenWorkspace(object):
    """Collection of Maven projects.

    A workspace contains a set of Maven projects.
    The projects in the workspace may depend on each other.
    """

    def __init__(
        self,
        repo,
        projects=(),
        superficial_init=False,
    ):
        """Initializes a new workspace of Maven projects.

        Args:
            repo: Maven repository where to search for artifacts.
            projects: Collection of project paths to pre-seed the workspace with.
            superficial_init: Whether to perform a superficial initialization only.
                This is useful when we must survive broken pom definitions.
        """
        # Map: path -> MavenProject
        self._path_map = dict()

        # Map: Artifact -> MavenProject
        self._artifact_map = dict()

        # Maven repository used to retrieve unknown artifact specifications:
        self._repo = repo

        # Pre-seeds the workspace (superficial initialization of the MavenProjects):
        self._init_phase = True

        projects = tuple(map(os.path.abspath, projects))
        logging.debug("Pre-seeding Maven workspace with %d projects.", len(projects))
        for project_path in projects:
            self.GetOrCreateForPath(path=project_path)

        # From now on, new projects are deeply initialized:
        self._init_phase = False
        logging.debug("Initializing Maven workspace with %d projects.", len(self._path_map))

        # Now that we can resolve parent POMs, force deep initialization:
        if not superficial_init:
            for project_path, project in self._path_map.items():
                project._deep_init()

        logging.debug("Maven workspace initialized with %d projects.", len(projects))

    @property
    def repo(self):
        """Returns: the Maven repository where to fetch unknown artifacts."""
        return self._repo

    @base.synchronized(lock=threading.RLock())
    def GetOrCreateForPath(self, path, parent=None):
        """Reports the MavenProject corresponding to a given directory path.

        Creates and registers a new MavenProject if this path isn't known already.

        Args:
            path: Path of the MavenProject to report.
            parent: Optional explicit parent MavenProject backlink, for sub-modules.
        Returns:
            The MavenProject associated with the specified path.
        """
        path = os.path.abspath(path)  # Normalize path to ensure key unicity

        project = self._path_map.get(path)
        if project is None:
            project = MavenProject(
                workspace=self,
                path=path,
                parent=parent,
                superficial=self._init_phase,
            )
            self._path_map[project.path] = project

            artifacts = frozenset(project.list_produced_artifacts(include_modules=False))
            for artifact in artifacts:
                assert (artifact not in self._artifact_map), \
                    ('Project in %s duplicates artifact %s (created by %s)'
                     % (path, artifact, self._artifact_map[artifact].path))
                self._artifact_map[artifact] = project

        return project

    def GetForArtifact(self, artifact):
        """Reports the MavenProject that produces the specified artifact.

        Args:
            artifact: Coordinate of the artifact to look up.
        Returns:
            The MavenProject that produces the specified artifact,
            or None if no project is known to produce this artifact.
        """
        return self._artifact_map.get(artifact, None)

    def GetOrFetchForArtifact(self, artifact):
        """Reports the MavenProject for the specified artifact.

        If the artifact is not produced in the workspace, attempt to fetch
        its pom file and to create a virtual project for it.

        Args:
            artifact: Coordinate of the artifact to look up.
        Returns:
            The MavenProject for the specified artifact.
        """
        project = self._artifact_map.get(artifact, None)
        if project is None:
            # Create a virtual project if the pom file can be downloaded:
            pom_xml_path = self.repo.Get(
                group=artifact.group_id,
                artifact=artifact.artifact_id,
                version=artifact.version,
                classifier=artifact.classifier,
                type='pom',
            )
            if pom_xml_path is None:
                raise Error('Unable to retrieve pom file for artifact %s' % artifact)
            with open(pom_xml_path, mode="rt", encoding="UTF-8") as pom_file:
                pom_xml = pom_file.read()

            try:
                project = MavenProject(workspace=self, pom_xml=pom_xml)
                self._artifact_map[artifact] = project
            except Exception as err:
                logging.error("Error parsing Maven project definition for %s", artifact)
                raise err

        return project

    def ListArtifacts(self):
        """Lists all the artifact coordinates known to this workspace."""
        return self._artifact_map.keys()


# --------------------------------------------------------------------------------------------------


class MavenProfile(object):
    def __init__(self, project, node):
        """Initializes a Maven profile node.

        Args:
            project: MavenProject this profile belongs to.
            node: DOM/XML node describing the profile.
        """
        self._project = project
        self._node = node
        self._id = xml_util.get_child_node_text_value(self._node, 'id')

    @property
    def project(self):
        """Returns: the MavenProject this profile belongs to."""
        return self._project

    @property
    def node(self):
        """Returns: the XML node describing this profile."""
        return self._node

    @property
    def id(self):
        """Returns: the ID/name of this profile."""
        return self._id

    @property
    def activation(self):
        """Returns: the activation of this profile."""
        return xml_util.get_child_node_text_value(self._node, 'activation')

    @property
    def active_by_default(self):
        """Returns: whether this profile is active by default."""
        active = xml_util.get_child(self._node, 'activation', 'activeByDefault')
        if active is None:
            return False
        return base.truth(xml_util.get_node_text_value(active))

    @property
    def activation_property(self):
        """Returns: the system property to activate this profile, if any, or None."""
        prop = xml_util.get_child(self._node, 'activation', 'property', 'name')
        if prop is not None:
            prop = xml_util.get_node_text_value(prop)
        return prop

    @property
    def properties(self):
        return xml_util.get_child(self._node, 'properties')

    @property
    def dependencies(self):
        return xml_util.get_child(self._node, 'dependencies')


# --------------------------------------------------------------------------------------------------


class MavenProject(object):
    """Wraps a Maven project.

    A Maven project is characterized by a top-level Maven pom.file and
    potential sub-modules.
    In addition to the top-level project, each sub-module is wrapped by
    a MavenProject instance.

    A Maven project:
     - depends on other artifacts;
     - produces artifacts (including its sub-modules).
    """

    def __init__(
        self,
        workspace,
        path=None,
        pom_xml=None,
        parent=None,
        superficial=False,
    ):
        """Initializes a wrapper for a Maven project.

        A project may be concrete or virtual:
         - virtual means we only have the pom file (no actual source).
         - concrete means we have the actual project source.
        Only one of arguments 'path' and 'pom_xml' may be specified.

        Args:
            workspace: MavenWorkspace this project belongs to.
            path: Root path of the Maven project, if concrete.
            pom_xml: content of the pom.xml file, for virtual projects only.
            parent: Optional explicit parent MavenProject backlink, for sub-modules.
            superficial: When true, perform a superficial initialization.
                This is necessary when seeding the workspace to avoid circular dependencies.
        """
        self._workspace = workspace
        assert (self._workspace is not None)

        assert ((path is None) or (pom_xml is None))
        self._virtual = (path is None)

        self._path = path
        self._pom_xml = pom_xml

        self._parent = parent

        if not self.virtual:
            assert os.path.exists(self.pom_file_path), \
                ('Project definition file does not exist: %r' % self.pom_file_path)
            logging.debug('Reading pom file: %s', self.pom_file_path)
            with open(self.pom_file_path, mode="rt", encoding="UTF-8") as f:
                self._pom_xml = f.read()

        try:
            self._dom = xml_util.parse_from_string(text=self._pom_xml)
        except Exception as err:
            # FIXME: This log is currently not helpful
            logging.error("Error parsing Maven project definition in path=%r (parent=%r)\n%s",
                          self._path, self._parent, self._pom_xml)
            raise err

        # ------------------------------------------------------------------------------------------
        # Properties defined in the pom file:
        self._properties = dict(self._list_properties())

        self._properties["groupId"] = self.group_id
        self._properties["artifactId"] = self.artifact_id
        self._properties["version"] = self.version

        self._properties["pom.groupId"] = self.group_id
        self._properties["pom.artifactId"] = self.artifact_id
        self._properties["pom.version"] = self.version

        # ------------------------------------------------------------------------------------------
        # Maven repositories
        self._repositories = frozenset(self._list_repositories())
        if len(self._repositories) > 0:
            logging.debug('Maven project %s references the following repositories: %s',
                          self.artifact, ' '.join(sorted(self._repositories)))

        for repo in self.repositories:
            self._workspace.repo.add_remote(repo)

        # ------------------------------------------------------------------------------------------
        # Build and register this project's modules:
        modules = []
        module_map = {}
        if not self.virtual:
            logging.debug("Maven project %s includes the following modules: %r",
                          self.artifact, list(self.module_names))
            for module_name in self.module_names:
                module_path = os.path.join(self.path, module_name)
                module = self.workspace.GetOrCreateForPath(path=module_path, parent=self)
                modules.append(module)
                module_map[module_name] = module

        self._module_map = base.ImmutableDict(module_map)
        self._modules = tuple(modules)

        if not superficial:
            self._deep_init()

    def _deep_init(self):
        """Second part of the constructor.

        This constructor is allowed to rely on transitive dependencies (eg. parent POM resolution).
        """
        # Force resolution of parent POM:
        try:
            self.parent
        except maven_repo.ArtifactNotFoundError as err:
            logging.error('Error resolving parent POM for project %s (in %r):\n%r',
                          self.artifact, self.path, err)
            raise err

        # Map: ArtifactKey -> managed Dependency
        self._managed_deps = base.ImmutableDict(self._evaluate_managed_dependencies())
        if logging.getLogger().isEnabledFor(LOG_LEVEL.DEBUG_VERBOSE):
            logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Managed dependencies for %s", self.artifact)
            for key, dep in sorted(self._managed_deps.items()):
                logging.log(LOG_LEVEL.DEBUG_VERBOSE, "\t%s -> %s", key, dep)

    def _list_repositories(self):
        """Lists the Maven repositories declared in the project."""
        reps = xml_util.get_child(self._dom, 'project', 'repositories')
        if reps is None:
            return
        for rep in xml_util.list_nodes_with_tag(reps.childNodes, 'repository'):
            url = xml_util.get_child_node_text_value(rep, 'url')
            url = self.resolve_properties(url)
            yield url

    def _evaluate_managed_dependencies(self):
        # Map: artifact key (group, artifact, type, classifier) -> Dependency
        managed_deps = dict()

        for managed_dep in self._list_expanded_managed_deps():
            akey = managed_dep.key
            existing = managed_deps.get(akey)
            if (existing is not None) and (existing != managed_dep):
                logging.log(
                    LOG_LEVEL.DEBUG_VERBOSE,
                    "From %s : managed dependency %s overrides %s",
                    self.artifact, managed_dep, existing)
            elif existing is not None:
                # Redeclare equivalent managed dependency
                pass

            managed_deps[akey] = managed_dep

        return managed_deps

    def _list_expanded_managed_deps(self):
        """Lists the managed dependencies after expansion of imported dependencies.

        Yields:
            Managed dependencies.
            Import scoped dependencies are expanded.
        """
        for dep in self.list_managed_dependencies_recursive():
            if dep.scope == "import":
                logging.debug("Expanding imported managed dependencies %s", dep)
                dep_import = self.workspace.GetOrFetchForArtifact(dep.artifact)
                yield from dep_import.list_managed_dependencies_recursive()
            else:
                yield dep

    @property
    def managed_deps(self):
        """Returns: the map: artifact key -> managed Dependency."""
        return self._managed_deps

    @property
    def repositories(self):
        """Returns: the set of Maven repositories (URLs) referenced in this project."""
        # Does not include repositories listed in nested modules.
        return self._repositories

    @property
    def virtual(self):
        """Returns: whether this project is virtual."""
        return self._virtual

    @property
    def path(self):
        """Returns: this Maven project's root path."""
        return self._path

    @property
    def workspace(self):
        """Returns: the MavenWorkspace this project belongs to."""
        return self._workspace

    @property
    def group_id(self):
        """Returns: the group ID of the artifact produced by this project."""
        group_node = xml_util.get_child(self._dom, 'project', 'groupId')
        if group_node is not None:
            return xml_util.get_node_text_value(group_node)

        group_node = xml_util.get_child(self._dom, 'project', 'parent', 'groupId')
        if group_node is not None:
            return xml_util.get_node_text_value(group_node)

        raise Error('No group ID for project %r', self.path)

    @property
    def artifact_id(self):
        """Returns: the artifact ID of the artifact produced by this project."""
        aid = xml_util.get_child(self._dom, 'project', 'artifactId')
        return xml_util.get_node_text_value(aid)

    @property
    def version(self):
        """Returns: the version ID of the artifact produced by this project."""
        version_node = xml_util.get_child(self._dom, 'project', 'version')
        if version_node is not None:
            return xml_util.get_node_text_value(version_node)

        return xml_util.get_node_text_value(
            xml_util.get_child(self._dom, 'project', 'parent', 'version'))

    @property
    def packaging(self):
        """Returns: the packaging of the artifact produced by this project."""
        packaging_node = xml_util.get_child(self._dom, 'project', 'packaging')
        if packaging_node is not None:
            return xml_util.get_node_text_value(packaging_node)
        return None

    @property
    def module_names(self):
        """Returns: the names of this project's modules."""
        module_names = []
        modules_node = xml_util.get_child(self._dom, 'project', 'modules')
        if modules_node is not None:
            for module_node in \
                xml_util.list_nodes_with_tag(nodes=modules_node.childNodes, tag='module'):
                module_names.append(xml_util.get_node_text_value(module_node))
        return module_names

    @property
    def artifact_name(self):
        """Returns: the name of the artifact produced by this project."""
        return ArtifactName(group_id=self.group_id, artifact_id=self.artifact_id)

    @property
    def artifact(self):
        """Returns: the coordinate of the artifact produced by this project."""
        return Artifact(
            group_id=self.group_id,
            artifact_id=self.artifact_id,
            version=self.version,
            packaging=self.packaging,
        )

    @property
    def pom_artifact(self):
        """Returns: the coordinate of the artifact this project's POM file."""
        return Artifact(
            group_id=self.group_id,
            artifact_id=self.artifact_id,
            version=self.version,
            packaging='pom',
        )

    @property
    def parent_artifact_name(self):
        """Returns: the name of the parent artifact for this project, if any."""
        parent_node = xml_util.get_child(self._dom, 'project', 'parent')
        if parent_node is None: return None
        return ArtifactNameFromXML(parent_node)

    @property
    def parent_artifact(self):
        """Returns: the coordinate of the parent artifact for this project, if any."""
        parent_node = xml_util.get_child(self._dom, 'project', 'parent')
        if parent_node is None: return None
        return ArtifactFromXML(parent_node, default_packaging='pom')

    @property
    def parent(self):
        """Returns: this project's parent project, if any, or None."""
        if self._parent is None:
            parent_artifact = self.parent_artifact
            if parent_artifact is None: return None
            self._parent = self.workspace.GetOrFetchForArtifact(artifact=parent_artifact)
        return self._parent

    @property
    def pom_file_path(self):
        """Returns: the path of this project's POM file."""
        return os.path.join(self.path, 'pom.xml')

    @property
    def modules(self):
        """Returns: this project's sub-modules, in order."""
        return self._modules

    @property
    def module_map(self):
        """Returns: this project's sub-modules map, keyed by module name."""
        return self._module_map

    @property
    def all_modules(self):
        """Returns: all modules included in this projects, including itself."""
        yield self
        for module in self.modules:
            yield from module.all_modules

    @property
    def unresolved_deps(self):
        for comment in self._list_comment_nodes():
            text = comment.data.strip()
            if text.startswith("dep:"):
                yield text[4:].strip()

    @property
    def kiji_build_exclusions(self):
        for comment in self._list_comment_nodes():
            text = comment.data.strip()
            if text.startswith("exclusion:"):
                yield text[10:].strip()

    def _list_comment_nodes(self):
        """Lists the top-level XML comment nodes."""
        project = xml_util.get_child(self._dom, "project")
        return xml_util.list_nodes_with_type(project.childNodes, minidom.Element.COMMENT_NODE)

    # ----------------------------------------------------------------------------------------------

    def __str__(self):
        if self.virtual:
            return 'MavenProject(%s)' % (self.artifact,)
        else:
            return 'MavenProject(%s, path=%s)' % (self.artifact, self.path)

    def __repr__(self):
        return str(self)

    # ----------------------------------------------------------------------------------------------

    def WritePomFile(self):
        """Writes the (potentially updated) POM file of this project back."""
        assert (not self.virtual)
        with open(self.pom_file_path, mode="wb") as f:
            xml = self._dom.toxml(encoding="UTF-8")
            encoding_prefix = b'<?xml version="1.0" encoding="UTF-8"?>'
            if (xml.startswith(encoding_prefix)
                and (xml[len(encoding_prefix)] != b'\n')):
                # Insert missing new line after encoding prefix:
                xml = xml[:len(encoding_prefix)] + b'\n' + xml[len(encoding_prefix):]
            f.write(xml)
            f.write(b'\n')

    def SetVersion(self, version):
        """Updates the version ID of the artifact produced by this project.

        Recursively updates the sub-modules parent dependency to match.

        Args:
            version: New version ID of the artifact this project produces.
        Returns:
            Whether the version ID has been updated or not.
        """
        version_node = xml_util.get_child(self._dom, 'project', 'version')
        if version_node is not None:
            xml_util.set_node_text_value(node=version_node, text=version)
            self._UpdateModulesParentLink()
            return True
        return False

    def SetParentVersion(self, version):
        """Updates the version ID of this project's parent dependency.

        Args:
            version: New version ID of this project's parent dependency.
        Returns:
            Whether the parent dependency has been updated or not.
        """
        version_node = xml_util.get_child(self._dom, 'project', 'parent', 'version')
        if version_node is not None:
            xml_util.set_node_text_value(node=version_node, text=version)
            return True
        return False

    def _UpdateModulesParentLink(self):
        """Updates this project's sub-modules parent's dependency."""
        for module in self.modules:
            module.SetParentVersion(self.version)

    # ----------------------------------------------------------------------------------------------
    # Build profiles

    def ListProfiles(self):
        """Lists the build profiles for this project."""
        profiles = xml_util.get_child(self._dom, 'project', 'profiles')
        if profiles is None:
            return
        for profile_node in xml_util.list_nodes_with_tag(profiles.childNodes, tag='profile'):
            yield MavenProfile(project=self, node=profile_node)

    def ListActiveProfiles(self):
        """Lists the build profiles expected on by default."""
        for profile in self.ListProfiles():
            if profile.active_by_default:
                logging.log(
                    LOG_LEVEL.DEBUG_VERBOSE,
                    'Assuming %s profile %r is enabled.',
                    self.artifact, profile.id)
                yield profile
            else:
                prop = profile.activation_property
                if prop is not None and prop.startswith('!'):
                    logging.log(
                        LOG_LEVEL.DEBUG_VERBOSE,
                        'Assuming %s profile %r is enabled.',
                        self.artifact, profile.id)
                    yield profile
                else:
                    logging.log(
                        LOG_LEVEL.DEBUG_VERBOSE,
                        'Assuming %s profile %r is not enabled.',
                        self.artifact, profile.id)

    # ----------------------------------------------------------------------------------------------
    # Dependencies

    def list_project_modules_dependencies(self):
        """Lists the artifacts this project and its modules depend on.

        Yields:
            Artifact this projects and its modules depend on.
        """
        if self.parent_artifact is not None:
            yield self.parent_artifact
        for dep in self.list_direct_dependencies(managed=False):
            yield dep.artifact
        for module in self.modules:
            for artf in module.list_project_modules_dependencies():
                yield artf

    def list_managed_dependencies_recursive(self):
        """Lists the managed dependencies recursively up to the root parent project.

        Yields:
            Managed dependencies declared in this project.
        """
        if self.parent is not None:
            yield from self.parent.list_managed_dependencies_recursive()
        yield from self.list_direct_dependencies(managed=True)

    def dep_set(self, deps):
        return dep_set(deps, self.pom_artifact)

    def _list_all_immediate_dependencies(self, scopes=(), include_optional=True):
        """Lists all the immediate dependencies, parent first."""
        if self.parent is not None:
            yield from self.parent._list_all_immediate_dependencies(
                scopes=scopes,
                include_optional=include_optional,
            )
        yield from self.list_direct_dependencies(
            managed=False,
            scopes=scopes,
            include_optional=include_optional,
        )

    def list_all_immediate_dependencies(
        self,
        scopes=(),
        include_optional=True,
        dep_chain=(),
        force_unresolved=False,
    ):
        """Lists this project's immediate dependencies.

        Includes dependencies declared in parent POMs.

        Args:
            scopes: Scopes to include. Empty means all.
            include_optional: Whether to include or exclude optional dependencies.
            dep_chain: Dependency chain leading to this Maven project.
                Chain is ordered from the deepest dependency (this) to the root.
            force_unresolved: Force the use of unresolved (kiji-build) dependencies.
                For kiji-build generated artifacts, this lists direct dependencies
                instead of all transitive dependencies.
        Returns:
            Ordered collection of Dependency instances (unique by artifact key).
        """
        if force_unresolved:
            unresolved_deps = list(self.unresolved_deps)
            exclusions = frozenset(map(artifact.parse_artifact, self.kiji_build_exclusions))
            if len(unresolved_deps) > 0:
                for dep in unresolved_deps:
                    yield Dependency(
                        artifact=artifact.parse_artifact(dep),
                        scope=SCOPE.COMPILE,
                        optional=False,
                        exclusions=exclusions,
                        source=self.pom_artifact,
                    )
                return

        deps = list()
        if self.parent_artifact is not None:
            deps.append(Dependency(artifact=self.parent_artifact))
        deps.extend(
            self._list_all_immediate_dependencies(scopes=scopes, include_optional=include_optional))
        deps = self.dep_set(deps)

        # Compute managed dependencies overrides:
        managed_deps = dict()
        for project_artf in dep_chain[1:]:
            project = self.workspace.GetForArtifact(project_artf)
            managed_deps.update(project.managed_deps)

        # Apply managed dependencies overrides:
        for dep in deps:
            managed = managed_deps.get(dep.key)
            if managed is not None:
                # Override merge (managed takes precedence over dep):
                merged = Dependency(
                    artifact = managed.artifact,
                    scope = managed.scope or dep.scope,
                    optional = managed.optional if (managed.optional is not None) else dep.optional,
                    exclusions = managed.exclusions if (managed.exclusions is not None) else dep.exclusions,
                    source = "%s overridden by %s" % (dep.source, managed.source),
                )
                logging.log(
                    LOG_LEVEL.DEBUG_VERBOSE,
                    'From %s : dependency %s overridden by managed dependency %s into %s',
                    self.artifact, dep, managed, merged, self)
            yield dep

    def list_direct_dependencies(
        self,
        managed=False,
        scopes=(),
        include_optional=True,
    ):
        """Lists this project's direct dependencies.

        Args:
            managed: Whether to report managed or concrete dependency definitions.
            scopes: Set of scopes to include. Empty means all.
            include_optional: Whether to include optional dependencies.
        Returns:
            Ordered collection of Dependency instances (unique by artifact key).
        """
        deps = list()
        for deps_node in self._list_direct_dep_nodes(managed=managed):
            deps.extend(self.dep_set(self._list_direct_deps(
                deps_node,
                managed=managed,
                scopes=scopes,
                include_optional=include_optional,
            )))
        return self.dep_set(deps)

    def _list_direct_deps(
        self,
        deps_node,
        managed,
        scopes=(),
        include_optional=True,
    ):
        """Lists the direct dependencies declared in this Maven project.

        Args:
            deps_node: Root XML node to list dependencies from.
            managed: Whether to list managed or concrete dependencies.
            scopes: Dependency scopes to include. Empty means all.
            include_optional: Whether to include or exclude optional dependencies.
        Yields:
            Dependency instances.
        """
        for dep in xml_util.list_nodes_with_tag(deps_node.childNodes, 'dependency'):
            dep = self._make_dependency_from_node(dep)
            if not managed:
                # Apply managed dependencies backfill:
                dep = self.resolve_dependency(dep)

            # Force evaluation of the Dependency:
            dep = dep.eval()

            if dep.optional and not include_optional:
                continue

            # Filter dependency by scopes, if specified:
            if (len(scopes) > 0) and (dep.scope not in scopes):
                continue

            yield dep

    def _list_direct_dep_nodes(self, managed=False):
        """Lists the XML nodes describing a collection of dependencies."""
        if managed:
            path = ['dependencyManagement', 'dependencies']
        else:
            path = ['dependencies']

        base_nodes = itertools.chain(
            [xml_util.get_child(self._dom, 'project')],
            map(lambda profile: profile.node, self.ListActiveProfiles()),
        )

        for base_node in base_nodes:
            deps_node = xml_util.get_child(base_node, *path)
            if deps_node is not None:
                yield deps_node

    def _make_dependency_from_node(self, node, default_packaging='jar'):
        """Makes a Dependency out of an XML node.

        Args:
            node: XML node with some artifact dependency description.
        Returns:
            Dependency instance parsed from the XML node.
        """
        dep = DependencyFromXML(node, default_packaging=default_packaging)
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, 'Resolving raw dependency %s', dep)

        artifact = dep.artifact

        group_id = artifact.group_id
        if group_id is None:
            logging.warn("Unspecified group ID in dependency %r", dep)
            group_id = "org.apache.maven.plugins"
        else:
            group_id = self.resolve_properties(group_id)

        artifact_id = artifact.artifact_id
        if artifact_id is None:
            logging.warn("Unspecified artifact ID in dependency %r", dep)
        else:
            artifact_id = self.resolve_properties(artifact_id)

        version = artifact.version
        if version is not None:
            version = self.resolve_properties(version)
        if not version:
            version = None

        classifier = artifact.classifier
        if classifier is not None:
            classifier = self.resolve_properties(classifier)
        if not classifier:
            classifier = None

        resolved = artifact.update(
            group_id=group_id,
            artifact_id=artifact_id,
            version=version,
            classifier=classifier,
        )

        dep = dep.update(
            artifact=resolved,
            source=self.pom_artifact,
        )
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, 'Resolved dependency %s', dep)
        return dep

    # ----------------------------------------------------------------------------------------------
    # Build plugins

    def list_plugins_recursive(self):
        """Lists the plugins this project and its modules depend on.

        Yields:
            Plugins this projects and its modules depend on.
        """
        for dep in self.list_plugins(managed=False):
            yield dep.artifact
        for module in self.modules:
            yield from module.list_plugins_recursive()

    def list_plugins(self, managed=False):
        for plugin_node in self._ListBuildPluginNodes(managed=managed):
            plugin = self._make_dependency_from_node(plugin_node, default_packaging='maven-plugin')
            if managed:
                # Lists the managed dependencies (no resolution needed?)
                pass
            else:
                # Lists concrete plugin dependencies (resolution applies)
                plugin = self.ResolvePlugin(plugin)
            yield plugin

    def _ListBuildPluginNodes(self, managed=False):
        """Lists the build plugins declared in the project.

        Returns:
            Collection of plugin DOM nodes.
        """
        if managed:
            path = ['project', 'build', 'pluginManagement', 'plugins']
        else:
            path = ['project', 'build', 'plugins']

        plugins_node = xml_util.get_child(self._dom, *path)
        if plugins_node is None:
            return ()
        return xml_util.list_nodes_with_tag(plugins_node.childNodes, tag='plugin')

    def _GetBuildPluginNode(self, plugin_name):
        """Reports the plugin node with the given artifact name.

        Args:
            plugin_name: Artiface name of the plugin to report.
        Returns:
            Node for the specified plugin, or None if no such plugin exists.
        """
        for plugin_node in self._ListBuildPluginNodes():
            name = ArtifactNameFromXML(plugin_node)
            if plugin_name == name:
                return plugin_node
        return None

    # ----------------------------------------------------------------------------------------------

    def UpdateDependency(self, artifact, managed=False, **kwargs):
        """Updates the version ID of the specified project's dependency.

        Args:
            artifact: Existing dependency to update.
            managed: Whether to update a concrete or a managed dependency.
            **kwargs: Parameters to update in the existing dependency.
        """
        deps_node = xml_util.get_child(self._dom, 'project')
        if managed:
            deps_node = xml_util.get_child(deps_node, 'dependencyManagement')
        deps_node = xml_util.get_child(deps_node, 'dependencies')
        assert (deps_node is not None)

        for dep_node in xml_util.list_nodes_with_tag(deps_node.childNodes, 'dependency'):
            dep = self._make_dependency_from_node(dep_node)
            if dep.artifact != artifact:
                continue

            if 'version' in kwargs:
                version = kwargs['version']
                # Is the version controlled by a property?
                version_node = xml_util.get_child(dep_node, 'version')
                if version_node is not None:
                    version_text = xml_util.get_node_text_value(version_node).strip()
                    re_prop = re.compile(r'^\${(.*?)}$')
                    prop_match = re_prop.match(version_text)
                    if prop_match is not None:
                        prop_name = prop_match.group(1)
                        logging.info(
                            'Version for dependency %s is controlled by property %s in %s/pom.xml',
                            artifact, prop_name, self.path)
                        if prop_name != 'project.version':
                            self.UpdateProperty(prop_name, version)

            for key, value in kwargs.items():
                node = xml_util.get_child(dep_node, key)
                if node is None:
                    node = minidom.Element(key)
                    dep_node.appendChild(node)
                xml_util.set_node_text_value(node, value)

    def list_produced_artifacts(self, include_modules=True):
        """Lists the artifacts produced by this project and its modules.

        Args:
            include_modules: Whether to include artifacts produced by modules.

        Yields:
            Artifact produced by this projects and its modules.
        """
        artf = self.artifact
        if artf.packaging != "pom":
            yield artf.update(packaging="pom")
        yield artf
        if artf.packaging == "jar":
            # FIXME: It is not clear which packaging/type and classifier combinations should be used
            yield artf.update(packaging="test-jar")
            yield artf.update(packaging="test-jar", classifier="tests")

        # Parse build-helper plugin to determine other produced artifacts:
        build_helper_plugin = self._GetBuildPluginNode(
            ArtifactName(group_id="org.codehaus.mojo", artifact_id="build-helper-maven-plugin"))
        if build_helper_plugin is not None:
            executions = xml_util.get_child(build_helper_plugin, "executions")
            if executions is not None:
                for execution in executions.childNodes:
                    configuration = xml_util.get_child(execution, "configuration")
                    if configuration is not None:
                        artifacts = xml_util.get_child(configuration, "artifacts")
                        if artifacts is not None:
                            for artifact in \
                                xml_util.list_nodes_with_tag(artifacts.childNodes, "artifact"):

                                # Build the artifact from the project"s artifact:
                                produced = artf

                                # Apply overrides:
                                #  - None means no override
                                #  - "" means explicit override to an empty string
                                group_id = xml_util.get_child_node_text_value(artifact, "groupId")
                                artifact_id = xml_util.get_child_node_text_value(artifact, "artifactId")
                                version = xml_util.get_child_node_text_value(artifact, "version")
                                classifier = xml_util.get_child_node_text_value(artifact, "classifier")
                                packaging = xml_util.get_child_node_text_value(artifact, "type")

                                if group_id is None: group_id = DEFAULT
                                if artifact_id is None: artifact_id = DEFAULT
                                if version is None: version = DEFAULT
                                if classifier is None: classifier = DEFAULT
                                if packaging is None: packaging = DEFAULT

                                resolved = produced.update(
                                    group_id=group_id,
                                    artifact_id=artifact_id,
                                    version=version,
                                    packaging=packaging,
                                    classifier=classifier,
                                )
                                logging.debug("Resolved produced artifact: %s into %s",
                                              produced, resolved)
                                yield resolved

        # Parse maven assembly plugin to determine even more produced artifacts:
        assembly_plugin = self._GetBuildPluginNode(
            ArtifactName(group_id="org.apache.maven.plugins", artifact_id="maven-assembly-plugin"))
        if assembly_plugin is not None:
            # Build the artifact from the project's artifact:
            # Assume tar.gz release artifact, cause I don't want to parse the XML.
            produced = self.artifact.update(
                packaging="tar.gz",
                classifier="release",
            )
            yield produced

        # Include artifacts produced in sub-modules:
        if include_modules:
            for module in self.modules:
                for artifact in module.list_produced_artifacts(include_modules=True):
                    yield artifact

    def _list_properties(self):
        """Lists the properties defined in the pom file.

        Yields:
            Properties as (key, value) pairs.
        """
        props_nodes = itertools.chain(
            [xml_util.get_child(self._dom, 'project', 'properties')],
            map(lambda p: p.properties, self.ListActiveProfiles()),
        )

        for props in props_nodes:
            if props is None: return
            for child in props.childNodes:
                if child.nodeType != minidom.Element.ELEMENT_NODE: continue
                yield (child.tagName, xml_util.get_node_text_value(child))

    def SetProperty(self, name, value):
        """Sets or update a property of this project.

        Args:
            name: Name of the property to set or update.
            value: Text value of the property.
        """
        project = xml_util.get_child(self._dom, 'project')
        assert (project is not None)
        properties = xml_util.get_child(project, 'properties')
        if properties is None:
            properties = minidom.Element('properties')
            project.appendChild(properties)
        prop = xml_util.get_child(properties, name)
        if prop is None:
            prop = minidom.Element(name)
            properties.appendChild(properties)
        xml_util.set_node_text_value(prop, value)

        # Overwrite the property in the local map:
        self._properties[name] = value

    def UpdateProperty(self, name, value):
        """Update a property's definition.

        Locates the actual definition of the property, recursing through parent
        pom files.

        Args:
            name: Name of the property to update.
            value: Text value of the property.
        """
        if name in self.properties:
            self.SetProperty(name, value)
        elif self.parent is not None:
            self.parent.UpdateProperty(name, value)
        else:
            raise Error('Cannot find project where property %r is defined.' % name)

    @property
    def properties(self):
        """Returns: the map of properties defined in the pom file."""
        return self._properties

    def get_property(self, name):
        """Reports the value associated to a property name.

        Performs resolution through parent projects,
        and expansion of recursive properties if necessary.

        Args:
            name: Name of the property to look-up.
        Returns:
            The fully resolved property value, or None if the property does not exist.
        """
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, 'Looking up property %s in %s', name, self)

        value = self.properties.get(name)

        if (value is None) and name.startswith("project."):
            return self.get_property(name[len("project."):])

        if (value is None) and (self.parent is not None) and name.startswith("parent."):
            value = self.parent.get_property(name[len("parent."):])

        if (value is None) and (self.parent is not None):
            value = self.parent.get_property(name)

        if value is not None:
            value = self.resolve_properties(value)
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, 'Resolved property %r to %r', name, value)

        return value

    # Regexp for a property: ${name}
    _RE_PROP = re.compile(r'\${(.*?)}')

    def resolve_properties(self, text, strict=False):
        """Resolves properties used in a text input.

        Args:
            text: Input text potentially using properties '${property.name}'.
            strict: When strict, fail on missing properties (raise Error).
                Otherwise, ignore missing properties and keep their ${name} form.
        Returns:
            The resolved text.
        """
        def _resolve(match):
            property_name = match.group(1)
            resolved = self.get_property(property_name)
            if resolved is None:
                if strict:
                    raise Error("Unable to expand missing property {!r} in {!r}"
                                .format(property_name, text))
                else:
                    logging.warning("In %s : unknown property: %r",
                                    self.pom_artifact, property_name)
                    fallback_value = "<missing property {!r}>".format(property_name)
                    # Save the fallback, to avoid repeating the warning several times:
                    self._properties[property_name] = fallback_value
                    return fallback_value
            else:
                return resolved

        try:
            resolved = self._RE_PROP.sub(_resolve, text)
        except TypeError as err:
            logging.error('text = %r', text)
            raise err
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, 'Resolving properties: %r -> %r', text, resolved)
        return resolved

    GetProperty = base.deprecated(get_property)
    ResolveProperties = base.deprecated(resolve_properties)

    def resolve_dependency(self, dep):
        """Resolves a direct/immediate dependency using dependency management.

        Args:
            dep: Dependency to resolve.
        Returns:
            The resolved dependency.
        """
        logging.log(
            LOG_LEVEL.DEBUG_VERBOSE,
            'From %s : applying managed dependencies %s with key = %s',
            self.artifact, dep, dep.key)

        managed = self.managed_deps.get(dep.key)
        if managed is None:
            logging.log(
                LOG_LEVEL.DEBUG_VERBOSE,
                "From %s : no managed dependency for %s",
                self.artifact, dep)
        else:
            # Backfill merge (dep takes precedence over managed):

            # (group,artifact,classifier,type) are the key and do not change.
            # The only Artifact field that is managed is 'version'.
            if dep.artifact.version is not None:
                merged_artifact = dep.artifact
            else:
                merged_artifact = managed.artifact

            merged = Dependency(
                artifact = merged_artifact,
                scope = dep.scope or managed.scope,
                optional = managed.optional if dep.optional is None else dep.optional,
                # FIXME : should exclusions be unioned?
                exclusions = managed.exclusions if dep.exclusions is None else dep.exclusions,
                source = "%s backfilled with %s" % (dep.source, managed.source),
            )
            logging.log(
                LOG_LEVEL.DEBUG_VERBOSE,
                "From %s : backfilled dependency %s with managed dependency %s into %s",
                self.artifact, dep, managed, merged)
            dep = merged

        if dep.artifact.version is None:
            logging.error("Error resolving dependency %s from %s", dep, self.artifact)
            for key, managed_dep in sorted(self.managed_deps.items(), key=lambda k: ":".join(map(str, k))):
                logging.error("Managed dependency %s -> %s", key, managed_dep)
            raise DependencyResolutionError(
                "From {!s} : unable to resolve dependency {!s}".format(self.artifact, dep))

        return dep

    def ResolvePlugin(self, plugin):
        """Resolves a plugin using plugin management.

        Args:
            plugin: Plugin to resolve.
        Returns:
            The resolved plugin.
        """
        if plugin.artifact.version is not None:
            # Nothing to resolve? Or should we still attempt to resolve
            # other properties, such as exclusions?
            return plugin

        logging.debug('Resolving plugin %s in %s', plugin, self)
        # Look for managed plugin in the project pom file:
        for managed in self.list_plugins(managed=True):
            if managed.artifact.name == plugin.artifact.name:
                if managed.artifact.version is None:
                    logging.debug('Ignoring managed plugin with no version: %r', managed)
                    continue

                merged = Dependency(artifact = managed.artifact)
                logging.debug('Resolved plugin %s as %s in %s\n', plugin, merged, self)
                return merged

        if self.parent is not None:
            try:
                return self.parent.ResolvePlugin(plugin=plugin)
            except PluginResolutionError as err:
                logging.error('Error resolving plugin: %s', err)
                # Pass and raise error on the call-site project.

        raise PluginResolutionError('Cannot resolve plugin %s in project %s' % (plugin, self))


# --------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    raise Error('%r cannot be used as a standalone script.' % args[0])
