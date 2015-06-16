#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Helpers to work with Maven artifact specifications."""

import collections
import logging

from base import base

DEFAULT = base.DEFAULT

# ------------------------------------------------------------------------------


_ArtifactName = collections.namedtuple(
    typename='ArtifactName',
    field_names=(
        'group_id',
        'artifact_id',
    ),
)

class ArtifactName(_ArtifactName):
    """Artifact name, without version."""

    def __str__(self):
        return ':'.join(map(str, self))


# ------------------------------------------------------------------------------


DEFAULT_PACKAGING = "jar"


class ArtifactKey(object):
    """Artifact key (immutable part of a Maven dependency)."""

    def __init__(
        self,
        group_id,
        artifact_id,
        packaging=DEFAULT_PACKAGING,
        classifier=None,
    ):
        """Initializes a new ArtifactKey instance."""
        if packaging is None:
            packaging = DEFAULT_PACKAGING
        if (classifier is not None) and (len(classifier) == 0):
            classifier = None
        self._group_id = group_id
        self._artifact_id = artifact_id
        self._classifier = classifier
        self._packaging = packaging

        # Compute the string representation ahead:
        components = [group_id, artifact_id]
        if (packaging != DEFAULT_PACKAGING) or (classifier is not None):
            components.append(packaging)
        if classifier is not None:
            components.append(classifier)
        self._str = ':'.join(map(str, components))

    def __eq__(self, other):
        if not isinstance(other, ArtifactKey):
            return False
        return self._str == other._str

    def __ne__(self, other):
        if not isinstance(other, ArtifactKey):
            return True
        return self._str != other._str

    def __lt__(self, other):
        return self._str < other._str

    def __hash__(self):
        return hash(self._str)

    def __str__(self):
        return self._str

    def __repr__(self):
        return (
            "ArtifactKey("
            "group={group}, "
            "artifact={artifact}, "
            "classifier={classifier}, "
            "packaging={packaging})"
        ).format(
            group=self._group_id,
            artifact=self._artifact_id,
            classifier=self._classifier,
            packaging=self._packaging,
        )

    @property
    def group_id(self):
        return self._group_id

    @property
    def artifact_id(self):
        return self._artifact_id

    @property
    def packaging(self):
        return self._packaging

    @property
    def classifier(self):
        return self._classifier

    def update(
        self,
        group_id=DEFAULT,
        artifact_id=DEFAULT,
        packaging=DEFAULT,
        classifier=DEFAULT,
    ):
        """Creates a new artifact with the specified fields modified.

        Args:
            group_id: Optional group_id override.
            artifact_id: Optional artifact_id override.
            packaging: Optional packaging override.
            classifier: Optional classifier override.
        Returns:
            A new artifact key with the specified overridden fields.
        """
        return ArtifactKey(
            group_id=get_or_default(group_id, self._group_id),
            artifact_id=get_or_default(artifact_id, self._artifact_id),
            packaging=get_or_default(packaging, self._packaging),
            classifier=get_or_default(classifier, self._classifier),
        )


# ------------------------------------------------------------------------------


class Artifact(object):
    """Full artifact coordinate.

    Essentially, an Artifact key with a version.
    This is a subset of a Maven dependency.
    """

    def __init__(
        self,
        group_id,
        artifact_id,
        version,
        packaging=DEFAULT_PACKAGING,
        classifier=None,
    ):
        # Temporary hack to work around version ranges
        if (version is not None) and version.startswith("[") and version.endswith("]"):
            logging.warning("Discarding version range: %r", version)
            version = version[1:-1]

        if packaging is None:
            packaging = DEFAULT_PACKAGING
        if (classifier is not None) and (len(classifier) == 0):
            classifier = None
        self._group_id = group_id
        self._artifact_id = artifact_id
        self._version = version
        self._classifier = classifier
        self._packaging = packaging

        # Compute the string representation ahead:
        components = [group_id, artifact_id]
        if (packaging != DEFAULT_PACKAGING) or (classifier is not None):
            components.append(packaging)
        if classifier is not None:
            components.append(classifier)
        components.append(version)
        self._str = ":".join(map(str, components))

    def __eq__(self, other):
        if not isinstance(other, Artifact):
            return False
        return self._str == other._str

    def __ne__(self, other):
        if not isinstance(other, Artifact):
            return True
        return self._str != other._str

    def __lt__(self, other):
        return self._str < other._str

    def __hash__(self):
        return hash(self._str)

    def __str__(self):
        return self._str

    def __repr__(self):
        return (
            "Artifact("
            "group={group}, "
            "artifact={artifact}, "
            "packaging={packaging}, "
            "classifier={classifier}, "
            "version={version})"
        ).format(
            group=self._group_id,
            artifact=self._artifact_id,
            packaging=self._packaging,
            classifier=self._classifier,
            version=self._version,
        )

    @property
    def group_id(self):
        return self._group_id

    @property
    def artifact_id(self):
        return self._artifact_id

    @property
    def version(self):
        return self._version

    @property
    def packaging(self):
        return self._packaging

    @property
    def classifier(self):
        return self._classifier

    @property
    def name(self):
        return ArtifactName(group_id=self.group_id, artifact_id=self.artifact_id)

    @property
    def is_snapshot(self):
        assert (self.version is not None), ("Artifact has no version: %r" % self)
        return self.version.endswith("-SNAPSHOT")

    @property
    def key(self):
        """Returns: the key for this artifact."""
        return ArtifactKey(
            group_id=self._group_id,
            artifact_id=self._artifact_id,
            packaging=self._packaging,
            classifier=self._classifier,
        )

    def update(
        self,
        group_id=DEFAULT,
        artifact_id=DEFAULT,
        packaging=DEFAULT,
        classifier=DEFAULT,
        version=DEFAULT,
    ):
        """Creates a new artifact with the specified fields modified.

        Args:
            group_id: Optional group_id override.
            artifact_id: Optional artifact_id override.
            packaging: Optional packaging override.
            classifier: Optional classifier override.
            version: Optional version override.
        Returns:
            A new artifact with the specified overridden fields.
        """
        return Artifact(
            group_id=get_or_default(group_id, self._group_id),
            artifact_id=get_or_default(artifact_id, self._artifact_id),
            packaging=get_or_default(packaging, self._packaging),
            classifier=get_or_default(classifier, self._classifier),
            version=get_or_default(version, self._version),
        )

    With = base.deprecated(update)


# ------------------------------------------------------------------------------


def get_or_default(value, default):
    """Picks a value or its default.

    Args:
        value: Value to pick, or base.Default.
        default: Picked when value is base.Default.
    Returns:
        The chosen value.
    """
    return default if value is DEFAULT else value


# ------------------------------------------------------------------------------


def parse_artifact(text):
    """Parses a text artifact coordinate into an Artifact tuple.

    Coordinates are formatted: group:artifact[:packaging[:classifier]]:version

    Args:
        text: Text specification of a Maven artifact
    Returns:
        The parsed Artifact tuple.
    """
    group_id = None
    artifact_id = None
    version = None
    packaging = None
    classifier = None

    comps = text.split(":")
    assert (len(comps) >= 3), "Invalid artifact coordinate: {!r}".format(text)

    group_id, comps = comps[0], comps[1:]
    artifact_id, comps = comps[0], comps[1:]
    version, comps = comps[-1], comps[:-1]
    if len(comps) > 0:
        packaging, comps = comps[0], comps[1:]
    if len(comps) > 0:
        classifier, comps = comps[0], comps[1:]
    assert (len(comps) == 0), "Invalid artifact coordinate: {!r}".format(text)

    return Artifact(
        group_id = group_id,
        artifact_id = artifact_id,
        version = version,
        packaging = packaging,
        classifier = classifier,
    )


def parse_artifact_name(text):
    """Parses a text artifact name into an Artifact name.

    Coordinates are formatted: group:artifact

    Args:
        text: Text specification of a Maven artifact
    Returns:
        The parsed ArtifactName tuple.
    """
    comps = text.split(":")
    assert (len(comps) == 2), ("Invalid Artifact name: %r" % text)
    return ArtifactName(group_id = comps[0], artifact_id = comps[1])


def parse_artifact_key(text):
    """Parses a text artifact  into an Artifact tuple.

    Coordinates are formatted: group:artifact[:packaging[:classifier]]

    Args:
        text: Text specification of a Maven artifact key.
    Returns:
        The parsed ArtifactKey.
    """
    comps = text.split(":")
    assert (2 <= len(comps) <= 4), ("Invalid Artifact key: %r" % text)
    group_id = comps[0]
    artifact_id = comps[1]
    packaging = None
    if len(comps) >= 3:
        packaging = comps[2]
    classifier = None
    if len(comps) >= 4:
        classifier = comps[3]
    return ArtifactKey(
        group_id=group_id,
        artifact_id=artifact_id,
        packaging=packaging,
        classifier=classifier,
    )


# ------------------------------------------------------------------------------


if __name__ == "__main__":
    raise Exception("Not a standalone module")
