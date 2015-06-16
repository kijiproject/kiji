#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Maven repository wrapper."""

import hashlib
import logging
import os
import sys
import tempfile
import urllib.error
import urllib.request
import xml.etree.ElementTree as etree

from base import base

from wibi.maven import artifact


FLAGS = base.FLAGS
DEFAULT = base.DEFAULT
LOG_LEVEL = base.LOG_LEVEL


class Error(Exception):
    """Errors used in this module."""
    pass


class ArtifactNotFoundError(Error):
    """Raised when an artifact is not found."""
    pass


# ------------------------------------------------------------------------------


CLOJARS_REPO = "https://clojars.org/repo"
CLOUDERA_REPO = "https://repository.cloudera.com/artifactory/cloudera-repos"
CONCURRENT_REPO = "http://conjars.org/repo"
MAVEN_APACHE_REPO = "http://repo.maven.apache.org/maven2"
MAVEN_CENTRAL_REPO = "http://central.maven.org/maven2"
SONATYPE_REPO = "https://oss.sonatype.org/content/repositories/public"
TWITTER_REPO = "http://maven.twttr.com"


# ------------------------------------------------------------------------------


# TODO: This is unused, but generally useful. Move to base?
def get_file_md5(path):
    assert os.path.exists(path), ("File not found: %r" % path)
    buffer_size = 128 * 1024

    md5 = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            bytes = f.read(buffer_size)
            if len(bytes) == 0:
                break
            md5.update(bytes)
    return md5.hexdigest()


# TODO: This is unused, but generally useful. Move to base?
def get_file_sha1(path):
    assert os.path.exists(path), ("File not found: %r" % path)
    buffer_size = 128 * 1024

    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            bytes = f.read(buffer_size)
            if len(bytes) == 0:
                break
            sha1.update(bytes)
    return sha1.hexdigest()


def get_file_fingerprints(path):
    """Computes the MD5 and the SHA1 sums of a specified path.

    Args:
        path: Path of the file to sum.
    Returns:
        A pair (MD5, SHA1) with the sums of the specified file.
    """
    assert os.path.exists(path), ("File not found: %r" % path)
    buffer_size = 128 * 1024

    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            bytes = f.read(buffer_size)
            if len(bytes) == 0:
                break
            md5.update(bytes)
            sha1.update(bytes)
    return (md5.hexdigest(), sha1.hexdigest())


# ------------------------------------------------------------------------------


class RemoteRepository(object):
    """Wraps a single Maven repository, whether local or remote."""

    def __init__(self, path, use_checksum=False):
        """Initializes a Maven repository wrapper.

        Args:
            path: Path (URL) of the Maven repository,
                eg. 'file:///home/x/.m2/repository' for a local repository,
                and 'https://repo.wibidata.com/artifactory/kiji-packages' for a remote repository.
            use_checksum: Whether to fetch and compare MD5 and SHA1 checksums.
        """
        self._path = path
        self._use_checksum = use_checksum

    @property
    def path(self):
        return self._path

    @property
    def is_local(self):
        """Returns: whether this Maven repository is local."""
        return self.path.startswith("file://")

    @property
    def is_remote(self):
        """Returns: whether this Maven repository is remote."""
        return (self.path.startswith("http://") or self.path.startswith("https://"))

    def __str__(self):
        return "RemoteRepository(path=%s)" % self.path

    def __repr__(self):
        return str(self)

    def read_file(self, path):
        """Reads a file and returns its content as a byte array.

        Args:
            path: Path of the file to read.
        Returns:
            The file content as a byte array, or None if the file does not exist.
        """
        url = os.path.join(self._path, path)
        try:
            logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Fetching %r", url)
            with urllib.request.urlopen(url) as opened:
                return opened.read()
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return None
            if err.code == 401:
                logging.warn("Not authorized to read %r", path)
                return None
            else:
                raise err

    def get_path(self, artf):
        """Gets the relative file path for the specified Maven artifact.

        Note: snapshot artifact version are not supported (snapshot timestamps).

        Args:
            artf: Maven artifact coordinates, as an Artifact object.
        Returns:
            File path for the specified Maven artifact, relative to the Maven repository root.
        """
        return self.GetPath(
            group=artf.group_id,
            artifact=artf.artifact_id,
            version=artf.version,
            type=artf.packaging,
            classifier=artf.classifier,
            snapshot_version=None,
        )

    def get_artifact_url(self, artf):
        """Returns: an full, absolute URL for the specified artifact in this Maven repository."""
        return os.path.join(self.path, self.get_path(artf))

    def GetPath(
        self,
        group,
        artifact=None,
        version=None,
        type=None,
        classifier=None,
        snapshot_version=None,
    ):
        """Reports the relative path for the specified resource.

        Deprecated: use get_path(artifact) instead.

        Args:
            group: Artifact group ID.
            artifact: Optional artifact ID.
            version: Optional artifact version.
                Requires artifact to be set.
            type: Optional artifact type (eg. 'jar', 'pom' or 'tar.gz').
                Requires version to be set.
            classifier: Optional classifier (eg. 'release' or 'tests').
                Requires type to be set.
            snapshot_version: Optional snapshot version.
                Require version ending with '-SNAPSHOT'.
        Returns:
            The path for the specified resource.
        """
        path = os.path.join(*group.split("."))
        if artifact is None: return path  # Return group directory
        path = os.path.join(path, artifact)
        if version is None: return path  # Return artifact directory
        path = os.path.join(path, version)
        if type is None: return path  # Return artifact version directory

        # Requested resource is a file:

        name_parts = [artifact]
        if snapshot_version is not None:
            assert version.endswith("-SNAPSHOT")
            name_parts.append(snapshot_version)
        else:
            name_parts.append(version)

        if type == "test-jar":
            type = "jar"
            if classifier is None:
                classifier = "tests"
            else:
                assert (classifier == "tests"), \
                    "Invalid test-jar classifier {!r}".format(classifier)

        if classifier is not None:
            name_parts.append(classifier)

        name = "%s.%s" % ("-".join(name_parts), type)
        path = os.path.join(path, name)

        return path

    def get_url(self, **coordinate):
        """Reports the full URL for the specified artifact coordinate.

        Args:
            **coordinate: Artifact coordinates.
        Returns:
            The Full URL for the specified artifact.
        """
        return os.path.join(self.path, self.GetPath(**coordinate))

    def read_metadata_file(self, group, artifact, version=None):
        """Retrieves the metadata file content for an artifact.

        Args:
            group: Artifact group ID.
            artifact: Artifact name.
            version: Optional version ID.
        Returns:
            The metadata file content, as a byte array, or None.
        """
        if not self.is_remote:
            return None
        path = self.GetPath(group=group, artifact=artifact, version=version)
        metadata_path = os.path.join(path, "maven-metadata.xml")
        return self.read_file(metadata_path)

    def list_versions(self, group, artifact):
        """Lists the versions of an artifact.

        Args:
            group: Artifact group ID.
            artifact: Artifact name.
        Yields:
            Version IDs.
        """
        try:
            metadata = self.ReadMetadataFile(group=group, artifact=artifact)
            if metadata is None:
                return
        except urllib.error.HTTPError as err:
            return
        xml = etree.fromstring(metadata)
        [versioning] = xml.getiterator("versioning")
        [versions] = versioning.findall("versions")
        for version in versions.findall("version"):
            yield version.text.strip()

    def resolve(
        self,
        group,
        artifact,
        version,
        type,
        classifier=None,
        snapshot_version=None,
    ):
        """Resolves an artifact coordinate into an absolute.

        Resolves snapshot coordinates into absolute artifact coordinates.

        Args:
            group: Artifact coordinate.
            artifact: Artifact coordinate.
            version: Artifact coordinate.
            type: Artifact coordinate.
            classifier: Artifact coordinate.
            snapshot_version: Artifact coordinate.
        Returns:
            The resolved artifact coordinate, as a dictionary.
            None if the artifact is unknown on this repository.
        """
        if version.endswith("-SNAPSHOT") and (snapshot_version is None):
            if self.is_remote:
                # Resolve remote snapshot version:
                metadata = self.ReadMetadataFile(
                    group=group,
                    artifact=artifact,
                    version=version,
                )
                if metadata is None:
                    return None

                if metadata is not None:
                    xml = etree.fromstring(metadata)
                    [snapshot] = xml.getiterator(tag="snapshot")

                    [timestamp] = snapshot.findall("timestamp")
                    timestamp = timestamp.text

                    [build_number] = snapshot.findall("buildNumber")
                    build_number = build_number.text

                    base_version = base.StripSuffix(version, "-SNAPSHOT")
                    snapshot_version = "-".join([base_version, timestamp, build_number])

            else:
                # Is there a version of the snapshot in the local repository?
                path = self.GetPath(
                    group=group,
                    artifact=artifact,
                    version=version,
                    type=type,
                    classifier=classifier,
                    snapshot_version=snapshot_version,
                )
                if self.read_file(path) is None:
                    return None

        coordinate = dict(
            group=group,
            artifact=artifact,
            version=version,
            type=type,
            classifier=classifier,
            snapshot_version=snapshot_version,
        )
        return coordinate

    def read_md5_file(self, path):
        """Reads the MD5 sum file for a specified path.

        Args:
            path: Path of the file whose MD5 sum file to read.
        Returns:
            The MD5 sum, or None if the MD5 sum file does not exist.
        """
        if not self._use_checksum:
            return None

        md5 = self.read_file("%s.md5" % path)
        if md5 is not None:
            md5 = md5.decode().strip()
            if len(md5) != 32:
                logging.error("Invalid MD5 sum: %r", path)
                return None
        return md5

    def read_sha1_file(self, path):
        """Reads the SHA1 sum file for a specified path.

        Args:
            path: Path of the file whose SHA1 sum file to read.
        Returns:
            The SHA1 sum, or None if the SHA1 sum file does not exist.
        """
        if not self._use_checksum:
            return None

        sha1 = self.read_file("%s.sha1" % path)
        if sha1 is not None:
            sha1 = sha1.decode().strip()
            if len(sha1) != 40:
                logging.error("Invalid SHA1 sum: %r", path)
                return None
        return sha1

    def open(
        self,
        group,
        artifact,
        version,
        type,
        classifier=None,
        snapshot_version=None,
    ):
        """Fetches a Maven artifact.

        Args:
            group: Artifact group ID.
            artifact: Artifact name.
            version: Version of the artifact.
            type: Type of the artifact to retrieve, eg. 'pom', 'jar', 'tar.gz'.
            classifier: Optional classifier, eg. 'release' or 'tests'.
            snapshot_version: Optional snapshot version.

        Returns:
            A tuple with (HTTP reply, MD5 sum, SHA1 sum) for the specified artifact,
            or None if the artifact does not exist.
        """
        coordinate = dict(
            group=group,
            artifact=artifact,
            version=version,
            type=type,
            classifier=classifier,
        )
        coordinate = self.resolve(**coordinate)
        file_path = self.GetPath(**coordinate)
        md5 = self.read_md5_file(file_path)
        sha1 = self.read_sha1_file(file_path)
        file_url = os.path.join(self._path, file_path)

        logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Opening %r", file_url)
        http_req = urllib.request.Request(url=file_url)
        try:
            http_reply = urllib.request.urlopen(http_req)
            return (http_reply, md5, sha1)
        except urllib.error.HTTPError as err:
            if err.code == 404:
                logging.debug("Artifact %r does not exist in %s", file_path, self.path)
                return None
            elif err.code == 409:
                logging.debug(
                    "Artifact %r rejected due to snapshot/release policies in repository %r",
                    file_path, self.path)
                return None
            else:
                raise err

    @staticmethod
    def read_to_file(http_reply, output_path, buffer_size=1024*1024):
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Fetching %r to %r", http_reply.url, output_path)

        md5 = hashlib.md5()
        sha1 = hashlib.sha1()

        with tempfile.NamedTemporaryFile(prefix=output_path, delete=False, mode="wb") as temp:
            while True:
                data = http_reply.read(buffer_size)
                if len(data) == 0: break
                temp.file.write(data)
                md5.update(data)
                sha1.update(data)

            temp.file.close()
            os.rename(src=temp.name, dst=output_path)

        logging.debug("Fetch of %r completed to %r", http_reply.url, output_path)
        return (md5.hexdigest(), sha1.hexdigest())

    def artifact_for_path(self, path):
        """Reverse engineer the Artifact coordinate from a repository file path.

        Args:
            path: Path of a Maven artifact in this repository.
        Returns:
            The parsed Artifact coordinate.
            None if the file path does not belong to this repository.
        """
        path = "file://" + os.path.abspath(path)
        if not path.startswith(self.path):
            return None
        path = os.path.relpath(path, self.path)
        [*group_id, artifact_id, version, name] = path.split("/")
        group_id = ".".join(group_id)

        name = base.strip_prefix(name, artifact_id + "-" + version)
        if name.startswith("-"):
            (classifier, extension) = name[1:].split(".", 1)
        elif name.startswith("."):
            classifier = None
            extension = name[1:]
        else:
            raise Error("Invalid file name does not match expected Maven artifact format: {!r}"
                        .format(path))

        if (classifier == "tests") and (extension == "jar"):
            packaging = "test-jar"
        else:
            packaging = extension

        return artifact.Artifact(
            group_id=group_id,
            artifact_id=artifact_id,
            version=version,
            classifier=classifier,
            packaging=packaging,
        )

# ------------------------------------------------------------------------------


class MavenRepository(object):
    """Maven repository.

    A Maven repository consists in:
     - a main local (writable) repository,
     - a set of "remote" (read-only) repositories.
    A remote repository is specified via a URL (file:// or http[s]://).

    Any artifact from a remote repository may be brought locally.
    Snapshot artifacts are always checked for newer versions agaist the remote
    repositories.
    """

    def __init__(
        self,
        local=DEFAULT,
        remotes=(),
        exclusions=frozenset(),
        remote_snapshots=False,
    ):
        """Initializes a Maven repository.

        Args:
            local: Path of the directory for the local repository.
            remotes: Ordered list of remote repository URLs (file:// or http[s]://).
            exclusions: Optional set of excluded Maven repositories.
            remote_snapshots: Whether to allow fetching snapshot artifacts remotely.
        """
        if local is DEFAULT:
            local = os.path.join(os.environ["HOME"], ".m2", "repository")
        if len(urllib.request.urlparse(local).scheme) == 0:
            local = "file://%s" % local
        assert (urllib.request.urlparse(local).scheme == "file"), (
                "Invalid local repository path: %r" % local)
        self._local_path = local
        self._local = RemoteRepository(path=self._local_path, use_checksum=True)

        self._remote_paths = tuple(remotes)
        self._remotes = tuple(map(RemoteRepository, self._remote_paths))

        self._exclusions = frozenset(exclusions)

        self._remote_snapshots = remote_snapshots

        logging.debug("Initialized Maven repository with local=%s and remotes=%r",
                      self.local, self.remotes)

    def __str__(self):
        return "MavenRepository(local=%s, remotes=%s, exclusions=%s)" % (
            self.local,
            ",".join(self.remotes),
            ",".join(self.exclusions),
        )

    def __repr__(self):
        return str(self)

    @property
    def local(self):
        """Local writable Maven repository."""
        return self._local

    @property
    def remotes(self):
        """Ordered list of remote Maven repositories."""
        return self._remotes

    @property
    def exclusions(self):
        """Set of excluded Maven remote repositories."""
        return self._exclusions

    def add_remote(self, remote):
        """Adds a new remote repository, unless it belongs to the exclusion list.

        Args:
            remote: URL of the Maven remote repository.
        Returns:
            Whether the remote repository was added or not.
        """
        if remote in self._remote_paths:
            # Remote repository already registered.
            pass
        elif remote in self.exclusions:
            logging.debug("Ignoring excluded Maven remote repository: %r", remote)
        else:
            logging.debug("Adding Maven remote repository: %r", remote)
            self._remote_paths += (remote,)
            self._remotes += (RemoteRepository(remote),)

    def get(self, artf):
        """Retrieves an artifact locally.

        If the artifact is a snapshot (version ends with '-SNAPSHOT'),
        all remotes are checked for a newer version.

        Args:
            group: Artifact group ID.
            artifact: Artifact ID.
            version: Artifact version.
            type: Artifact type, aka. packaging ('jar', 'pom', etc).
            classifier: Optional classifier (eg. 'test' or 'release').
        Returns:
            The path of the artifact in the local repository.
            None if the artifact cannot be found.
        """
        return self.Get(
            group=artf.group_id,
            artifact=artf.artifact_id,
            version=artf.version,
            type=artf.packaging,
            classifier=artf.classifier,
        )

    def Get(self, group, artifact, version, type, classifier=None):
        """Retrieves an artifact locally.

        Deprecated, this method is going away: use get(artifact) instead.

        If the artifact is a snapshot (version ends with '-SNAPSHOT'),
        all remotes are checked for a newer version.

        Args:
            group: Artifact group ID.
            artifact: Artifact ID.
            version: Artifact version.
            type: Artifact type, aka. packaging ('jar', 'pom', etc).
            classifier: Optional classifier (eg. 'test' or 'release').
        Returns:
            The path of the artifact in the local repository.
            None if the artifact cannot be found.
        """
        coordinate = dict(
            group=group,
            artifact=artifact,
            version=version,
            type=type,
            classifier=classifier,
        )
        logging.log(LOG_LEVEL.DEBUG_VERBOSE, "Getting artifact with coordinate %r", coordinate)
        path = self.local.GetPath(**coordinate)
        parsed = urllib.request.urlparse(self.local.get_url(**coordinate))
        assert (parsed.scheme == "file")
        local_path = parsed.path
        md5_path = "%s.md5" % local_path
        sha1_path = "%s.sha1" % local_path

        # Artifact is a snapshot, resolve it first if allowed:
        if version.endswith("-SNAPSHOT") and self._remote_snapshots:
            # Find the most recent snapshot version from all the remote repositories:

            def ScanRemotes():
                for remote in self.remotes:
                    resolved = remote.resolve(**coordinate)
                    if resolved is None: continue
                    # Allow for snapshots resolved in a local repository:
                    # if resolved["snapshot_version"] is None: continue
                    yield (resolved, remote)

            # Snapshot on remote repositories are expected to have a snapshot_version:
            best_remote = None
            best_version = dict(snapshot_version="", **coordinate)

            # Snapshot built locally into a local repository has no snapshot_version.
            # This lists the local repositories where unversioned snapshots are found:
            local_repos = list()

            for (resolved, remote) in ScanRemotes():
                if resolved["snapshot_version"] is None:
                    local_repos.append(remote)
                elif best_version["snapshot_version"] < resolved["snapshot_version"]:
                    best_remote = remote
                    best_version = resolved

            if (best_remote is None) and (len(local_repos) == 0):
                raise ArtifactNotFoundError(
                    "Artifact %s:%s:%s:%s:%s not found in remote repositories" % (
                        coordinate["group"],
                        coordinate["artifact"],
                        coordinate["classifier"],
                        coordinate["type"],
                        coordinate["version"]))
            elif len(local_repos) == 0:
                logging.debug("Artifact resolved to %s in remote %s", best_version, best_remote)
            elif best_remote is None:
                assert (len(local_repos) == 1), \
                    ("Multiple snapshot local copies of %r" % coordinate)
                local_repo = local_repos[0]
                parsed = urllib.request.urlparse(local_repo.get_url(**coordinate))
                assert (parsed.scheme == "file")
                local_path = parsed.path
                return local_path
            else:
                raise Error(
                    "Multiple snapshot copies in local repositories and remote repositories: %r"
                    % coordinate)

            (http_reply, md5, sha1) = best_remote.open(**best_version)
            try:
                # Do we have this snapshot artifact locally already:
                if (os.path.exists(local_path)
                    and os.path.exists(md5_path)
                    and os.path.exists(sha1_path)
                    and md5 == self.local.read_md5_file(path)
                    and sha1 == self.local.read_sha1_file(path)
                    and (get_file_fingerprints(local_path) == (self.local.read_md5_file(path),
                                                               self.local.read_sha1_file(path)))
                ):
                    logging.log(LOG_LEVEL.DEBUG_VERBOSE,
                                "Snapshot artifact found locally: %r", local_path)
                    return local_path
            finally:
                http_reply.close()
            logging.log(LOG_LEVEL.DEBUG_VERBOSE,
                        "Snapshot artifact not found locally: %r", coordinate)
            remotes = (best_remote,)

        # Artifact is a snapshot but we do not allow remote snapshots:
        elif version.endswith("-SNAPSHOT") and not self._remote_snapshots:
            logging.log(LOG_LEVEL.DEBUG_VERBOSE,
                        "Restricting snapshot artifact to local FS: %r", local_path)
            if os.path.exists(local_path):
                logging.log(LOG_LEVEL.DEBUG_VERBOSE,
                            "Local snapshot artifact found: %r", local_path)
                return local_path
            else:
                logging.debug("Local snapshot artifact not found: %r", local_path)
                return None

        else:
            # Do we have this non-snapshot artifact locally already?
            if (os.path.exists(local_path)
                and (os.path.getsize(local_path) > 0)   # FIXME quick workaround
                and os.path.exists(md5_path) and os.path.exists(sha1_path)
                and (get_file_fingerprints(local_path) == (self.local.read_md5_file(path),
                                                           self.local.read_sha1_file(path)))
            ):
                # Yes, artifact found locally, with matching checksums:
                logging.log(LOG_LEVEL.DEBUG_VERBOSE,
                            "Artifact found locally with matching checksums: %r", local_path)
                return local_path
            else:
                # Look for the artifact in the configured remotes:
                logging.debug("Artifact not found locally: %r", local_path)
                remotes = self.remotes

        # Artifact does not exist locally.
        # Try each remote repository on after another,
        # pick the first that contains the artifact we are looking for:
        for remote in remotes:
            try:
                open_result = remote.open(**coordinate)
                if open_result is None:
                    continue

                (http_reply, md5, sha1) = open_result
                try:
                    base.make_dir(os.path.dirname(local_path))
                    (actual_md5, actual_sha1) = RemoteRepository.read_to_file(
                        http_reply=http_reply,
                        output_path=local_path,
                    )

                    if md5 is None:
                        logging.debug("No MD5 sum for %r from %s", local_path, remote.path)
                        md5 = actual_md5
                    elif md5 != actual_md5:
                        logging.error("MD5 mismatch for %r from %r: expected %r, got %r",
                                      local_path, remote.path, md5, actual_md5)

                    if sha1 is None:
                        logging.debug("No SHA1 sum for %r from %s", local_path, remote.path)
                        sha1 = actual_sha1
                    elif sha1 != actual_sha1:
                        logging.error("SHA1 mismatch for %r from %r: expected %r, got %r",
                                      local_path, remote.path, sha1, actual_sha1)

                    if (md5 == actual_md5) and (sha1 == actual_sha1):
                        logging.debug("Writing MD5 sum for %r", local_path)
                        with open(md5_path, "w") as f:
                            f.write(md5)
                        logging.debug("Writing SHA1 sum for %r", local_path)
                        with open(sha1_path, "w") as f:
                            f.write(sha1)
                        return local_path
                    else:
                        logging.warning("Checksum invalid for %r", local_path)
                        os.remove(local_path)
                finally:
                    http_reply.close()

            except urllib.error.HTTPError as err:
                logging.error("Error on remote %r: %r", remote, err.readall().decode())

            except urllib.error.URLError as err:
                if isinstance(err.reason, FileNotFoundError):
                    logging.debug("File not found: %r", err.reason.filename)
                else:
                    logging.error("Error on remote %r: %r", remote, err)


        # Artifact is nowhere to be found:
        return None

    AddRemote = base.deprecated(add_remote)


# ------------------------------------------------------------------------------


if __name__ == "__main__":
    raise Error("Cannot run %s as a standalone script." % base.get_program_name())
