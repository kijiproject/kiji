#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Wrapper for Git."""

import logging
import os
import re
import tempfile
import time

from base import base
from base import command


class Error(Exception):
    """Errors used in this module."""
    pass


# Regexp for a commit tag line: "tag:value"
_RE_TAG_LINE = re.compile(r"^([\w_.\-]+):(.*)$")


# Regexp matching a Git remote specification line:
#     "repo_name repo_address (fetch|push)"
_RE_REMOTE_SPEC = re.compile(r"^([^\s]+)\s+(.*)\s+\((fetch|push)\)$")


_HEAD_PREFIX = "refs/heads/"
_REMOTE_PREFIX = "refs/remotes/"
_TAG_PREFIX = "refs/tags/"


def format_ref(ref):
    """Formats a git reference.

    Strips prefixes such as "refs/{heads,remotes,tags}/".

    Args:
        ref: Git reference name.
    Returns:
        Pretty reference name.
    """
    if ref.startswith(_REMOTE_PREFIX):
        return ref[len(_REMOTE_PREFIX):]
    elif ref.startswith(_HEAD_PREFIX):
        return ref[len(_HEAD_PREFIX):]
    elif ref.startswith(_TAG_PREFIX):
        return ref[len(_TAG_PREFIX):]
    else:
        return ref


def format_refs(refs):
    """Formats a set of git references.

    Args:
        refs: Iterable of git references.
    Returns:
        Formatted set of references.
    """
    symbols = map(str.strip, refs)
    symbols = filter(None, symbols)
    symbols = map(FormatRemote, symbols)
    return "(%s)" % " ".join(symbols)


# ------------------------------------------------------------------------------


class Git(object):
    """Wrapper for a Git repository."""

    def __init__(self, path=None):
        """Initializes a wrapper for a Git repository.

        Args:
            path: Optional path of the Git repository.
                    Defaults to the current working directory.
        """
        self._path = path or os.getcwd()
        self._version = None

        version = self.version
        assert(version[0:2] >= ("1", "8")), \
                ("Must be using git version greater than 1.8, got %r." % (version,))

    @property
    def path(self):
        """Returns: the Git repository path."""
        return self._path

    @property
    def exists(self):
        return os.path.exists(self._path)

    @property
    def version(self):
        """Returns: a tuple representing the verion of git script uses."""
        if self._version is None:
            version_cmd = command.Command("git", "--version", exit_code=0)
            version_str = version_cmd.output_text.split()[2]
            self._version = tuple(version_str.split("."))
        return self._version

    def command(self, *args, exit_code=0, **kwargs):
        """Runs a Git command on this repository.

        Args:
            *args: Git command, eg. ["log"].
            exit_code: Required exit code. Defaults to 0.
            **kwargs: Extra keyword arguments.
        """
        assert self.exists, ("Git repository %r does not exist." % self.path)
        return command.Command(
            "git",
            *args,
            exit_code=exit_code,
            work_dir=self.path,
            **kwargs
        )

    def fetch(self, remote=None, all=False, prune=False):
        """Fetches new changes from the specified remote.

        Args:
            remote: Optional Git remote repository name to fetch from.
            all: Whether to fetch from all known remote repository.
            prune: Whether to prune discarded remote branches.
        """
        args = []
        if remote is not None: args.append(remote)
        if all: args.append("--all")
        if prune: args.append("--prune")
        self.command("fetch", *args)

    def push(
        self,
        remote,
        local_ref,
        remote_ref=None,
        force=False,
    ):
        """Pushes a local reference to a remote repository.

        Args:
            remote: Remote repository to push to.
            local_ref: Local reference to push to the remote.
            remote_ref: Remote reference to push to.
            force: Whether to force the push.
        """
        if remote_ref is None: remote_ref = local_ref
        args = ["push", remote, "%s:%s" % (local_ref, remote_ref)]
        if force: args.append("--force")
        self.command(*args, exit_code=0)

    def checkout(self, ref, create_branch=False, force=False):
        """Checkout or create a branch.

        Args:
            ref: Reference to checkout (eg. commit hash, tag, branch name, etc).
            create_branch: Whether to create a branch.
            force: Whether to force the checkout (discarding local changes).
        """
        args = [ref]
        if create_branch: args.insert(0, "-b")
        if force: args.append("--force")
        self.command("checkout", *args, exit_code=0)

    def create_branch(self, branch_name, ref=None, force=False):
        """Create new branch off current or specified branch, w/o checking it out/

        Args:
            branch_name: Name of branch to create
            ref: commit or branch that new branch is created from, defaults to HEAD
            force: When true, force branch creation (overwrite).
        """
        args = []
        if ref is not None: args.append(ref)
        if force: args.append("--force")
        self.command("branch", branch_name, *args, exit_code=0)

    def delete_branch(self, branch, ignore_merge_status=True):
        """Deletes an existing branch.

        Args:
            branch: Branch name.
            ignore_merge_status: Whether to ignore merge status.
        """
        if ignore_merge_status:
            delete = "-D"
        else:
            delete = "--delete"
        self.command("branch", delete, branch)

    def merge(self, ref, ff_only=True):
        """Merges the current head with the specified reference.

        Args:
            ref: Reference to merge with (eg. a commit hash, a tag or a branch name).
            ff_only: Fast-forward merge only.
        """
        args = [ref]
        if ff_only: args.append("--ff-only")
        self.command("merge", *args)

    def add(
        self,
        paths=(),
        all=False,
        force=False,
    ):
        """Stages changes.

        Args:
            paths: Optional explicit set of file paths to stage.
            all: Whether to stage all files.
            force: Whether to force staging.
        """
        args = ["add"]
        if force:
            args.append("--force")
        if all:
            args.append("--all")
        args.append("--")
        args.extend(paths)
        self.command(*args)

    def commit(
        self,
        message,
        allow_empty=True,
        date=None,
        files=(),
        author=None,
        all=False,
    ):
        """Commits the staged changes.

        Args:
            message: Commit message.
            allow_empty: Whether to allow empty commits (no modified paths).
            date: Optional date for the commit (as a time.struct_time).
            files: Optional explicit list of paths to include in the commit.
            author: Optional explicit commit author.
            all: Automatically stage modifications.
        Returns:
            The id of the commit.
        """
        with tempfile.NamedTemporaryFile() as f:
            f.write(message.encode())
            f.flush()

            args=[]
            args.append("--file={}".format(f.name))
            if allow_empty:
                args.append("--allow-empty")
            if all:
                args.append("--all")
            if author is not None:
                args.append("--author={}".format(author))
            if date is not None:
                args.append("--date={}".format(time.strftime("%s", date)))
            if len(files) > 0:
                args.append("--")
                args.extend(files)

            commit_output = self.command("commit", *args)
            commit_hdr = re.findall(r"\[([^]]*)\]", commit_output.output_lines[0])[0]
            short_id = commit_hdr.split(" ")[-1]

            # Use "git rev-parse" to get the long commit id.
            rev_parse = self.command("rev-parse", short_id, "HEAD")
            lines = list(filter(None, map(str.strip, rev_parse.output_lines)))
            assert (len(lines) == 2), "Expected 2 revisions, got {!r}".format(lines)
            assert (lines[0] == lines[1]), "Expected {!r}, got {!r}".format(lines[0], lines[1])
            return lines[0]

    def update_commit_message(self, summary, body_lines):
        """Updates the top git commit message.

        Args:
            summary: Summary of the Git commit.
            body_lines: Git commit message, as an array of lines.
        """
        summary = summary.strip()

        # Strip leading empty lines:
        while (len(body_lines) > 0) and (len(body_lines[0]) == 0):
            body_lines = body_lines[1:]

        # Strip trailing empty lines:
        while (len(body_lines) > 0) and (len(body_lines[-1]) == 0):
            body_lines = body_lines[:-1]

        # Rewrite the top commit message:
        with tempfile.NamedTemporaryFile() as f:
            f.write(summary.encode())
            f.write(b"\n\n")
            f.write("\n".join(body_lines).encode())
            f.flush()

            self.command("commit", "--amend", "--allow-empty", "--file=%s" % f.name)

    def update_commit_tag(self, tag, value):
        """Updates tags in the top git commit.

        Args:
            tag: Tag to update.
            value: New tag value.
        """
        (git_summary, body_lines) = self.GetCommitMessage()

        tag_line_prefix = "%s:" % tag
        body_lines = list(
                filter(lambda line: not line.startswith(tag_line_prefix), body_lines))
        while (len(body_lines) > 0) and (len(body_lines[-1]) == 0):
            body_lines = body_lines[:-1]
        if (len(body_lines) > 0) and not _RE_TAG_LINE.match(body_lines[-1]):
            body_lines.append("")
        body_lines.append("%s: %s" % (tag, value))

        self.UpdateCommitMessage(git_summary, body_lines)

    def get_commit_hash(self, revision="HEAD"):
        """Reports the commit hash for the specified named revision.

        Commit hashes are 40 hex-digits.

        Args:
            revision: Named git revision, eg. "master", "origin/master", "HEAD^", etc.
                    Defaults to the current head.
        Returns:
            The commit hash for the specified named revision.
        """
        git = self.command("log", "--max-count=1", "--pretty=%H", revision, "--")
        commit_hash = git.output_text.strip()
        assert len(commit_hash) == 40, commit_hash
        return commit_hash

    def current_branch(self):
        """Returns: the name of the current Git branch, or None."""
        git_status = self.command("branch", "--color=never", "--list")
        for line in git_status.output_lines:
            line = line.strip()
            if line.startswith("*"):
                return line[1:].strip()
        return None

    def get_commit_message(self, commit="HEAD"):
        """Reports a git commit message.

        Args:
            commit: Git commit (eg. a commit hash). Default to HEAD.
        Returns:
            A tuple (summary, message as array of lines) of the specified git commit.
        """
        git_log = self.command("log", commit, "--max-count=1", "--pretty=%s%n%b")
        summary = git_log.output_lines[0].strip()
        message = list(map(lambda line: line.rstrip(), git_log.output_lines[1:]))

        # Remove leading empty lines:
        while (len(message) > 0) and (len(message[0]) == 0):
            message = message[1:]

        # Remove trailing empty lines
        while (len(message) > 0) and (len(message[-1]) == 0):
            message = message[:-1]

        return (summary, message)

    def get_commit_details(self, ref="HEAD"):
        """Reports details of the specified commit (HEAD by default).

        Args:
            ref: Optional explicit commit reference to report details of.
        Returns:
            A tuple with:
             - commit hash (full SHA1)
             - parent commit hash (full SHA1)
             - author name
             - author email
             - author time (as a time.struct_time)
             - committer name
             - committer email
             - commit time (as a time.struct_time)
             - commit subject
        """
        cmd = self.command(
            "log",
            "--pretty="
            "%H%n"   # Commit hash
            "%P%n"   # Parent commit hash
            "%an%n"  # Author name
            "%ae%n"  # Author email
            "%ai%n"  # Author time
            "%cn%n"  # Committer name
            "%ce%n"  # Committer email
            "%ci%n"  # Commit time
            "%s%n"   # Subject line
            "%b",    # Body
            "--max-count=1",
            ref,
        )
        lines = cmd.output_lines
        (
            commit, parent,
            author_name, author_email, author_time,
            committer_name, committer_email, committer_time,
            subject,
        ) = lines[:9]
        body = "\n".join(lines[9:])
        author_time = time.strptime(author_time, "%Y-%m-%d %H:%M:%S %z")
        committer_time = time.strptime(committer_time, "%Y-%m-%d %H:%M:%S %z")
        return (
            commit,
            parent,
            author_name, author_email, author_time,
            committer_name, committer_email, committer_time,
            subject, body,
        )

    def get_diff(self, ref1, ref2):
        cmd = self.command("diff", ref1, ref2, "--")

    def list_remote_addresses(self, name=None):
        """Lists the addresses of the Git remote repo.

        Optionally filters according to a specified repository name.

        Args:
            name: Optional name of the Git remote repo, eg. "origin".
        Yields:
            Pairs ("fetch" or "push", Git repo address).
        """
        git = self.command("remote", "--verbose")
        for line in git.output_lines:
            match = _RE_REMOTE_SPEC.match(line)
            if (match is not None) and ((name is None) or (match.group(1) == name)):
                yield (match.group(3), match.group(2))

    def set_remote(self, name, url):
        """Adds or updates a Git remote.

        Args:
            name: Git remote name.
            url: URL of the remote.
        """
        remotes = list(self.ListRemoteAddresses(name=name))
        if len(remotes) == 0:
            self.command("remote", "add", name, url)
        else:
            self.command("remote", "set-url", name, url)

    def list_tags(self):
        """Lists all tags found in the Git repo.

        Returns:
            frozenset of string tag names.
        """
        tags = self.command("tag").output_lines
        tags = filter(lambda line: line != "", tags)
        tags = map(str.strip, tags)
        tags = frozenset(tags)
        return tags

    def github_commit_web_link(self, commit_hash, repo=None):
        """Generates a link for a Github commit, if the repository belongs to Github.

        Args:
            commit_hash: SHA1 commit hash.
            repo: Git repository address.
        Returns:
            A Github link to the specified commit, if relevant, or None.
        """
        assert len(commit_hash) == 40

        # ID of a Git repository: "username/project":
        git_id = None

        ssh_prefix = "git@github.com:"
        git_prefix = "git://github.com/"

        if repo is None:
            repos = map(lambda pair: pair[1], self.ListRemoteAddresses())
        else:
            repos = (repo,)

        for repo in repos:
            if repo.startswith(ssh_prefix):
                git_id = base.strip_prefix(repo, ssh_prefix)
            elif repo.startswith(git_prefix):
                git_id = base.strip_prefix(repo, git_prefix)
            else:
                pass

        if git_id is None:
            return None

        git_id = base.strip_optional_suffix(git_id, ".git")
        return "https://github.com/%s/commit/%s" % (git_id, commit_hash)

    def tag(self, tag, force=False):
        """Tags the current branch/commit.

        Args:
            tag: Tag to attach to the current git branch/commit.
        """
        args = []
        if force: args.append("--force")
        self.command("tag", tag, *args)

    def clone(self, address, recursive=False, depth=None):
        """Clones a Git repository into this repository.

        This repository path must not exist already.

        Args:
            address: Address of the Git repository to clone.
            recursive: Whether to clone submodules recursively.
            depth: Number of commits back in history to pull. If None, pull all.
        """
        assert (not self.exists), ("Git repository %r already exists" % self.path)
        # Cannot use self.command() as self.path does not exist yet:
        cmd = ["git", "clone"]
        if depth is not None:
            cmd.extend(["--depth", depth])
        if recursive:
            cmd.append("--recursive")
        cmd.extend(["--", address, self.path])
        command.Command(*cmd, exit_code=0)

    _RE_SUBMODULE = re.compile(r"""\[\s*submodule\s+"([^"]+)"\s*\]""")
    _RE_KEY_VALUE = re.compile(r"(\w+)\s*=\s*(.*)")

    def list_submodules(self):
        gitmodules_path = os.path.join(self.path, ".gitmodules")
        if not os.path.exists(gitmodules_path):
            return

        with open(gitmodules_path, mode="rt", encoding="UTF-8") as file:
            lines = file.readlines()

        name = None
        path = None
        url = None

        for line in lines:
            line = line.strip()
            if len(line) == 0:
                continue
            match = self._RE_SUBMODULE.match(line)
            if match is not None:
                # Emit current entry:
                if name is not None:
                    assert (path is not None) and (url is not None)
                    yield (name, os.path.join(self.path, path), url)

                # Reset for next entry:
                (name, path, url) = (match.group(1), None, None)
                continue

            match = self._RE_KEY_VALUE.match(line)
            if match is not None:
                key = match.group(1)
                value = match.group(2)
                if key == "url":
                    url = value
                elif key == "path":
                    path = value
                else:
                    logging.error("Ignoring key-value in %r : %r", gitmodules_path, line)
                continue

            logging.error("Ignoring invalid line in %r : %r", gitmodules_path, line)

        # Emit the last entry:
        if name is not None:
            assert (path is not None) and (url is not None)
            yield (name, os.path.join(self.path, path), url)

    def list_submodule_status(self, recursive=True):
        """Lists the submodules in this git repository.

        Args:
            recursive: Whether to lists the submodules recursively.
        Yields:
            Tuples (git commit hash, submodule name, ref diff or None).
        """
        args = ["submodule", "status"]
        if recursive:
            args.append("--recursive")
        cmd = self.command(*args)
        for line in cmd.output_lines:
            line = line.strip()
            if len(line) == 0:
                continue
            split = line.split()
            (commit_hash, module_name) = split[:2]
            if len(split) > 2:
                ref_diff = split[2]
                # ref_diff is formatted as "(<tag>-<commit-count>-g<short-hash>)"
                ref_diff = base.strip_prefix(ref_diff, "(")
                ref_diff = base.strip_suffix(ref_diff, ")")
            else:
                ref_diff = None
            yield (commit_hash, module_name, ref_diff)


    def add_submodule(self, address, path=None):
        """Adds a submodule to this repository.

        Args:
            address: Address of the Git repository to add as sub-module.
        """
        args = ["submodule", "add", address]
        if path is not None: args.append(path)
        self.command(*args, exit_code=0)

    def update_submodule(self, *submodules, init=False, recursive=False):
        """Updates a submodule in this repository.

        Args:
            *submodules: List of submodule names to update.
            init: Whether to initialize while updating.
            recursive: Whether to update recursively.
        """
        args = ["submodule", "update"]
        if init:
            args.append("--init")
        if recursive:
            args.append("--recursive")
        args.append("--")
        args.extend(submodules)

        self.command(*args, exit_code=0)

    def list_revisions(self, head="HEAD", since=None):
        """Lists the revisions from the specified commit to the initial commit.

        Revisions are ordered from the most recent to the oldest one.
        Note: this may return unexpected results if the commits are not fast-forward.

        Args:
            head: Optionally specifies the most recent commit to list revisions for.
            since: Optionally specifies the oldest commit to list revisions for.
        Returns:
            An iterable of Git commit hash.
        """
        if since is None:
            slice = head
        else:
            slice = "%s..%s" % (since, head)
        cmd = self.command("log", slice, "--pretty=%H")
        revisions = map(lambda str: str.strip(), cmd.output_lines)
        revisions = filter(None, revisions)
        return revisions

    def list_trees(self, head="HEAD", since=None):
        """Lists the trees from the specified commit to the initial commit.

        Revisions are ordered from the most recent to the oldest one.
        Note: this may return unexpected results if the commits are not fast-forward.

        Args:
            head: Optionally specifies the most recent commit to list revisions for.
            since: Optionally specifies the oldest commit to list revisions for.
        Returns:
            An iterable of Git commit hash.
        """
        if since is None:
            slice = head
        else:
            slice = "%s..%s" % (since, head)
        cmd = self.command("log", slice, "--pretty=%T")
        revisions = map(lambda str: str.strip(), cmd.output_lines)
        revisions = filter(None, revisions)
        return revisions

    def diff_tree_list_paths(self, before, after):
        cmd = self.command("diff-tree", "-r", "--name-only", "--no-renames", before, after)
        for line in cmd.output_lines:
            line = line.strip()
            if len(line) == 0:
                continue
            yield line

    def diff_tree_make_patch(self, before, after, paths=()):
        """Creates a binary patch between the specified git trees.

        Args:
            before: Git tree to diff from.
            after: Git tree to diff to.
            paths: Optional explicit list of paths to include in the diff.
        Returns:
            Binary patch (bytes).
        """
        cmd = self.command("diff-tree", "-p", "--binary", before, after, "--", *paths)
        return cmd.output_bytes

    def get_merge_base(self, *commits):
        """Returns the best merge base for a set of commits.

        Args:
            commits: commit references to determine a common ancestor for.
        Returns:
            The SHA1 commit hash for the identified merge-base.
        """
        cmd = self.command("merge-base", *commits)
        lines = list(filter(None, cmd.output_lines))
        assert (len(lines) == 1), ("Expecting exactly one line: %r" % lines)
        merge_base = lines[0]
        assert (len(merge_base) == 40), ("Expecting SHA1 commit hash: %r" % merge_base)
        return merge_base

    def list_branches(self):
        """Lists the branches in this repository.

        Returns:
            A list of branch names. Current branch comes first.
        """
        cmd = self.command("branch")

        current = None
        branches = []
        for line in filter(None, map(str.strip, cmd.output_lines)):
            if line.startswith("* "):
                current = line[2:].strip()
            else:
                branches.append(line)
        branches.insert(0, current)
        return branches

    def list_refs(self):
        """Lists the references in this repository.

        Yields:
            Pairs (commit hash, reference name).
            Reference name examples are "refs/remotes/x".
        """
        cmd = self.command("show-ref")
        for line in filter(None, map(str.strip, cmd.output_lines)):
            (commit, ref) = line.split(None, 1)
            yield (commit, ref)

    def list_head_ref_names(self):
        """Lists the reference names for the current head.

        Yields:
            Names for the current head.
        """
        head_commit = self.GetCommitHash()
        for (commit, ref) in self.ListRefs():
            if commit == head_commit:
                yield ref

    def list_files(self):
        """Lists all files the current git repository is tracking.

        All files are relative to the root of the git repository.

        Returns:
            An iterable of paths to files git is tracking.
        """
        cmd = self.command("ls-files")
        return filter(None, map(str.strip, cmd.output_lines))

    def apply(self, patch_path, index=True):
        """Applies a patch to this Git repository.

        Args:
            patch_path: Path of the patch file to apply.
            index: Whether to update the Git index.
        """
        args = [patch_path]
        if index: args.append("--index")
        self.command("apply", *args)

    def status(self, untracked="all", ignored=False):
        """Lists the status of files in this Git repo.

        Args:
            untracked: Whether to include untracked files.
                    One of "all", "normal" or "no".
            ignored: Whether to include ignored files.
        Yields:
            Pairs (status code, file path).
            Status code are 2 letters strings as specified by the Git status
            command.
        """
        args = ["--short"]
        if untracked is not None: args.append("--untracked=" + untracked),
        if ignored: args.append("--ignored")

        status = self.command("status", "--porcelain", *args)
        for line in filter(None, status.output_lines):
            split = line.split()
            assert (len(split) == 2), line
            yield split


    # Deprecated aliases, available for temporary compatibility only:

    AddSubmodule = add_submodule
    Apply = apply
    Checkout = checkout
    Clone = clone
    Command = command
    Commit = commit
    CreateBranch = create_branch
    CurrentBranch = current_branch
    DeleteBranch = delete_branch
    Fetch = fetch
    GetCommitDetails = get_commit_details
    GetCommitHash = get_commit_hash
    GetCommitMessage = get_commit_message
    GithubCommitWebLink = github_commit_web_link
    ListBranches = list_branches
    ListFiles = list_files
    ListHeadRefNames = list_head_ref_names
    ListRefs = list_refs
    ListRemoteAddresses = list_remote_addresses
    ListRevisions = list_revisions
    ListSubmoduleStatus = list_submodule_status
    ListTags = list_tags
    Merge = merge
    Push = push
    SetRemote = set_remote
    Status = status
    Tag = tag
    UpdateCommitMessage = update_commit_message
    UpdateCommitTag = update_commit_tag
    UpdateSubmodule = update_submodule


FormatRef = base.deprecated(format_ref)
FormatRefs = base.deprecated(format_refs)
