#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""Stub for self-extracting Java executables."""

import collections
import hashlib
import json
import logging
import os
import pkgutil
import shutil
import subprocess
import sys
import tempfile
import zipfile

from base import base
from base import cli
from base import log


# Global map to collect launcher, JVM and program command-line arguments:
arg_map = collections.defaultdict(list)

# Keys of the 'arg_map':
LAUNCHER_ARGS = os.environ.get("LAUNCHER_ARGS_FLAG_NAME", "--launcher-args")
JVM_ARGS = os.environ.get("JVM_ARGS_FLAG_NAME", "--jvm-args")
PROGRAM_ARGS = os.environ.get("PROGRAM_ARGS_FLAG_NAME", "--program-args")


def md5_sum(path):
    """Computes the MD5 sum of the specified file."""
    with open(path, mode="rb") as bfile:
        return hashlib.md5(bfile.read()).hexdigest()


# --------------------------------------------------------------------------------------------------


class Launcher(cli.Action):
    USAGE = """\
    |Usage:
    |
    |    {program} <program-arg> ... \\
    |        [{launcher_args} <launcher-arg> ...] \\
    |        [{jvm_args} <jvm-arg> ...] \\
    |        [{program_args} <program-arg> ...]
    |
    |Environment variables:
    |    CLASSPATH : Pre-existing classpath to append to.
    |
    |JVM arguments (<jvm-arg>) are options passed to the JVM, eg. '-Xmx2G'.
    |Program arguments (<program-arg>) are passed to the Java program invoked.
    |Launcher arguments (<launcher-arg>) are listed below.
    """.format(
        program=base.get_program_name(),
        launcher_args=LAUNCHER_ARGS,
        jvm_args=JVM_ARGS,
        program_args=PROGRAM_ARGS,
    )

    def register_flags(self):
        profiles = self.config["profiles"]
        profile_name = base.get_program_name()
        if profile_name not in profiles:
            profile_name = self.config["default_profile"]
        profile = profiles[profile_name]

        self.flags.add_string(
            name="java-path",
            default=self.which_java(),
            help="Path of the 'java' executable. Default is the result of $(which java).",
        )
        self.flags.add_string(
            name="class-name",
            default=None,
            help=("Fully qualified name of the Java class to invoke.\n"
                  "Java class for profile {!r} is {!r}"
                  .format(profile_name, profile["main_class"])),
        )
        self.flags.add_string(
            name="profile",
            default=None,
            help=("Profile of the JVM application to run.\n"
                  "Defaults is to use args[0].\n"
                  "Currently selected profile is {!r}."
                  .format(profile_name)),
        )
        self.flags.add_boolean(
            name="print-classpath",
            default=False,
            help="When set, prints the selected classpath and exits.",
        )
        self.flags.add_boolean(
            name="print-config",
            default=False,
            help="When set, prints the selected JSON config and exits.",
        )
        self.flags.add_boolean(
            name="ignore-profile-jvm-args",
            default=False,
            help="When set, ignore JVM arguments from the profile configuration.",
        )

    def run(self, args):
        assert (len(args) == 0), "Unexpected launcher command-line arguments: {!r}".format(args)

        assert os.path.exists(self.flags.java_path), \
            "JVM executable not found: {!r}".format(self.flags.java_path)

        self.extract()

        # Identify which profile to use:
        profiles = self.config["profiles"]

        if self.flags.profile is not None:
            # Profile explicitly specified by user must exist:
            assert (self.flags.profile in profiles), \
                "Invalid profile {!r}, use one of {}." \
                .format(self.flags.profile, sorted(profiles.keys()))
            profile_name = self.flags.profile
        else:
            # No explicit override, default is to use the program name if possible,
            # falling back to the configured default profile if needed.
            profile_name = base.get_program_name()
            if profile_name not in profiles:
                profile_name = self.config["default_profile"]
        profile = profiles[profile_name]

        # Compute the JVM arguments from explicit extension/overrides and from global map:
        jvm_args = list()
        if not self.flags.ignore_profile_jvm_args:
            jvm_args.extend(profile.get("jvm_args", tuple()))
        jvm_args.extend(arg_map[JVM_ARGS])

        # Recover program arguments from global map:
        program_args = arg_map[PROGRAM_ARGS]

        # Compute concrete classpath and set environment CLASSPATH accordingly:
        cp_entries = tuple(self.make_classpath_entries(profile))
        env = dict(os.environ)
        env["CLASSPATH"] = ":".join(cp_entries)

        # Handle --print-X launcher commands:
        should_exit = False  # becomes true if a special command is invoked (eg. --print-foo)
        if self.flags.print_classpath:
            print(":".join(cp_entries))
            should_exit = True
        if self.flags.print_config:
            print(base.json_encode(self.config))
            should_exit = True
        if should_exit:
            return os.EX_OK

        # Determine which Java class to invoke:
        class_name = profile["main_class"]
        if self.flags.class_name is not None:
            class_name = self.flags.class_name

        log.info("Using Java executable: {!r}", self.flags.java_path)
        log.info("Using JVM arguments: {!r}", jvm_args)
        log.info("Using Java main class: {!r}", class_name)
        log.info("Using classpath: {}", base.json_encode(cp_entries))
        log.info("Using Java program arguments: {!r}", program_args)
        log.debug("Using environment: {}", base.json_encode(env))

        args = list()
        args.append(self.flags.java_path)
        args.extend(jvm_args)
        args.append(class_name)
        args.extend(program_args)

        os.execve(self.flags.java_path, args, env)

    def make_classpath_entries(self, profile):
        """Generates the complete classpath for the specified Java application profile.

        Args:
            profile: Java application profile.
        Yields:
            Classpath entry absolute file path, in order.
        """
        yield from filter(None, os.environ.get("CLASSPATH", "").split(":"))
        for cp_entry in profile["classpath"]:
            yield os.path.join(self.package_dir, cp_entry)

    @base.memoized_property
    def config(self):
        """Loads the configuration record (Python dictionary) from the 'java.config' resource."""
        return eval(base.get_resource(package_name="java", resource="config"))

    def extract(self):
        """Extracts this archive into a MD5-named folder.

        Also creates or updates the symlink 'package'.
        """
        this_path = self.this_path
        run_dir = "{}.run".format(this_path)

        # Extract archive, if necessary:
        md5 = md5_sum(this_path)
        md5_dir = os.path.join(run_dir, "md5.%s" % md5)
        if not os.path.isdir(md5_dir):
            pid_dir = '%s.%s' % (md5_dir, os.getpid())

            # Extract the archive contained in this shell-script/file:
            zf = zipfile.ZipFile(this_path)
            zf.extractall(path=pid_dir)

            try:
                # Atomically renamed the 'md5.<sum>.<pid>' directory into 'md5.<sum>'.
                os.rename(pid_dir, md5_dir)
            except OSError as err:
                # The directory rename failed, most likely because another concurrent process raced
                # with this process. At this point, the 'md5.<sum>' folder exists, we just need to
                # cleanup the pid_dir that could not be renamed.
                shutil.rmtree(pid_dir)
            assert os.path.isdir(md5_dir), "Unable to self-extract into {!r}".format(md5_dir)

        # Symlink the md5 dir:
        with tempfile.NamedTemporaryFile(
            prefix="%s." % self.package_dir,
            suffix=".new",
            delete=False
        ) as new_package:
            os.remove(new_package.name)
            os.symlink(src=md5_dir, dst=new_package.name)
            os.rename(src=new_package.name, dst=self.package_dir)

    @property
    def this_path(self):
        """Absolute path of the self-extracting executable archive."""
        return os.path.realpath(sys.argv[0])

    @property
    def run_dir(self):
        """Working directory of self-extracting executable."""
        return "{}.run".format(self.this_path)

    @property
    def package_dir(self):
        """Directory where the content of the self-extracting archive is unpacked."""
        return os.path.join(self.run_dir, "package")

    def which_java(self):
        """Looks up the path of the 'java' executable.

        Returns:
            The path of the 'java' executable available in the current environment.
            None if no 'java' executable is found on the shell path.
        """
        try:
            return subprocess.check_output(args=["which", "java"]).decode().strip()
        except subprocess.CalledProcessError:
            return None


# --------------------------------------------------------------------------------------------------


def main(args):
    """Python entry point for the self-extracting Java archive.

    Args:
        args: Unparsed command-line arguments intended for the Python launcher.
            Global flags are parsed already.
            JVM and program flags have been extracted in the global arg_map.
    """
    tool = Launcher(parent_flags=base.FLAGS)
    return tool(args)


# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    # Command-line arguments in sys.argv are: ['/path/to/self/extracting/archive', 'arg1', ...]

    # Be nice to the console by default:
    arg_map[LAUNCHER_ARGS].append("--log-console-level=warning")

    # Separate launcher, JVM and program command-line arguments:
    key = PROGRAM_ARGS
    for arg in sys.argv[1:]:
        if arg in frozenset({LAUNCHER_ARGS, JVM_ARGS, PROGRAM_ARGS}):
            key = arg
        else:
            arg_map[key].append(arg)

    # Re-create sys.argv to only include the Python launcher command-line arguments:
    launcher_args = arg_map[LAUNCHER_ARGS]

    args = list()
    args.append(sys.argv[0])
    args.extend(launcher_args)
    sys.argv = args

    base.run(main)
