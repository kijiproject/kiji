#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
# (c) Copyright 2014 WibiData, Inc.
"""API for the bento cluster.

Two classes make up the api:

import bento
# Install this with: pip install docker-py
import docker

# BentoSystem represents the portion of the api not specific to a bento-cluster instance.
bento_system = bento.BentoSystem(docker_client=docker.Client())

# Bento represents the api for interacting with a bento-cluster instance.
bento1 = bento_system.create_bento(bento_name='bento1')

# Starting and stopping the bento cluster.
assert bento1 not bento1.is_running
bento1.start()
assert bento1.is_running
bento1.stop()
assert bento1 not bento1.is_running

# Listing and deleting all bento clusters.
for bento_instance in bento_system.list_bentos():
    bento_instance.stop()

    # Delete the state for the bento cluster.
    bento_system.delete_bento(bento_name=bento_instance.bento_name)
"""

import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
import xmlrpc.client

import docker

HOSTS_UPDATER_ETC = 'update-etc-hosts'
HOSTS_UPDATER_HOSTALIASES = 'update-user-hosts'

DEFAULT_GLOBAL_SCRIPT_PATH = '/usr/local/bin'
DEFAULT_BENTO_NAME = 'bento'
DEFAULT_BENTO_IMAGE = 'kijiproject/bento-cluster'
DEFAULT_BENTO_PLATFORM = 'cdh5.0.3'
DEFAULT_TIMEOUT_MS = 120000
DEFAULT_POLL_INTERVAL = 0.1

BENTO_ENV_TEMPLATE = """\
#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# -*- mode: shell -*-

# Canonicalize a path into an absolute, symlink free path.
#
# Portable implementation of the GNU coreutils "readlink -f path".
# The '-f' option of readlink does not exist on MacOS, for instance.
#
# Args:
#   param $1: path to canonicalize.
# Stdout:
#   Prints the canonicalized path on stdout.
function resolve_symlink() {
  local target_file=$1

  if [[ -z "${target_file}" ]]; then
    echo ""
    return 0
  fi

  cd "$(dirname "${target_file}")"
  target_file=$(basename "${target_file}")

  # Iterate down a (possible) chain of symlinks
  local count=0
  while [[ -L "${target_file}" ]]; do
    if [[ "${count}" -gt 1000 ]]; then
      # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
      break
    fi

    target_file=$(readlink "${target_file}")
    cd $(dirname "${target_file}")
    target_file=$(basename "${target_file}")
    count=$(( ${count} + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  local phys_dir=$(pwd -P)
  echo "${phys_dir}/${target_file}"
}

# ------------------------------------------------------------------------------

bento_env_path="${BASH_SOURCE:-$0}"
bento_env_path=$(resolve_symlink "${bento_env_path}")
bento_conf_dir=$(dirname "${bento_env_path}")

export HADOOP_CONF_DIR="${bento_conf_dir}/hadoop/"
export HBASE_CONF_DIR="${bento_conf_dir}/hbase/"

# Linux environments obey the HOSTALIASES environment variable:
if [[ "$(uname)" == "Linux" ]]; then
  if [[ -z "${HOSTALIASES}" ]]; then
    echo "WARNING: The HOSTALIASES environment variable is not set." 1>&2
    echo "WARNING: This may prevent applications from resolving the Bento host name." 1>&2
    echo "WARNING: Please update your bash configuration and add: " 1>&2
    echo "WARNING:     export HOSTALIASES=${HOME}/.hosts" 1>&2

    # Make sure the file exists
    touch "${HOME}/.hosts"
    export HOSTALIASES="${HOME}/.hosts"
  fi
fi
"""

CORE_SITE_TEMPLATE = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://%(bento_host)s:8020</value>
  </property>
</configuration>
"""
MAPRED_SITE_TEMPLATE = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>

  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>%(bento_host)s:10020</value>
  </property>
</configuration>
"""
YARN_SITE_TEMPLATE = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>%(bento_host)s</value>
  </property>
  <property>
    <name>yarn.application.classpath</name>
    <value>/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*</value>
  </property>
  <property>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>/var/log/hadoop-yarn/apps</value>
  </property>
</configuration>
"""
HBASE_SITE_TEMPLATE = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>%(bento_host)s</value>
  </property>
</configuration>
"""
SUDOERS_RULE_TEMPLATE = """\
%%bento ALL = (ALL) NOPASSWD: %(script_path)s
"""


class BentoSystem(object):
    """API for interacting with the bento system."""

    def __init__(
        self,
        docker_client=None,
        bento_image=DEFAULT_BENTO_IMAGE,
    ):
        """Constructs a new BentoSystem.

        Provides methods for creating, deleting, and listing bento instances.

        Args:
            docker_client: A docker client to use to manage the bento docker image/container.
            bento_image: Name of the bento docker image to use.
        """
        # Avoid using a mutable default argument.
        if docker_client is None:
            docker_host = os.environ.get('DOCKER_HOST')
            if docker_host is None:
                self._docker_client = docker.Client()
            else:
                self._docker_client = docker.Client(docker_host)
        else:
            self._docker_client = docker_client
        self._bento_image = bento_image

    @property
    def docker_client(self):
        """Docker client to interact with the bento container with."""
        return self._docker_client

    @property
    def bento_image(self):
        """Name of the bento docker image to use."""
        return self._bento_image

    def pull_bento(
        self,
        platform_version=DEFAULT_BENTO_PLATFORM,
        verbose=False,
    ):
        """Pulls a new bento docker image from dockerhub.

        Uses the repository name specified upon creation of this class.

        Args:
            platform_version: String identifying the version of the hadoop/hbase stack the started
                bento cluster will run.
            verbose: Set to true to print the response from the docker daemon.
        """
        response = self.docker_client.pull(
            repository=self.bento_image,
            tag=platform_version,
        )

        if verbose:
            # Print output to the screen.
            _print_output_from_docker_cmd(response)

    def create_bento(
        self,
        bento_name=DEFAULT_BENTO_NAME,
        platform_version=DEFAULT_BENTO_PLATFORM,
        write_client_config=True,
        client_config_dir=None,
        verbose=False,
    ):
        """Creates a new bento instance.

        Creates a new bento docker container from an image. Will error if a bento image has not been
        cached already.

        Args:
            bento_name: Name to give the bento instance.
            platform_version: String identifying the version of the hadoop/hbase stack the started
                bento cluster will run.
            write_client_config: Should be set to 'True' to write hadoop/hbase client configuration
                files for the bento cluster. These files will be written to '~/.bento/<bento-name>'
                unless 'client_conf_dir' is specified.
            client_config_dir: The directory to write hadoop/hbase client configuration files to.
            verbose: Set to true to print the response from the docker daemon.
        Returns:
            A bento instance.
        """
        response = self.docker_client.create_container(
            image='%s:%s' % (self.bento_image, platform_version),
            name=bento_name,
            hostname=bento_name,
        )

        if verbose:
            # Print output to the screen.
            _print_output_from_docker_cmd(response)

        created_bento = Bento(
            bento_container=bento_name,
            docker_client=self.docker_client,
            client_config_dir=client_config_dir,
        )
        if write_client_config:
            created_bento.write_hadoop_config()
        return created_bento

    def delete_bento(self, bento_name=DEFAULT_BENTO_NAME, verbose=False):
        """Deletes a bento instance along with all of its state.

        This will delete all hbase, cassandra, hdfs, zookeeper, etc. data.

        Args:
            bento_name: Name of the bento instance to delete.
            verbose: Set to true to print the response from the docker daemon.
        """
        response = self.docker_client.remove_container(container=bento_name)

        if verbose:
            # Print output to the screen.
            _print_output_from_docker_cmd(response)

    def list_bentos(self, only_running=True):
        """Lists bento instances.

        Args:
            only_running: Set to true to include only the bento instances that are currently
            running.
        Returns:
            A list of bento instances.
        """
        running_docker_containers = self.docker_client.containers(all=(not only_running))
        return [
            Bento(bento_container=container['Id'], docker_client=self.docker_client)
            for container in running_docker_containers
            if container['Image'].startswith(self.bento_image)
        ]


class Bento(object):
    """API for interacting with a bento instance."""

    def __init__(
        self,
        bento_container,
        docker_client=None,
        client_config_dir=None,
    ):
        """Constructs a new Bento.

        Args:
            bento_container: The name of the bento container running this bento instance.
            docker_client: The docker client to use to connect to the bento container.
            client_config_dir: The directory to write hadoop/hbase client configuration files to.
            hosts_file_path: Path to the hosts file to update with dns entries.
        """
        self._bento_container = bento_container
        # Avoid using a mutable default argument.
        if docker_client is None:
            docker_host = os.environ.get('DOCKER_HOST')
            if docker_host is None:
                self._docker_client = docker.Client()
            else:
                self._docker_client = docker.Client(docker_host)
        else:
            self._docker_client = docker_client
        if client_config_dir is None:
            self._client_config_dir = os.path.join(os.environ['HOME'], '.bento', bento_container)
        else:
            self._client_config_dir = client_config_dir

    @property
    def docker_client(self):
        """Docker client to interact with the bento container with."""
        return self._docker_client

    @property
    def bento_container(self):
        """Name of the docker container running this bento instance."""
        return self._bento_container

    @property
    def client_config_dir(self):
        """Directory to write hadoop/hbase client configuration files to."""
        return self._client_config_dir

    @property
    def docker_config(self):
        """Gets the docker configuration for the docker container running this bento instance."""
        return self.docker_client.inspect_container(self.bento_container)

    @property
    def is_running(self):
        """Is True if this bento instance is running.

        A bento instance is determined to be 'running' if its hdfs-init script has completed
        successfully according to supervisord.
        """
        try:
            hdfs_init_status = _get_hdfs_init_status(host=self.bento_ip)
            return hdfs_init_status['statename'] == 'EXITED' and hdfs_init_status['exitstatus'] == 0
        except ConnectionRefusedError:
            return False
        except socket.gaierror:
            return False

    @property
    def is_container_running(self):
        """Is True if docker container running this bento instance is running.

        This may be True while self.is_running is False during startup.
        """
        return self.docker_config['State']['Running']

    @property
    def bento_name(self):
        """Name of this bento instance."""
        # Remove the leading '/' character.
        return self.docker_config['Name'][1:]

    @property
    def bento_hostname(self):
        """Hostname of the docker container running this bento instance."""
        return self.docker_config['Config']['Hostname']

    @property
    def bento_ip(self):
        """Ip address of the docker container running this bento instance."""
        return self.docker_config['NetworkSettings']['IPAddress']

    def start(
        self,
        update_hosts=True,
        write_client_config=True,
        use_hostaliases=False,
        verbose=False,
        poll_interval=DEFAULT_POLL_INTERVAL,
        timeout_ms=DEFAULT_TIMEOUT_MS,
    ):
        """Starts this bento instance.

        Args:
            update_hosts: Should be set to 'True' to update the user's hosts file with a DNS entry
                for the bento cluster.
            write_client_config: Should be set to 'True' to write hadoop/hbase client configuration
                files for the bento cluster. These files will be written to '~/.bento/<bento-name>'
                unless 'client_conf_dir' is specified.
            use_hostaliases: Should be set to 'True' to add dns entries to the file pointed to by
                the HOSTALIASES environment variable instead of /etc/hosts.
            verbose: Set to true to print the response from the docker daemon.
            poll_interval: Interval at which to poll the bento instance for completion of its
                startup procedure.
            timeout_ms: Maximum amount of time in milliseconds to wait for completion before
                failing.
        """
        assert not self.is_container_running, \
            'Bento container %r already started.' % self.bento_container
        response = self.docker_client.start(container=self.bento_container)

        if verbose:
            # Print output to the screen.
            _print_output_from_docker_cmd(response)

        if write_client_config:
            self.write_hadoop_config(self.client_config_dir)
        if update_hosts:
            self.update_hosts(use_hostaliases=use_hostaliases)

        # Wait until the system is started.
        _wait_for(
            # This has to be a lambda because 'self.is_container_running' is a property.
            condition=lambda: self.is_container_running,
            poll_interval=poll_interval,
            timeout_ms=timeout_ms,
        )
        logging.info('Bento container started.')
        _wait_for(
            # This has to be a lambda because 'self.is__running' is a property.
            condition=lambda: self.is_running,
            poll_interval=poll_interval,
            timeout_ms=timeout_ms,
        )
        logging.info('Bento services started.')

    def stop(
        self,
        verbose=False,
        poll_interval=DEFAULT_POLL_INTERVAL,
        timeout_ms=DEFAULT_TIMEOUT_MS,
    ):
        """Stops this bento instance. Does not delete state.

        Args:
            verbose: Set to true to print the response from the docker daemon.
            poll_interval: Interval at which to poll the bento instance for completion of its
                shutdown procedure.
            timeout_ms: Maximum amount of time in milliseconds to wait for completion before
                failing.
        """
        assert self.is_container_running, 'Bento container %r not running.' % self.bento_container
        response = self.docker_client.stop(container=self.bento_container)

        if verbose:
            # Print output to the screen.
            _print_output_from_docker_cmd(response)

        # Wait until the system is stopped.
        _wait_for(
            condition=lambda: not self.is_container_running,
            poll_interval=poll_interval,
            timeout_ms=timeout_ms,
        )
        logging.info('Bento stopped.')

    def get_log(self):
        """Gets the logs for the docker container running this bento instance."""
        return self.docker_client.logs(container=self.bento_container)

    def update_hosts(
        self,
        use_hostaliases=False,
        global_script_path=DEFAULT_GLOBAL_SCRIPT_PATH,
    ):
        """Updates a hosts file to include an entry for this bento instance.

        Args:
            use_hostaliases: Should be set to 'True' to add dns entries to the file pointed to by
                the HOSTALIASES environment variable instead of /etc/hosts.
            global_script_path: The path to the folder the update-etc-hosts is expected to be
                found in if installed.
        """
        # Get the full path to the update hosts script. If the package was setup correctly, this
        # script should be on the user's PATH.
        if use_hostaliases:
            hosts_updater_script_abspath = _which_exec(HOSTS_UPDATER_HOSTALIASES)
        else:
            hosts_updater_script_abspath = os.path.join(global_script_path, HOSTS_UPDATER_ETC)
            if not os.path.isfile(hosts_updater_script_abspath):
                hosts_updater_script_abspath = None

        # Check the path to the hosts updater script.
        if hosts_updater_script_abspath is None:
            logging.warning(
                'Failed to locate the "%s" script.\n'
                'Please update your /etc/hosts file to include the following entry: "%s %s"',
                HOSTS_UPDATER_HOSTALIASES if use_hostaliases else HOSTS_UPDATER_ETC,
                self.bento_ip,
                self.bento_hostname,
            )
            return

        if use_hostaliases:
            # Run the update-user-hosts script.
            subprocess.check_call(
                args=[hosts_updater_script_abspath, self.bento_ip, self.bento_hostname]
            )
        else:
            # Run the update-etc-hosts script.
            update_etc_hosts_command = _sudo_command(
                args=[
                    hosts_updater_script_abspath,
                    self.bento_ip,
                    self.bento_hostname
                ]
            )
            subprocess.check_call(args=update_etc_hosts_command)

    def write_hadoop_config(self, config_dir=None):
        """Writes hadoop and hbase configuration files for usage with hadoop/hbase/kiji clients.

        Args:
            config_dir: Path to the directory to write the configuration files to. If non-existent,
                it will be created.
        """
        checked_config_dir = self.client_config_dir if config_dir is None else config_dir
        # Assume that this file is in a bento assembly and use ../ as the assembly root.
        output_conf_dir = os.path.abspath(os.path.expanduser(checked_config_dir))

        hadoop_output_dir = os.path.join(output_conf_dir, 'hadoop')
        hbase_output_dir = os.path.join(output_conf_dir, 'hbase')
        os.makedirs(hadoop_output_dir, exist_ok=True)
        os.makedirs(hbase_output_dir, exist_ok=True)

        with open(os.path.join(hadoop_output_dir, 'core-site.xml'), 'wt', encoding='utf-8') \
                as core_site_file:
            core_site_file.write(CORE_SITE_TEMPLATE % dict(bento_host=self.bento_hostname))
        with open(os.path.join(hadoop_output_dir, 'mapred-site.xml'), 'wt', encoding='utf-8') \
                as mapred_site_file:
            mapred_site_file.write(MAPRED_SITE_TEMPLATE % dict(bento_host=self.bento_hostname))
        with open(os.path.join(hadoop_output_dir, 'yarn-site.xml'), 'wt', encoding='utf-8') \
                as yarn_site_file:
            yarn_site_file.write(YARN_SITE_TEMPLATE % dict(bento_host=self.bento_hostname))
        with open(os.path.join(hbase_output_dir, 'hbase-site.xml'), 'wt', encoding='utf-8') \
                as hbase_site_file:
            hbase_site_file.write(HBASE_SITE_TEMPLATE % dict(bento_host=self.bento_hostname))

        with open(os.path.join(output_conf_dir, 'bento-env.sh'), 'wt', encoding='utf-8') \
                as bento_env_file:
            bento_env_file.write(BENTO_ENV_TEMPLATE)


def install_sudoers_rule(
    global_script_path=DEFAULT_GLOBAL_SCRIPT_PATH,
    rule_file_path='/etc/sudoers.d/bento',
):
    """Installs sudoers rule for the bento-update-hosts script.

    Args:
        global_script_path: The path to the folder to install the update-etc-hosts script to.
        rule_file_name: The path to the desired sudoers rule file. Defaults to
            '/etc/sudoers.d/bento'.
    """
    # Get the path to the update-etc-hosts script.
    hosts_updater_abspath = _which_exec(HOSTS_UPDATER_ETC)
    hosts_updater_destination = os.path.join(global_script_path, HOSTS_UPDATER_ETC)

    if hosts_updater_abspath is None:
        logging.error(
            'Failed to locate the "%s" script. Unable to add sudoers rule for bento.',
            HOSTS_UPDATER_ETC,
        )
        return

    # Copy the script to the desired location.
    cp_hosts_updater = ['sudo', 'cp', hosts_updater_abspath, hosts_updater_destination]
    assert subprocess.check_call(args=cp_hosts_updater) == 0, \
        'Failed to copy bento sudoers file to: %s' % rule_file_path

    logging.info('Installed %s to %s', hosts_updater_abspath, hosts_updater_destination)

    # Replace script-location in template.
    script_contents = SUDOERS_RULE_TEMPLATE % dict(script_path=hosts_updater_destination)

    with tempfile.NamedTemporaryFile(prefix=os.path.basename(rule_file_path)) as rule_file:
        # Write the file to a temporary location.
        rule_file.write(script_contents.encode('utf-8'))
        rule_file.flush()

        # Chmod the file to 0440.
        os.chmod(path=rule_file.name, mode=0o440)

        # Validate the file with visudo -cf <path-to-file>.
        assert subprocess.check_call(args=['visudo', '-cf', rule_file.name]) == 0, \
            'Invalid sudoers file produced: %s' % script_contents

        # Copy the file to (overwriting anything that was there).
        assert subprocess.check_call(args=['sudo', 'cp', rule_file.name, rule_file_path]) == 0, \
            'Failed to copy bento sudoers file to: %s' % rule_file_path

    logging.info('Installed sudoers file to: %s', rule_file_path)
    logging.info('To complete setup add the current user to the "bento" group.')


def _sudo_command(args):
    """Generates a list of command line arguments that will be run as root using sudo.

    This method will correctly add the '-n' flag when running in a non-interactive shell.

    Args:
        args: The command line arguments to run with sudo.
    Returns:
        The argument list to run.
    """
    sudo_args = ['sudo']
    if not os.isatty(sys.stdin.fileno()):
        # Enable non-interactive mode for sudo when using a non-interactive shell.
        sudo_args.append('-n')
    return sudo_args.extend(args)


def _which_exec(executable_name):
    """Runs the bash 'which' command to determine the full location of an executable.

    Args:
        executable_name: The name of the executable to search for.
    Returns:
        A string absolute path to the specified executable or None if not found.
    """
    try:
        result = subprocess \
            .check_output(args=['which', executable_name]) \
            .decode('utf-8') \
            .strip()
        return os.path.realpath(result)
    except subprocess.CalledProcessError:
        return None


def _print_output_from_docker_cmd(response, output_buffer=sys.stderr):
    """Prints output from a docker command.

    Attempts to pretty-print docker daemon response strings.

    Args:
        response: A generator of strings containing the response(s) from a docker daemon. Contained
            strings may contain json.
        output_buffer: The buffer to write response lines to.
    """
    if response is None:
        return

    for line in response:
        try:
            line_dict = json.loads(line)
        except ValueError:
            output_buffer.write(line)
            return
        if 'stream' in line_dict:
            output_buffer.write(line_dict['stream'])
        elif 'progress' in line_dict:
            output_buffer.write(line_dict['progress'])
        elif 'status' in line_dict:
            output_buffer.write(line_dict['status'])
        else:
            output_buffer.write(line_dict)


def _wait_for(condition, poll_interval, timeout_ms):
    """Waits with a timeout until the specified condition is true.

    Args:
        condition: To poll for True.
        poll_interval: The rate at which to check the condition.
        timeout_ms: The maximum number of milliseconds to wait for the condition before failing.
    Raises:
        TimeoutError: If the condition fails to become true before the maximum number of attempts
            have been used.
    """
    start_time = time.time()
    while not condition():
        if time.time() - start_time > timeout_ms / 1000.0:
            raise TimeoutError(
                'Failed to start bento container after %dms waiting %f seconds between checks'
                % (timeout_ms, poll_interval)
            )
        time.sleep(poll_interval)


def _get_hdfs_init_status(host=DEFAULT_BENTO_NAME):
    """Reports the status of the hdfs-init process.

    Args:
        host: Host name of the Bento Cluster docker container.
    Returns:
        Descriptor of the hdfs-init process, as a dictionary with the following fields:
        - statename: "RUNNING" or "EXITED",
        - pid: process ID,
        - exitstatus: 0,
        - logfile: path of the log file,
        - stderr_logfile: path of the captured stderr,
        - stop: timestamp of the process end time (seconds since Epoch),
        - description: some description???,
        - state: ???,
        - now: current timestamp (seconds since Epoch),
        - start: timestamp of the process start time (seconds since Epoch),
        - spawnerr: ???,
        - stdout_logfile: path of the captured stdout,
        - name: name of the process,
        - group: group of the process.
        """
    # SuperVisor client:
    svc = xmlrpc.client.ServerProxy('http://%s:9001/RPC2' % host)
    processes = svc.supervisor.getAllProcessInfo()
    hdfs_init_procs = list(filter(lambda proc: proc['name'] == 'hdfs-init', processes))
    assert (len(hdfs_init_procs) == 1), \
        ('Expecting exactly one hdfs-init process, got %d' % len(hdfs_init_procs))
    return hdfs_init_procs[0]
