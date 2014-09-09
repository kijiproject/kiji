# Bento Cluster Docker

A prototype for a Bento cluster (self contained HDFS/YARN/HBase/Cassandra environment). Runs CDH 5
and Cassandra 2 in pseudo-distributed mode.

## Installation

The Bento Cluster can be installed using pip:

    pip3 install kiji-bento-cluster

To upgrade your installation:

    pip3 install --upgrade kiji-bento-cluster

## Usage

The Bento Cluster is controlled from the `bento` script. `bento help` will show a listing of
all available bento options.

In this section strings in code blocks situated in between angle braces (`<`, `>`) represent fields
to be filled in by the user.

Many of the commands below make usage of the `--bento-name=` flag. If it is not specified, the name
`'bento'` is used.

### A simple workflow

#### Pull

Run the `bento pull` command to fetch the latest bento docker image from dockerhub. Note: this
operation may pause for a long period of time without any output.

If a specific platform version is desired, specify it with the `--platform-version=` flag:

    bento pull --platform-version=cdh5.1.2

#### Create

Run the `bento create` command to create a new bento instance. A name can be specified with the
`--bento-name=` flag:

    bento --bento-name=<my_very_own_bento> create

This command can be validated by running the `bento list` command with the `--all` option (displays
running bento instances as well as ones that aren't running):

    bento list --all

#### Start

Run the `bento start` command to start a created bento instance. Data already existing within bento
instances being started is preserved. To start a specific bento instance use the `--bento-name=`
flag:

    bento --bento-name=<my_very_own_bento> start

This command will block until hdfs has been initialized. The list of running bento instances can be
printed by running:

    bento list

The status of a particular bento instance can also be printed by running:

    bento --bento-name=<my_very_own_bento> status

Bento cluster uses supervisor to bring up and watch its various services. The status of each service
can be observed by accessing the supervisor web UI available at: `http://<my_very_own_bento>:9001/`.

After starting a bento instance hadoop/hbase client configuration files will be written to:

    ~/.bento/<bento-instance-name>/hadoop/
    ~/.bento/<bento-instance-name>/hbase/

To use a particular bento instance source its 'bento-env.sh' script in:

    ~/.bento/<bento-instance-name>/bento-env.sh

#### Stop

Run the `bento stop` command to stop running processes of a bento (not delete its state/data). To
stop a specific bento instance use the `--bento-name=` flag:

    bento --bento-name=<my_very_own_bento> stop

This command will wait until the bento container has stopped.

#### Rm

Run the `bento rm` command to delete a bento instance. To delete a specific bento instance use the
`--bento-name=` flag:

    bento --bento-name=<my_very_own_bento> rm

### Use of Sudo

Bento may ask for sudo permissions when creating or starting a container by default. This is
necessary to update `/etc/hosts` with an entry for the container. This can be handled in several
different ways:

- Skip adding a dns entry for the bento instance by using the `--skip-hosts-edit` (`-e`) flag with
  `bento create` or `bento start`.
- Add a sudoers rule allowing members of the `bento` group to run the `update-etc-hosts` script as
  root without providing a sudo password:

      bento setup-sudoers

  This will also copy the `update-etc-hosts` script to `/usr/local/bin/`. Note for Mac users: the
  `/etc/sudoers.d` directory is not included by default but can be enabled by adding the
  `#includedir /etc/sudoers.d` directive in `/etc/sudoers`.

Note that HBase and some Hadoop functionality will not work if the container's hostname can not be
resolved.

### Maven

See [bento-maven-plugin](https://github.com/kijiproject/bento-maven-plugin).

## Requirements

#### Linux Host

Requires:

1. [Docker](https://docker.com/). Docker
   [requires](http://docker.readthedocs.org/en/v0.5.3/installation/kernel/) Linux kernel 3.8 or
   above.

#### OS X Host

Requires:

1. [boot2docker](https://github.com/boot2docker/boot2docker)
3. A network route to the bento box:
    ```bash
        sudo route add $(bento ip)/16 $(boot2docker ip 2> /dev/null)
    ```

    If you are using OS X and planning on running any MapReduce jobs, you will likely want to allocate
    `boot2docker` more than its default 2 GB of RAM.  You can find directions for doing so on the github
    [page](https://github.com/boot2docker/boot2docker-cli) for the `boot2docker` CLI.  In short, do the
    following:

    - Create a `boot2docker` profile with the default settings:
    ```bash
        boot2docker config > ~/.boot2docker/profile
    ````
    - Update the line in `~/.boot2docker/profile` to increase the amount of memory from 2048 to perhaps
    8192.
    - Sanity check your new settings by running `boot2docker config` again.
    - Destroy your old `boot2docker` VM and start again:
    ```bash
        boot2docker destroy
        boot2docker init
        boot2docker up
    ```
    - Validate your new settings:
    ```bash
        boot2docker info
    ```
