# Bento Cluster Docker

A prototype for a Bento cluster (self contained HDFS/YARN/HBase/Cassandra environment). Runs CDH 5 and Cassandra 2 in pseudo-distributed mode.

## Installation

The Bento Cluster can be installed using pip:

    pip3 install kiji-bento-cluster

## Usage

The Bento Cluster is controlled from the `bento` script. `bento help` will show a listing of
all available bento options.

After starting a bento instance hadoop/hbase client configuration files will be written to:

    ~/.bento/<bento-instance-name>/hadoop/
    ~/.bento/<bento-instance-name>/hbase/

To use a particular bento instance source its 'bento-env.sh' script in:

    ~/.bento/<bento-instance-name>/bento-env.sh

### Use of Sudo

Bento will ask for sudo permissions when creating or starting a container by
default. This is necessary to update `/etc/hosts` with an entry for the
container. This can be disabled by using the `-e` flag with `bento create` or
`bento start`.  Note that HBase and some Hadoop functionality will not work
if the container's hostname can not be resolved.

Linux systems may use the `HOSTALIASES` environment variable in order to avoid
the need for sudo: `export HOSTALIASES=~/.hosts`.
Make sure the `HOSTALIASES` variable is set globally in your environments
(shell and desktop graphical sessions) or else you might encounter problems
resolving the host names created for the Bento docker container.

## Requirements

#### Linux Host

Requires:

1. [Docker](https://docker.com/). Docker [requires](http://docker.readthedocs.org/en/v0.5.3/installation/kernel/) Linux kernel 3.8 or above.

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
