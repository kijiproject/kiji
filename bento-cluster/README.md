# Bento Cluster Docker

A prototype for a Bento cluster (self contained HDFS/YARN/HBase/Cassandra environment). Runs CDH 5 and Cassandra 2 in pseudo-distributed mode.

## Usage

The Bento Cluster is controlled from the `bento` script located at `bin/bento`.
The `bento-env.sh` script located at `bin/bento-env.sh` should be sourced from
within your `.bashrc` or `.zshrc`, this will put the `bento` script on your path
and add the correct Hadoop & HBase client configurations to your environment.
`bento help` will give you a full list of available options.

## Requirements

#### Linux Host

Requires [Docker](https://docker.com/). Docker [requires](http://docker.readthedocs.org/en/v0.5.3/installation/kernel/) Linux kernel 3.8 or above.

#### OS X Host

1. Requires [boot2docker](https://github.com/boot2docker/boot2docker)
2. Requires a network route to the bento box:
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
