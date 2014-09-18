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
