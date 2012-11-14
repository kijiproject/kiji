(c) Copyright 2012 WibiData, Inc.

kiji-bento ${project.version}
=============================

kiji-bento is a standalone distribution of kiji-schema that includes:

* `kiji-schema`: Included at the top-level of the distribution,
kiji-schema provides a simple Java API and tools for storing and
managing typed data in HBase.
* `bento-cluster`: Located in the `cluster` directory, bento-cluster
allows users to run HDFS, MapReduce, and HBase clusters easily on the
local machine.
* `kiji-schema-shell`: Included in the `schema-shell` directory,
kiji-schema-shell provides a layout definition language for use with
`kiji-schema`.

Installation
------------

### Untar kiji-bento
Untar your kiji-bento distribution into a location convenient for
you. We suggest your home directory. In the future we'll call the
path to your kiji-bento distribution `$KIJI_HOME`.

### Configure your system to use kiji-bento.
kiji-bento includes a script that will configure your
environment to use the HDFS, MapReduce and HBase clusters managed by
the included bento-cluster distribution, as well as kiji and
kiji-schema-shell. To configure your environment, run:

> `source $KIJI_HOME/bin/kiji-env.sh`

Add this line to your `~/.bashrc` file to ensure your environment is
always configured for kiji-bento.

`kiji-env.sh` configures your environment to use the Hadoop
ecosystem distributions included with bento-cluster, as well as the
clusters we'll soon use bento-cluster to start. It also adds the
`hadoop`, `hbase`, `bento`, `kiji` and `kiji-schema-shell` scripts to
your `$PATH`, making them available as top-level commands.

### Start bento-cluster.
Now that your system is configured, you can use the `bento` script to
start your local HDFS, MapReduce, and HBase clusters.

> `bento start`

### Use kiji
With the clusters started, you can run Hadoop ecosystem tools like
kiji. For example, to list your home directory in HDFS, run:

> `hadoop fs -ls`

Or, to install the default kiji instance in HBase, run:

> `kiji install`

The `hadoop`, `hbase`, `kiji` and `kiji-schema-shell` scripts are
available for use. You can also use other Hadoop ecosystem tools
like hive or pig and they will use the local clusters managed by
bento-cluster when run in an environment configured with
`kiji-env.sh`.

### Stop bento-cluster.
When you're ready to call it a day, you can stop bento-cluster by
running:

> `bento stop`

This will shutdown the HBase, MapReduce, and HDFS clusters managed by
bento-cluster.

Quickstart
----------

To continue using Kiji, consult the online 
[quickstart guide](http://www.kiji.org/getstarted/#Quick_Start_Guide).
