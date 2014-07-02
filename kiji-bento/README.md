(c) Copyright 2013 WibiData, Inc.

Kiji BentoBox ${project.version}
================================

The Kiji BentoBox is an SDK for developing Big Data Applications with the
Kiji framework. It includes a complete set of Kiji framework modules,
with compatible versions of each.

This kiji-bento release (${project.version}) includes:

* `kiji-schema` ${kiji-schema.version}: Included at the top-level of the
  distribution, kiji-schema provides a simple Java API and command-line tools
  for storing and managing typed data in HBase.
* `kiji-mapreduce` ${kiji-mapreduce.version}: Included in the `lib` directory,
  kiji-mapreduce provides a simple Java API and command-line tools for using
  Hadoop MapReduce to import and process data in Kiji tables.
* `kiji-mapreduce-lib` ${kiji-mapreduce-lib.version}: Included in the `lib`
  directory, kiji-mapreduce-lib is a Java library of utilities for writing
  MapReduce jobs on Kiji tables, as well as ready to use producers, gatherers,
  and importers.
* `bento-cluster` ${bento-cluster.version}: Located in the `cluster`
  directory, bento-cluster allows users to run HDFS, MapReduce, and HBase
  clusters easily on the local machine.
* `kiji-schema-shell` ${kiji-schema-shell.version}: Included in the
  `schema-shell` directory, kiji-schema-shell provides a layout definition
  language for use with `kiji-schema`.
* `kiji-hive-adapter` ${kiji-hive-adapter.version}: Included in the
  `hive-adapter` directory, kiji-hive-adapter provides a SerDe for
  Hive to use Kiji tables as external Hive tables.
* `kiji-express` ${kiji-express.version}: Included in the `express`
  directory, kiji-express provides a Scala DSL for analyzing and modeling
  data stored in Kiji.
* `kiji-express-tools` ${kiji-express-tools.version}: included in the `express`
  directory, kiji-express-tools provides a REPL and related tools for Express.
* `kiji-modeling` ${kiji-modeling.version}: Included in the `modeling`
  directory, kiji-modeling provides a formalization for training, applying,
  and evaluation machine learning models built on top of kiji-express.
* `kiji-scoring` ${kiji-scoring.version}: Included in the `scoring` directory,
  kiji-scoring is a library and server that supports the real-time per-row
  calculations on kiji tables.
* `kiji-model-repository` {kiji-model-repository.version}: Included in the
  `model-repo` directory, the kiji-model-repository is a library which permits
  storage of trained kiji-modeling in a maven repository, indexed by a kiji
  table. kiji-scoring can use this models stored in this repository to for its
  scoring.
* `kiji-phonebook` ${kiji-phonebook.version}: Included in the
  `examples/phonebook` directory, kiji-phonebook is an example standalone
  application (with source code) that stores, processes, and retrieves data
  from a Kiji instance.
* `kiji-music` ${kiji-music.version}: Included in the
  `examples/music` directory, kiji-music is an example of loading the listening
  history of users of a music service into a Kiji table, and then generating new
  music recommendations.
* `kiji-express-music` ${kiji-express-music.version}: Included in the
  `examples/express-music` directory, kiji-express-music is a kiji-express
  implementation of the kiji-music example.
* `kiji-express-examples` ${kiji-express-examples.version}: Included in the
  `examples/express-examples` directory, kiji-express-examples provides example
  usage of kiji-express processing a newsgroups dataset.
* API documentation is made available in the `docs` directory.

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

Upgrade Server Check-in
------------------------
Kiji BentoBox will periodically check in with an upgrade server to
determine if there are any upgrades available for your distribution.
If upgrades are available, the `kiji` script that comes with BentoBox
will periodically remind you (once a day) of available upgrades.
BentoBox sends anonymized information when checking in with the
upgrade server.

To disable checking in with the upgrade server, write a file named
`.disable_kiji_checkin` to the `$HOME/.kiji`. Using the `touch` command
is sufficient. For
example:

> `touch $HOME/.kiji/.disable_kiji_checkin`

will disable check in with the upgrade server.

