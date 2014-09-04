---
layout: post
title: Setup
categories: [tutorials, phonebook-tutorial, devel]
tags: [phonebook]
order: 2
description: Setup and compile instructions for the phonebook tutorial.
---

For this tutorial, we assume you are either using the Kiji Standalone BentoBox or
have installed the individual components described [here](http://www.kiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the Kiji
Standalone Bento box in [three quick steps!](http://www.kiji.org/#tryit).

### Using HBase or Cassandra as Kiji's backing store

Kiji can create Kiji tables in HBase or Cassandra tables.  You can indicate to Kiji which you would
like to use in your Kiji URI.  The URI for an HBase Kiji table looks like:

    kiji://zookeeper-host:zookeeper-port/kiji-instance-name

The URI for a Cassandra Kiji, on the other hand, looks like:

    kiji-cassandra://zookeeper-host:zookeeper-port/cassandra-host:cassandra-port/kiji-instance-name

For more details, please see the Javadocs in the source for `org.kiji.schema.KijiURI` and
`org.kji.schema.cassandra.CassandraKijiURI`.


### Start a Kiji Cluster

If you plan to use a BentoBox, run the following command to set BentoBox-related environment
variables and start the Bento cluster.  To start a Bento Box with HBase:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/kiji-env.sh
bento start
{% endhighlight %}
</div>

To start a Bento Box with Cassandra:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/kiji-env.sh
cassandra-bento start
{% endhighlight %}
</div>

After the BentoBox starts, it displays a list of useful ports for cluster webapps and services.  The
MapReduce JobTracker webapp ([http://localhost:50030](http://localhost:50030) in particular will be
useful for this tutorial.

### Compiling

If you have downloaded the Kiji Standalone BentoBox, the code for this tutorial
is already compiled and located in the `$KIJI_HOME/examples/phonebook/lib/` directory.
You can skip to the [Install Kiji Instance](#ref.install_kiji_instance) section to
continue with the environment setup for playing with the example code.

The source code for this tutorial can be found in `$KIJI_HOME/examples/phonebook`.
The source is included along with a Maven project. To get started using Maven,
consult [Getting started With Maven]({{site.kiji_url}}/get-started-with-maven) or
the [Apache Maven Homepage](http://maven.apache.org/).

The following tools are required to compile this project:

* Maven 3.x
* Java 7

To compile, run `mvn package` from `$KIJI_HOME/examples/phonebook`. The build
artifacts (jars) will be placed in the `$KIJI_HOME/examples/phonebook/target/`
directory. This tutorial assumes you are using the pre-built jars included with
the phonebook example under `$KIJI_HOME/examples/phonebook/lib/`. If you wish to
use jars of example code that you have built, you should adjust the command
lines in this tutorial to use the jars in `$KIJI_HOME/examples/phonebook/target/`.

If you are using the Bento Box, `kiji-env.sh` will have set `$KIJI_HOME` for you
already. If not, you should set that yourself in your environment:

<div class="userinput">
{% highlight bash %}
export KIJI_HOME=/path/to/kiji-schema
{% endhighlight %}
</div>


### Create a Kiji URI

We refer to Kiji instances, Kiji tables, and even columns within a Kiji table with Kiji URIs.  Let's
define a Kiji URI for a new Kiji instance that we shall use for the rest of this tutorial.  If you
want to use HBase (and you started your BentoBox with the `bento start` command), define an
environment variable to contain a URI for an HBase-backed KIJI instance:

    export KIJI=kiji://localhost:2181/phonebook

If you want to use Cassandra (and you started your BentoBox with `cassandra-bento start`), instead
define your URI as follows:

    export KIJI=kiji-cassandra://localhost:2181/localhost:9042/phonebook

### Install Kiji Instance
<a name="ref.install_kiji_instance" id="ref.install_kiji_instance"> </a>

Install your Kiji instance. Running `kiji install` with no `--kiji` flag installs the default instance:

<div class="userinput">
{% highlight bash %}
kiji install --kiji=${KIJI}
{% endhighlight %}
</div>

### Setup Phonebook tutorial Environment

To work through this tutorial, various Kiji tools will require that Avro data
type definitions particular to the working phonebook example be on the
classpath. You can add your artifacts to the Kiji classpath by running:

<div class="userinput">
{% highlight bash %}
export KIJI_CLASSPATH="$KIJI_HOME/examples/phonebook/lib/*"
{% endhighlight %}
</div>

