---
layout: post
title: For Administrators
categories: [userguides, schema, 1.0.0-rc5]
tags : [schema-ug]
version: 1.0.0-rc5
order : 5
description: Installation and maintenance of kiji-schema.
---

In this chapter, you will learn how to install and configure all aspects
of the KijiSchema system.

## Installation Guide<a id="installation"> </a>

Kiji has been tested on GNU/Linux (Ubuntu 12.04, 12.10 and CentOS-6.3)
and Mac OS X (10.8.x, 10.7.x, 10.6.x).

Kiji is a Java-based system. To run Kiji applications, you will need to
[download the Oracle Java JRE](http://www.oracle.com/technetwork/java/javase/downloads/index.html).
To develop Kiji applications, you will
need the Oracle Java JDK. We have tested this system with the Oracle
Java “Hotspot” JVM, version 6. Other JVMs are not supported at this
time. If you are running OS X, this is installed by default.

Kiji is built on top of Hadoop and HBase. Our system depends on
[Cloudera's Distribution including Apache Hadoop](https://ccp.cloudera.com/display/SUPPORT/Downloads), version 4 (CDH4).
If you downloaded the BentoBox, a zero-configuration development/test
cluster is included in the package. If you downloaded KijiSchema, you
will need to install and configure Hadoop and HBase separately.

### Installing and Configuring Kiji Clients<a name="installingclients"> </a>

After downloading either the BentoBox
(`kiji-bento-version-release.tar.gz`) or KijiSchema
(`kiji-schema-version-release.tar.gz`), unzip the archive with the
command `tar vzxf filename`. This will create to a directory named
`kiji-bento-version/` or `kiji-schema-version/`.

For convenience, `$KIJI_HOME` should be set to this directory. For
example:

{% highlight bash %}
$ export KIJI_HOME=/path/to/kiji-bento-(version)
{% endhighlight %}

You should edit your `.bashrc` file to contain this line so that it's
incorporated in your environment for future bash sessions.

If you're using the BentoBox edition, you can set up your environment as
follows:

{% highlight bash %}
$ source $KIJI_HOME/bin/kiji-env.sh
$ bento start
{% endhighlight %}

This starts the bento mini cluster and updates your environment variables
to use it. After a few seconds you should be able to view the status
page of your own mini HBase cluster at [http://localhost:60010/](http://localhost:60010/). (If
you don't see it right away, wait 10 seconds and reload the page.)
You're now ready to proceed with installing Kiji onto your cluster.

If you installed only KijiSchema (not the BentoBox), you should instead
set `$HADOOP_HOME` and `$HBASE_HOME` and make sure the Hadoop HDFS and
HBase services are running.

For help configuring CDH, see Cloudera's CDH4 Installation Guide.

## Installing Kiji System Tables

Kiji will manage tables for you on top of your HBase cluster. Each Kiji
table corresponds to a physical HBase table. There are also a number of
system tables that hold metadata maintained by Kiji itself. Before you
can use Kiji, you must run its install command and create these system
tables.

Issue the following command from your Kiji release directory to install
a Kiji instance with the name `default`:

{% highlight bash %}
$ bin/kiji install
{% endhighlight %}

You should see output like the following:

    Creating kiji instance: kiji://localhost:2181/default/
    Creating meta tables for kiji instance in hbase...
    12/11/06 19:48:44 INFO org.kiji.schema.KijiInstaller: Installing a kiji instance named 'default'...
    12/11/06 19:48:50 INFO org.kiji.schema.KijiInstaller: Installed kiji 'default'
    Successfully created kiji instance: kiji://localhost:2181/default/

Now you can create some tables in the
[DDL schema-shell]({{site.userguide_schema_rc4}}/schema-shell-ddl-ref), explore the
[Phonebook example]({{site.tutorial_phonebook_rc4}}/phonebook-tutorial), and get started
[building a Maven project](http://www.kiji.org/get-started-with-maven) with
Kiji. See the
[quickstart guide](http://www.kiji.org/getstarted/#Quick_Start_Guide) to get
acquainted with the tools.

## Maintenance Guide

Due to the fault-tolerant nature of Hadoop and HBase, your Kiji instance
should not require much ongoing maintenance. The following list of
recommended practices describe what maintenance should be performed to
ensure uninterrupted operation.

+  Remove old logs - If log files fill a disk, Hadoop and HBase services may be
   disrupted. Make sure to archive or delete old log files in `$HADOOP_HOME/logs`
   and `$HBASE_HOME/logs` on a regular basis.

+  Flush KijiSchema metadata tables - KijiSchema metadata tables are not frequently
   updated. When an update happens, it is only recorded in the write-ahead logs
   for the table. Periodically, you should run `bin/kiji flush-table --meta` to
   flush metadata tables. This should be scheduled to run during a period of
   low activity, nightly or weekly.

## Troubleshooting Guide

### Missing HBase Regions<a name="trouble.data.access"> </a>

If HBase shuts down incorrectly or HDFS issues cause data loss, HBase's own
metadata tables `-ROOT-` and `.META.` may have inconsistent data. You can verify
that your HBase metadata tables are in good order by running `hbase hbck`.

If `hbase hbck` prints `Status: INCONSISTENT`,
the advice in this section may help you restore functionality.

Some problems can be fixed by running `hbase hbck -fix`.
This should be your first attempt.

One particular kind of issue is a "hole" in the region chain for a table.  Each
row stored in a table is assigned a region based on its row key. Each region is
responsible for a slice of the row key space.  There should be no gaps between
these slices. If you run `hbase hbck -details`, it will print an error similar
to the following if it detects a hole in the region chain:

{% highlight bash %}
$ hbase hbck -details
...

Chain of regions in table kiji..table.tablename is broken; edges does not contain key
Table kiji..table.tablename is inconsistent.
...
Status: INCONSISTENT
{% endhighlight %}

If the files for a missing region are present in HDFS, then no data has been
lost.  HBase has simply lost track of the metadata for the region.  The
following command can be used to scan your HBase root directory in HDFS for
region data, and add any regions back to tables that are missing from the HBase
metadata:

{% highlight bash %}
$ hbase hbck -repairHoles
{% endhighlight %}

See [hbck in depth](http://hbase.apache.org/book/hbck.in.depth.html) for more
details.
