---
layout: post
title: Setting up Kiji and HDFS
categories: [tutorials, express-recommendation, 2.0.1]
tags: [express-music]
order: 3
description: Setup for KijiExpress Tutorial
---

### Install Kiji BentoBox

If you don't have a working environment yet, install the standalone Kiji BentoBox in [three quick
steps!](http://www.kiji.org/#tryit)

### Start a Kiji Cluster

*  If you plan to use a BentoBox, run the following command to set BentoBox-related environment
   variables and start the Bento cluster:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/kiji-env.sh
bento start
{% endhighlight %}
</div>

After BentoBox starts, it displays a list of useful ports for cluster webapps and services.  The
MapReduce JobTracker webapp ([http://localhost:50030](http://localhost:50030) in particular will be
useful for this tutorial.


-  If you are running Kiji without a BentoBox, there are a few things you'll need to do to make sure
   your environment behaves the same way as a BentoBox:


<div id="accordion-container">
  <h2 class="accordion-header">Starting Kiji in Non-BentoBox Systems </h2>
  <div class="accordion-content">
<ol>
<li>Make sure HDFS is installed and started.</li>
<li>Make sure MapReduce is installed, that <code>HADOOP_HOME</code> is set to your
	<code>MR</code> distribution, and that MapReduce is started.</li>
<li>Make sure HBase is installed, that <code>HBASE_HOME</code> is set to your <code>hbase</code>
	distribution, and that HBase is started.</li>
<li>Export <code>KIJI_HOME</code> to the root of your <code>kiji</code> distribution.</li>
<li>Export <code>PATH=${PATH}:${KIJI_HOME}/bin</code>.</li>
<li>Export <code>EXPRESS_HOME</code> to the root of your <code>kiji-express</code> distribution.</li>
<li>Export <code>PATH=${PATH}:${EXPRESS_HOME}/bin</code></li>
</ol>
  </div>
</div>

When the tutorial refers to the BentoBox, you'll know that you'll have to manage your
Kiji cluster appropriately.

### Set Tutorial-Specific Environment Variables

*  Define an environment variable named `KIJI` that holds a Kiji URI to the Kiji
instance we'll use during this tutorial:

<div class="userinput">
{% highlight bash %}
export KIJI=kiji://.env/kiji_express_music
{% endhighlight %}
</div>

The code for this tutorial is located in the `${KIJI_HOME}/examples/express-music/` directory.
Commands in this tutorial will depend on this location.

*  Set a variable for the tutorial location:

<div class="userinput">
{% highlight bash %}
export MUSIC_EXPRESS_HOME=${KIJI_HOME}/examples/express-music
{% endhighlight %}
</div>

### Install Kiji

*  Install your Kiji instance:

<div class="userinput">
{% highlight bash %}
kiji install --kiji=${KIJI}
{% endhighlight %}
</div>

### Create Tables

The file `music-schema.ddl` defines table layouts that are used in this tutorial:
<div id="accordion-container">
  <h2 class="accordion-header"> music-schema.ddl </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/kiji-express-music-2.0.1/src/main/resources/org/kiji/express/music/music-schema.ddl"> </script>
  </div>
</div>

*  Create the Kiji music tables that have layouts described in `music-schema.ddl`.

<div class="userinput">
{% highlight bash %}
${KIJI_HOME}/schema-shell/bin/kiji-schema-shell --kiji=${KIJI} --file=${MUSIC_EXPRESS_HOME}/music-schema.ddl
{% endhighlight %}
</div>

This command uses kiji-schema-shell
to create the tables using the KijiSchema DDL, which makes specifying table layouts easy.
See [the KijiSchema DDL Shell reference]({{site.userguide_schema_1_4_0}}/schema-shell-ddl-ref)
for more information on the KijiSchema DDL.

*  Verify the Kiji music tables were correctly created:

<div class="userinput">
{% highlight bash %}
kiji ls ${KIJI}
{% endhighlight %}
</div>

You should see the newly-created `songs` and `users` tables:

    kiji://localhost:2181/express_music/songs
    kiji://localhost:2181/express_music/users

### Upload Data to HDFS

HDFS stands for Hadoop Distributed File System.  If you are running the BentoBox,
it is running as a filesystem on your machine atop your native filesystem.
This tutorial demonstrates loading data from HDFS into Kiji tables, which is a typical
first step when creating KijiExpress applications.

*  Upload the data set to HDFS:

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir express-tutorial
hadoop fs -copyFromLocal ${MUSIC_EXPRESS_HOME}/example_data/*.json express-tutorial/
{% endhighlight %}
</div>

You're now ready for the next step, [Importing Data](../express-importing-data).

### Kiji Administration Quick Reference

Here are some of the Kiji commands introduced on this page and a few more useful ones:

+ **Start a BentoBox Cluster**:

{% highlight bash %}
cd <path/to/bento>
source bin/kiji-env.sh
bento start
{% endhighlight %}

+ **Stop your BentoBox Cluster**:

{% highlight bash %}
bento stop
{% endhighlight %}

+ **Default location of the MapReduce JobTracker web app**:
[http://localhost:50030](http://localhost:50030)

+ **Install a Kiji instance**:

{% highlight bash %}
kiji install --kiji=<URI/of/instance>
{% endhighlight %}

The URI takes the form:

{% highlight bash %}
kiji://.env/<instance name>
{% endhighlight %}

+ **Running compiled KijiExpress jobs**

To run a KijiExpress job, you invoke a command of the following form:

{% highlight bash %}
express.py job \
    --user-jar=path/to/jar/containing/job \
    --job-name=org.MyKijiApp.MyJob \
    [--libjars=<list of JAR files, separated by colon>] \
    [--mode=local|hdfs] \
    [job-specific options]
{% endhighlight %}

The `mode=hdfs` flag indicates that KijiExpress should run the job against the Hadoop cluster
versus in Cascading's local environment.  The `-libjars` flag indicates additional JAR files needed
to run the command.

+ **Launching the KijiExpress shell**

KijiExpress includes an interactive shell that can be used to execute KijiExpress flows. To launch
the shell, you invoke a command of the following form:

{% highlight bash %}
express.py shell \
    [--libjars=<list of JAR files, separated by colon>] \
    [--mode=local|hdfs]
{% endhighlight %}

If the mode flag is set to 'hdfs', `mode=hdfs`, the shell will run jobs using Scalding's Hadoop
mode. For normal usage against a hadoop cluster, this option should be used.

If the `-libjars` flag is nonempty, the jar files specified will be placed on the classpath.
This is helpful if you are using external libraries or have compiled avro classes.

To execute multi-line statements in the shell, use paste mode. This can be used to execute existing
KijiExpress code. To start paste mode, enter the following command into a running KijiExpress shell:

    :set paste

Type `Ctrl+D` to end paste mode and execute the entered code.
