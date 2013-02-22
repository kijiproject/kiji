---
layout: post
title: Setup
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order: 2
description: Setup and compile instructions for the music recommendation tutorial.
---
For this tutorial, we assume you are either using the Kiji Standalone BentoBox or
have installed the individual components described [here](http://www.kiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the Kiji
Standalone Bento box in [three quick steps!](http://www.kiji.org/#tryit).

If you already installed the BentoBox, make sure you have started it:

<div class="userinput">
{% highlight bash %}
bento start
{% endhighlight %}
</div>

### Compiling

If you have downloaded the Kiji Standalone BentoBox, the code for this tutorial
is already compiled and located in the `$KIJI_HOME/examples/music/lib/` directory.
You can skip to [Set your environment variables](link-to-header-below) if you want
to get started playing with the example code.


The source code for this tutorial can be found in `$KIJI_HOME/examples/music`.
The source is included along with a Maven project. To get started using Maven,
consult [Getting started With Maven]({{site.kiji_url}}/get-started-with-maven) or
the [Apache Maven Homepage](http://maven.apache.org/).

The following tools are required to compile this project:
* Maven 3.x
* Java 6

To compile, run `mvn package` from `$KIJI_HOME/examples/music`. The build
artifacts (jars) will be placed in the `$KIJI_HOME/examples/music/target/`
directory. This tutorial assumes you are using the pre-built jars included with
the music recommendation example under `$KIJI_HOME/examples/music/lib/`. If you wish to
use jars of example code that you have built, you should adjust the command
lines in this tutorial to use the jars in `$KIJI_HOME/examples/music/target/`.

If you are using the Bento Box, `kiji-env.sh` will have set `$KIJI_HOME` for you
already. If not, you should set that yourself in your environment:

<div class="userinput">
{% highlight bash %}
export KIJI_HOME=/path/to/kiji-mapreduce
{% endhighlight %}
</div>

After Bento starts, it will display ports you will need to complete this tutorial.
You will need the MapReduce JobTracker webapp (http://localhost:50030 by default), HDFS NameNode port (8020 by default) and the Zookeeper client port (2181 by default).

##### Set your environment variables
If your HDFS NameNode port or Zookeeper client port are not the default values, change them to
match in the code below.

<div class="userinput">
{% highlight bash %}
export HDFS_ROOT=hdfs://localhost:8020
export KIJI=kiji://localhost:2181/kiji_music
{% endhighlight %}
</div>

To work through this tutorial, various Kiji tools will require that Avro data
type definitions particular to the working music recommendation example be on the
classpath. You can add your artifacts to the Kiji classpath by running:

<div class="userinput">
{% highlight bash %}
export LIBS_DIR=$KIJI_HOME/examples/music/lib
export KIJI_CLASSPATH=$(find ${LIBS_DIR} -name "*.jar" -printf '%p:')
{% endhighlight %}
</div>

### Install Kiji and Create Tables

Install your Kiji instance:

<div class="userinput">
{% highlight bash %} $ kiji install --kiji=${KIJI}
{% endhighlight %}
</div>

Create the Kiji music tables:

<div class="userinput">
{% highlight bash %}
kiji-schema-shell --kiji=${KIJI} --file=$KIJI_HOME/examples/music/music_schema.ddl
{% endhighlight %}
</div>


### Get Data
The music recommendation example comes with pregenerated song data in
`$KIJI_HOME/examples/music/data`.  If you wish to generate new data, wipe the data directory,
navigate to the `$KIJI_HOME/examples/music` directory and use the python script provided.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/examples/music/data_generator.py --output-dir=$KIJI_HOME/examples/music/data/
{% endhighlight %}
</div>

This should generate 3 JSON files: data/song-dist.json, data/song-metadata.json and data/song-plays.json.

Upload the data set to HDFS: (this step is required, even if you did not generate new data)

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir $HDFS_ROOT/kiji-mr-tutorial
hadoop fs -copyFromLocal $KIJI_HOME/examples/music/data/*.json $HDFS_ROOT/kiji-mr-tutorial/
{% endhighlight %}
</div>

