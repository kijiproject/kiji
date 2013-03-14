---
layout: post
title: Setup
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order: 2
description: Setup and compile instructions for the music recommendation tutorial.
---
For this tutorial, we assume you are either using the standalone Kiji BentoBox or
have installed the individual components described [here](http://www.kiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the standalone Kiji
BentoBox in [three quick steps!](http://www.kiji.org/#tryit)

If you already installed BentoBox, make sure you have started it:

<div class="userinput">
{% highlight bash %}
bento start
{% endhighlight %}
</div>

### Compiling

If you have downloaded the standalone Kiji BentoBox, the code for this tutorial
is already compiled and located in the `$KIJI_HOME/examples/music/` directory.
Commands in this tutorial will depend on this location:

<div class="userinput">
{% highlight bash %}
export MUSIC_HOME=${KIJI_HOME}/examples/music
{% endhighlight %}
</div>

If you are not using the Kiji BentoBox, set `MUSIC_HOME` to the path of your local
kiji-music repository.

Once you have done this, if you are using Kiji BentoBox you can skip to
"Set your environment variables" if you want to get started playing with the example code.
Otherwise, follow these steps to compile it from source.

The source code for this tutorial can be found in `$MUSIC_HOME`.
The source is included along with a Maven project. To get started using Maven,
consult [Getting started With Maven]({{site.kiji_url}}/get-started-with-maven) or
the [Apache Maven Homepage](http://maven.apache.org/).

The following tools are required to compile this project:
* Maven 3.x
* Java 6

To compile, run `mvn package` from `$MUSIC_HOME`. The build
artifacts (.jar files) will be placed in the `$MUSIC_HOME/target/`
directory. This tutorial assumes you are using the pre-built jars included with
the music recommendation example under `$MUSIC_HOME/lib/`. If you wish to
use jars of example code that you have built, you should adjust the command
lines in this tutorial to use the jars in `$MUSIC_HOME/target/`.

### Set your environment variables
After Bento starts, it will display ports you will need to complete this tutorial. It will be useful
to know the address of the MapReduce JobTracker webapp
([http://localhost:50030](http://localhost:50030) by default) while working through this tutorial.

It will be useful to define an environment variable named `KIJI` that holds a Kiji URI to the Kiji
instance we'll use during this tutorial.

<div class="userinput">
{% highlight bash %}
export KIJI=kiji://.env/kiji_music
{% endhighlight %}
</div>

To work through this tutorial, various Kiji tools will require that Avro data
type definitions particular to the working music recommendation example be on the
classpath. You can add your artifacts to the Kiji classpath by running:

<div class="userinput">
{% highlight bash %}
export LIBS_DIR=$MUSIC_HOME/lib
export KIJI_CLASSPATH="${LIBS_DIR}/*"
{% endhighlight %}
</div>

### Install Kiji and Create Tables

Install your Kiji instance:

<div class="userinput">
{% highlight bash %}
kiji install --kiji=${KIJI}
{% endhighlight %}
</div>

Create the Kiji music tables that have layouts described in `music_schema.ddl`.

<div id="accordion-container">
  <h2 class="accordion-header"> music_schema.ddl </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/music_schema.ddl"> </script>
  </div>
</div>


<div class="userinput">
{% highlight bash %}
kiji-schema-shell --kiji=${KIJI} --file=$MUSIC_HOME/music_schema.ddl
{% endhighlight %}
</div>

This command uses [kiji-schema-shell](https://github.com/kijiproject/kiji-schema-shell)
to create the tables using the KijiSchema DDL, which makes specifying table layouts easy.
See [the KijiSchema DDL Shell reference]({{site.userguide_schema_rc4}}/schema-shell-ddl-ref)
for more information on the KijiSchema DDL.

##### (Optional) Generate Data

The music recommendation example comes with pregenerated song data in
`$MUSIC_HOME/example_data`.  These .json files contain randomly-generated song information
and randomly-generated usage information for this tutorial.

If you wish to generate new data, wipe the data directory, then use the python script provided.

<div class="userinput">
{% highlight bash %}
rm $MUSIC_HOME/example_data/*
$MUSIC_HOME/bin/data_generator.py --output-dir=$MUSIC_HOME/example_data/
{% endhighlight %}
</div>

This should generate 3 JSON files: `example_data/song-dist.json`, `example_data/song-metadata.json`
and `example_data/song-plays.json`.

### Upload Data to HDFS

Upload the data set to HDFS (this step is required, even if you did not generate new data):

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir kiji-mr-tutorial
hadoop fs -copyFromLocal $MUSIC_HOME/example_data/*.json kiji-mr-tutorial/
{% endhighlight %}
</div>

