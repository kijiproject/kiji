---
layout: post
title: Setup
categories: [tutorials, express-recommendation, 0.3.0]
tags: [express-music]
order: 2
description: Setup for KijiExpress Tutorial
---

For this tutorial, we assume you are either using the standalone Kiji BentoBox or
have installed the individual components described [here](http://www.kiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the standalone Kiji
BentoBox in [three quick steps!](http://www.kiji.org/#tryit)

If you have downloaded the standalone Kiji BentoBox, the code for this tutorial
is already compiled and located in the `${KIJI_HOME}/examples/express-music/` directory.
Commands in this tutorial will depend on this location:

<div class="userinput">
{% highlight bash %}
export MUSIC_EXPRESS_HOME=${KIJI_HOME}/examples/express-music
{% endhighlight %}
</div>

If you are not using the Kiji BentoBox, set `MUSIC_EXPRESS_HOME` to the path of your local
kiji-music repository.

Once you have done this, if you are using Kiji BentoBox you can skip to
"Set your environment variables" if you want to get started playing with the example code.
Otherwise, follow these steps to compile it from source.

### Compiling the Tutorial (Optional)

The source is included along with a Maven project. Starting a Maven project that uses Kiji?
[Read the Maven setup instructions.]({{site.kiji_url}}/get-started-with-maven) or
the [Apache Maven Homepage](http://maven.apache.org/).

The following tools are required to compile this project:
* Maven 3.x
* Java 6

To compile the tutorial yourself:
{% highlight bash %}
cd ${MUSIC_EXPRESS_HOME}
mvn package
{% endhighlight %}

The build artifacts (.jar files) will be placed in the `$MUSIC_EXPRESS_HOME/target/`
directory. This tutorial assumes you are using the pre-built jars included with
the music recommendation example under `$MUSIC_EXPRESS_HOME/lib/`. If you wish to
use jars of example code that you have built, you should adjust the command
lines in this tutorial to use the jars in `$MUSIC_EXPRESS_HOME/target/`.

### Start a Bento cluster

If you already installed BentoBox, make sure you have started it:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/kiji-env.sh
bento start
{% endhighlight %}
</div>

### Set Environment Variables

After Bento starts, it will display ports you will need to complete this tutorial. It will be useful
to know the address of the MapReduce JobTracker webapp
([http://localhost:50030](http://localhost:50030) by default) while working through this tutorial.

It will be useful to define an environment variable named `KIJI` that holds a Kiji URI to the Kiji
instance we'll use during this tutorial.

<div class="userinput">
{% highlight bash %}
export KIJI=kiji://.env/kiji_express_music
{% endhighlight %}
</div>

To work through this tutorial, various Kiji tools will require that Avro data
type definitions particular to the working music recommendation example be on the
classpath. You can add your artifacts to the Kiji classpath by running:

<div class="userinput">
{% highlight bash %}
export KIJI_CLASSPATH="${MUSIC_EXPRESS_HOME}/lib/*"
{% endhighlight %}
</div>


### Install Kiji and Create Tables

Install your Kiji instance:

<div class="userinput">
{% highlight bash %}
kiji install --kiji=${KIJI}
{% endhighlight %}
</div>

Create the Kiji music tables that have layouts described in `music-schema.ddl`.

<div class="userinput">
{% highlight bash %}
kiji-schema-shell --kiji=${KIJI} --file=${MUSIC_EXPRESS_HOME}/music-schema.ddl
{% endhighlight %}
</div>

This command uses [kiji-schema-shell](https://github.com/kijiproject/kiji-schema-shell)
to create the tables using the KijiSchema DDL, which makes specifying table layouts easy.
See [the KijiSchema DDL Shell reference]({{site.userguide_schema_1_0_2}}/schema-shell-ddl-ref)
for more information on the KijiSchema DDL.

### Upload Data to HDFS

Upload the data set to HDFS:

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir express-tutorial
hadoop fs -copyFromLocal ${MUSIC_EXPRESS_HOME}/example_data/*.json express-tutorial/
{% endhighlight %}
</div>
