---
layout: post
title: Setup
categories: [tutorials, phonebook-tutorial, 1.1.5]
tags: [phonebook]
order: 2
description: Setup and compile instructions for the phonebook tutorial.
---

For this tutorial, we assume you are either using the Kiji Standalone BentoBox or
have installed the individual components described [here](http://www.kiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the Kiji
Standalone Bento box in [three quick steps!](http://www.kiji.org/#tryit).

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
* Java 6

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

### Install Kiji Instance
<a name="ref.install_kiji_instance" id="ref.install_kiji_instance"> </a>

Install your Kiji instance. Running `kiji install` with no `--kiji` flag installs the default instance:

<div class="userinput">
{% highlight bash %}
kiji install
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

