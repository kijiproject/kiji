---
layout: post
title: Setup
category: tutorial
tags: [article]
order: 2
description: Setup and compile instructions for the phonebook tutorial.
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
is already compiled and located in the `$KIJI_HOME/examples/phonebook/lib/` directory.
You can skip to [Create a Table]({{site.tutorial_url}}phonebook-create)
if you want to get started playing with the example code.


The source code for this tutorial can be found in `$KIJI_HOME/examples/phonebook`.
The source is included along with a Maven project. To get started using Maven,
consult [Getting started With Maven]({{site.kiji_url}}getting-started-with-maven) or
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

