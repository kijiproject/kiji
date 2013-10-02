---
layout: post
title: Overview
categories: [tutorials, express-recommendation, 0.8.0]
tags: [express-music]
order: 1
description: A tutorial to get you using KijiExpress with Kiji Tables.
---

KijiExpress is a modeling environment designed to make defining data processing MapReduce
jobs quick and expressive, particularly for data stored in Kiji tables. KijiExpress jobs
are written in the Scala programming language, which gives you access to Java libraries and
tools but is more concise and easier to write. KijiExpress gives you access to functionality for
building predictive models by including the Scalding library, a Twitter sponsored open-source library
for authoring flows of analytics-focused MapReduce jobs.
KijiExpress is integrated with Avro to give you access to complex records in your data
transformation pipelines.

In this tutorial, we demonstrate how to use KijiExpress to analyze your data effectively. You will:

* Run Kiji and create tables in HBase, the underlying data store.
* Quickly and efficiently import data into a Kiji table.
* Define a KijiExpress pipeline that reads from a Kiji table and counts occurrences of an event.
* Run your KijiExpress job locally and verify the output for jobs.
* Use KijiExpress to make recommendations based on users' past behavior.

The tutorial gets you started with the beginnings of a music recommendation engine. The input is in
the form of JSON files that contain metadata about songs and users' listening history. You will import this
data into Kiji tables. Then you'll put this material to use
by writing a simple program to count the number of times a song is played. The tutorial
goes on to show how to calculate a song recommendation from the most popular song played
after a given song.

### How to Use this Tutorial

* **Code Walkthrough** - Code snippets are in gray boxes with language-specific syntax highlighting.

{% highlight scala %}
println("Hello Kiji")
{% endhighlight %}

* **Shell Commands** - Shell commands to run the above code will be in light blue boxes, and the results in grey.
All of this text is literal unless it appears in angle brackets.

<div class="userinput">
{% highlight bash %}
echo "Hello Kiji"
{% endhighlight %}
</div>

    Hello Kiji

