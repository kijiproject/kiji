---
layout: post
title: Overview
categories: [tutorials, express-recommendation, 0.1.0]
tags: [express-music]
order: 1
description: A tutorial to get you using KijiExpress with Kiji Tables.
---

Analyzing data with MapReduce can be a long path, fraught with many Java classes.

KijiExpress is designed to make defining data processing jobs faster and more
expressive, particularly for data stored in Kiji tables. KijiExpress allows you
to define pipelines of MapReduce jobs easily, particularly when dealing with Kiji tables.


In this tutorial, we demonstrate how to use KijiExpress to analyze your data effectively. You will:

* Quickly and efficiently import data into a KijiTable.
* Define a KijiExpress pipeline that reads from a KijiTable and counts occurances of an event.
* Run KijiExpress jobs locally and verify the output for jobs.
* Work with complex Avro types in pipelines.

The tutorial gets you started with the beginnings of a music recommendation engine. The input is in
the form of JSON files that contain metadata about songs and users' listening history. We import this
data into Kiji tables.

We start by writing a simple program to the number of times a song is played.

We then show how to calculate the most popular song played after a given song. We do this by
splitting the users' listening history into bigrams of (song1, song2), where song2 was played right after
song1. We count these bigrams and for a given song, output a set of songs that followed this one, sorted
by the number of times the bigram appeared.

### How to Use this Tutorial

* **Code Walkthrough** - Code snippets are in gray boxes with language specific syntax highlighting.

{% highlight scala %}
println("Hello Kiji")
{% endhighlight %}

* **Shell Commands** - Shell commands to run the above code will be in light blue boxes, and the results in grey.

<div class="userinput">
{% highlight bash %}
echo "Hello Kiji"
{% endhighlight %}
</div>

    Hello Kiji

You can run KijiExpress on compiled jobs or uncompiled scripts. This tutorial will focus on running
compiled jobs, but runnable scripts that do the same work as the compiled classes will also be available.

