---
layout: post
title: Overview
categories: [tutorials, express-recommendation, DEVEL]
tags: [express-music]
order: 1
description: A tutorial to get you using KijiExpress with Kiji Tables.
---

KijiExpress is designed to make defining data processing MapReduce jobs quick and expressive,
particularly for data stored in Kiji tables. Models can be developed using
[Scalding](http://github.com/twitter/scalding), in the easy-to-use and powerful language of Scala,
and run over Kiji tables using KijiExpress.


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

### Quick Scala and Scalding Syntax to Get Started

#### Scalding and the Tuple Model

Scalding (and KijiExpress) view a data set as a collection of _named tuples_.  A tuple can be
thought of as an ordered list; a named tuple is an ordered list where each element has a name.
Generally, a single tuple corresponds to a single record from a data set. Each element of data in
the record is a field in the tuple that can be addressed by a specific user-supplied name.

By viewing a data set as a collection of named tuples, Scalding allows users to transform their data
sets using common functional operations. For example, consider this simple Scalding flow.

{% highlight scala %}
TextLine("linesOfText")
  .map('line -> 'length) { line: String => line.length }
  .write(Tsv("linesAndLengths.tsv"))
{% endhighlight %}

We'll explain flows like this in more detail in the coming sections. For now, we'll focus on how
data is viewed and used with the tuple model. The first line, `TextLine("linesOfText.txt")` produces
a Scalding `Source`. You can think of a `Source` as a view of a data set as a collection of tuples.
In this case, `TextLine` views a file (in this case the file `linesOfText.txt` in HDFS) as a
collection of tuples with one tuple corresponding to each line of text. Each tuple has a field named
`'line` that contains a line of text read from the file. Although unused here, the tuples also
contain a field named `'offset` that holds the byte offset in the file where the line read appears.

Once we have a view of the data set as a collection of tuples, we can use different operations to
derive results that can be stored in new tuple fields. Consider the call to `map` above. A `map`
operation is used to derive a new tuple field from existing tuple fields. The call above indicates
that the value of the existing tuple field `'line` should be passed to the function `line: String =>
line.length`. The result returned by that function (the length of the line of text) is then stored
in the new tuple field `'length`. After the call to `map` above executes, each tuple will now
contain a field named `'line` (still containing the line of text) and a field named `'length` (now
containing the length of the line of text).

KijiExpress provides a view of a Kiji table as a collection of tuples by viewing each row from the
table as a tuple. More details will come in subsequent sections.
KijiExpress is built on top of Twitter's [Scalding](http://github.com/twitter/scalding). Scalding is
a powerful Scala library that can be used to process collections of data using MapReduce.

#### Scala syntax

KijiExpress (and Scalding) jobs are written in the Scala programming language.  Scala is
object-oriented, so while functions are called with the familiar mathematical syntax
`function(object)`, there are sometimes _methods_ defined on _objects_, where the syntax of using
that method on the object is `object.function()`.

Functions can take other functions as parameters.  Anonymous functions can be defined inline with
the notation `input => computeStuff(input)`.

When writing KijiExpress jobs, you will often use methods in your pipelines that take a first
argument group in `()` parentheses that specifices a mapping from input field names to output field
names.  You can then define a function in `{}` curly braces immediately following that defines how
to map from the input fields to the output fields. The syntax looks like this:
`input.method ('inputfield -> 'outputfield) {x => function(x) }`

### Scalding Resources
There are many resources available to learn more about the Scalding library.

* [The Fields Based API
  Reference](http://github.com/twitter/scalding/wiki/Fields-based-API-Reference) contains details on
  the operations you can use with Scalding to derive results and transform collections of tuples.
* [The Scalding Github Page](http://github.com/twitter/scalding) contains a copy of the project,
  including an informative README file.
* [The Scalding Wiki](http://github.com/twitter/scalding/wiki) contains links to many resources
  about Scalding.
