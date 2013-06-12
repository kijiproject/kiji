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
[Scalding](http://github.com/twitter/scalding), which is written in Scala and supports calling
arbitrary Scala or Java code.


In this tutorial, we demonstrate how to use KijiExpress to analyze your data effectively. You will:

* Quickly and efficiently import data into a KijiTable.
* Define a KijiExpress pipeline that reads from a KijiTable and counts occurances of an event.
* Run KijiExpress jobs locally and verify the output for jobs.
* Work with complex Avro types in pipelines.

The tutorial gets you started with the beginnings of a music recommendation engine. The input is in
the form of JSON files that contain metadata about songs and users' listening history. We import this
data into Kiji tables.

We start by writing a simple program to count the number of times a song is played.

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

### A brief introduction to the KijiExpress language

KijiExpress is built on top of Twitter's [Scalding](http://github.com/twitter/scalding). Scalding is
a powerful Scala library that can be used to process collections of data using MapReduce.

KijiExpress views a data set as a collection of _named tuples_.  A named tuple can be thought of as
an ordered list where each element has a name. Generally, a single tuple corresponds to a single
record from a data set. In Kiji, a single record corresponds to a single row in a Kiji table, which
is all of the information about a specific entity.  Each element of data in the record is a field in
the tuple that can be addressed by a specific user-supplied name. KijiExpress provides a view of a
Kiji table as a collection of tuples by viewing each row from the table as a tuple.

By viewing a data set as a collection of named tuples, Scalding allows users to transform their data
sets using common functional operations.

KijiExpress jobs are written in the Scala programming language.  Scala is object-oriented, so while
functions are called with the familiar mathematical syntax `function(object)`, there are sometimes
_methods_ defined on _objects_, where the syntax of using that method on the object is
`object.function(parameters)`.

For example, the line

{% highlight scala %}
// Only keep the 'username, 'stateId, and 'totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)
{% endhighlight %}

is calling the `project` method on the object `userDataInput`, with the arguments `'username`,
`'stateId`, and `'totalSpent`.  The result of this is another object, called `userData`.

KijiExpress allows users to access fields by using a ' (single quote). The function above operates
on the `'username`, `'stateId`, and `'totalSpent` fields by including the `'username`, `'stateId`,
and `'totalSpent` symbols in the first parameter group.

When writing KijiExpress jobs, you will often use methods in your pipelines that take a first
argument group in `()` parentheses that specifies a mapping from input field names to output field
names.  You can then define a function in `{}` curly braces immediately following that defines how
to map from the input fields to the output fields. The syntax looks like this:
`input.method ('inputfield -> 'outputfield) {x => function(x) }`

For example, in the line

{% highlight scala %}
// Split each line on spaces into the fields 'username, 'stateId, and 'totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }
{% endhighlight %}

We call the `map` method on `userDataInput`, from the input field `'line` to the output fields
`'username`, `'stateId`, and `'totalSpent`, then pass in the function `{ line: String =>
(line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }` as another argument.  When using the
`map` method, this function is what is called on the `'line` field to populate the `'username`,
`'stateId`, and `'totalSpent` fields.

### An example

For a demonstration of some common methods on pipes, consider this simple KijiExpress flow.  At
each step, fields in the tuple can be created and operated on.

This script reads user data from "user-file.txt" and cleans it up. It keeps only users who have
spent more than $2, cleans up the data by joining it with side data in "state-names.txt", counts
the number of spendy users by state, and writes the result of that to an output file.

This script that expects two files in the directory you run it from:
  1. "user-file.txt" which contains user data in the form "username stateID totalSpent" on each
      line.
  2. "state-names.txt" which contains a mapping from state IDs to state names in the form
      "numericalID stateName" on each line, for example "1 California".

{% highlight scala %}

// Read data from a text file.
val input = TextLine("user-file.txt")

// Split each line on spaces into the fields 'username, 'stateId, and 'totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }

// Only keep the 'username, 'stateId, and 'totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)

// Keep only the customers who spent more than $2.00.
val importantCustomerData = userData.filter('totalSpent) { totalSpent: String =>
    totalSpent.toDouble > 2.0 }

// Create a new pipeline containing state ID to state name mappings.
val sideData = TextLine("state-names.txt")
    .map('line -> ('numericalStateId, 'stateName)) { line: String =>
        // Split the line up on the spaces, and create a tuple with the first and second words in
        // the line.
        (line.split(" ")(0), line.split(" ")(1))
    }
// Join the pipelines on the field 'stateId from "importantCustomerData" and 'numericalSTateId
// from "sideData".
val importantCustomerDataWithStateNames =
      importantCustomerData.joinWithSmaller('stateId -> 'numericalStateId, sideData)

// Group by the states customers are from and compute the size of each group.
val importantCustomersPerState =
    importantCustomerDataWithStateNames.groupBy('stateName) { group =>
        group.size('customersPerState) }

// Output to a file in tab-separated form.
importantCustomersPerState.write(Tsv("important-customers-by-state.txt"))
{% endhighlight %}

#### Input

{% highlight scala %}
// Read data from a text file.
val input = TextLine("user-file.txt")
{% endhighlight %}

First, we read our input with `TextLine`, which is one of the input formats predefined by Scalding.
`TextLine` views a file (in this case the file `sideData.txt` in HDFS) as a collection of tuples
with one tuple corresponding to each line of text.  Each tuple has a field named `'line` that
contains a line of text read from the file.  Although unused here, the tuples also contain a field
named `'offset` that holds the byte offset in the file where the line read appears.

Once we have a view of the data set as a collection of tuples, we can use different operations to
derive results that can be stored in new tuple fields.

#### Map

{% highlight scala %}
// Split each line on spaces into the fields 'username, 'stateId, and 'totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }
{% endhighlight %}

After this line, `userDataInput` contains the fields 'line, 'username, 'stateId, and 'totalSpent.
Notice that doing a `map` operation on `input` keeps the field `'line` around, and adds the
'username, 'stateId, and 'totalSpent fields. You can think of `userData` as a collection of named
tuples, where each has 4 fields.

#### Project

{% highlight scala %}
// Only keep the 'username, 'stateId, and 'totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)
{% endhighlight %}

We no longer need the `'line` field.  `project` projects the tuples onto the specified fields,
discarding any unspecified fields. `userData` contains the same tuples as `userDataInput`, but
without the `'line` and `'offset` fields that `TextLine` provided field.

#### Filter

{% highlight scala %}
// Keep only the customers who spent more than $2.00.
val importantCustomerData = userData.filter('totalSpent) { totalSpent: String =>
    totalSpent.toDouble > 2.0 }
{% endhighlight %}

After this line, "importantCustomerData" can be thought of as a collection of named tuples,
where each has the same 4 fields as "userData" does: 'line, 'username, 'stateId, and
'totalSpent.  The difference is that not all the tuples from "userData" are included: only the
ones for which the function we provide to the "filter" operation evaluates to "true" are
included.  So, "importantCustomerData" includes only the data for the users who have spent
more than 2 dollars on our service.

#### Join

{% highlight scala %}
// Create a new pipeline containing state ID to state name mappings.
val sideData = TextLine("state-names.txt")
    .map('line -> ('numericalStateId, 'stateName)) { line: String =>
        // Split the line up on the spaces, and create a tuple with the first and second words in
        // the line.
        (line.split(" ")(0), line.split(" ")(1))
    }
// Join the pipelines on the field 'stateId from "importantCustomerData" and 'numericalSTateId
// from "sideData".
val importantCustomerDataWithStateNames =
      importantCustomerData.joinWithSmaller('stateId -> 'numericalStateId, sideData)
{% endhighlight %}

First we define the pipeline to join with. "sideData" is a pipe containing tuples with fields
"'line", "'numericalStateId", and "'stateName". You've seen "TextLine" and ".map" before.
Notice instead of defining all the intermediate values as we have been, you can just chain calls
such as ".map" on pipes, so that your pipeline can look like
"TextLine(inputfile).map(...).filter(...)".

The join our main pipeline with the side data.  When we join two pipelines, we specify which field
from this pipeline (in this case, the field "'stateId") should be joined with which field from which
other pipeline (the field "'numericalStateId", from the pipeline "sideData").  This means that for
all tuples in this pipeline, we've added all the fields from 'sideData, in this case a single field
"'stateName".  The value of "'stateName" for each tuple is the value of "'stateName" corresponding
to the "'numericalStateId" that is the same as the "'stateId" for any particular tuple.

Since "sideData" is smaller (it contains only 50 tuples, one for each state in the United
States, while "importantCustomerData" could be very big), we use the "joinWithSmaller"
operation on "importantCustomerData".  This lets Scalding optimize the mapreduce jobs.

#### Group by

{% highlight scala %}
// Group by the states customers are from and compute the size of each group.
val importantCustomersPerState =
    importantCustomerDataWithStateNames.groupBy('stateName) { group =>
        group.size('customersPerState) }
{% endhighlight %}

This step groups the tuples from the previous step by their 'stateName, and for each group,
puts the size of the group in a new field called "'customersPerState".

#### Output

{% highlight scala %}
// Output to a file in tab-separated form.
importantCustomersPerState.write(Tsv("important-customers-by-state.txt"))
{% endhighlight %}

Tsv is one of the pre-written output formats from Scalding, which writes the tuples out to a
file in tab-separated form.  See [here](https://github.com/twitter/scalding/wiki/Scalding-Sources)
for other input and output formats that Scalding provides.  KijiExpress provides sources to read
from and write to Kiji tables, which you will see in the rest of the tutorial.

#### Results

If you run this script where the contents of "user-file.txt" are:
{% highlight scala %}
daisy 1 3
robert 4 0
kiyan 2 5
juliet 1 4
renuka 2 2
{% endhighlight %}

and the contents of "state-names.txt" are:
{% highlight scala %}
1 California
2 Washington
{% endhighlight %}

Then the output in the file `important-customers-by-state.txt` is:
{% highlight scala %}
California  2
Washington  1
{% endhighlight %}

Which shows that there are 2 customers in California who spent more than $2.00, and 1 in Washington
who spent more than $2.00.


### Scalding Resources
There are many resources available to learn more about the Scalding library.

* [The Fields Based API
  Reference](http://github.com/twitter/scalding/wiki/Fields-based-API-Reference) contains details on
  the operations you can use with Scalding to derive results and transform collections of tuples.
* [The Scalding Github Page](http://github.com/twitter/scalding) contains a copy of the project,
  including an informative README file.
* [The Scalding Wiki](http://github.com/twitter/scalding/wiki) contains links to many resources
  about Scalding.
