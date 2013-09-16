---
layout: post
title: KijiExpress Language
categories: [tutorials, express-recommendation, devel]
tags: [express-music]
order: 2
description: Short introduction to the KijiExpress language.
---
This section introduces Scala and the KijiExpress language. It does not have any tutorial steps.
If you are already on board with Scala and the Scalding library for data processing,
skip ahead to [Setup](../express-setup/);
you can also refer back to the [summary](#summary) at the end of this page.



KijiExpress is built on top of Twitter's [Scalding](http://github.com/twitter/scalding/wiki). Scalding is
a powerful [Scala](http://www.scala-lang.org) library that can be used to process collections of data
using MapReduce. It uses [Cascading](http://www.cascading.org), which is a Java library that provides an abstraction over
MapReduce and gives us data flow control. (Scala + Cascading = "Scalding")

![Scala-Scalding-Cascading-Hadoop-Stack][scala-context]

[scala-context]: ../../../../assets/images/scala-context.png

### Tuples and Pipelines

KijiExpress views a data set as a collection of _named tuples_.  A named tuple can be thought of as
an ordered list where each element has a name. When using KijiExpress with data stored in a Kiji table,
a row from the Kiji table corresponds to a single tuple, where columns from the Kiji table correspond
to named fields in the tuple. KijiExpress provides a view of a
Kiji table as a collection of tuples by viewing each row from the table as a tuple.

By viewing a data set as a collection of named tuples, KijiExpress (through Scalding) allows you
to transform your data using common functional operations.

With Scalding, data processing occurs in _pipelines_, where the input and output
from each pipeline is a stream of named tuples represented by a data object. Each operation you
described in your KijiExpress program, or _job_, defines the input, output, and processing for a _pipe_.

### Jobs

Scala - the language you'll write KijiExpress jobs in - is object-oriented, so while
functions are called with the familiar syntax `function(object)`, there are sometimes
_methods_ defined on _objects_, where the syntax of using that method on the object is
`object.function(parameters)`.

For example, the line

{% highlight scala %}
val userData = userDataInput.project('username, 'stateId, 'totalSpent)
{% endhighlight %}

is calling the `project` method on the object `userDataInput`, with the arguments `'username`,
`'stateId`, and `'totalSpent`.  The result of this is another object, called `userData`.

KijiExpress allows users to access tuple fields by using a ' (single quote) to name the field.
The function above operates
on the `username`, `stateId`, and `totalSpent` fields by including the `'username`, `'stateId`,
and `'totalSpent` symbols in the first parameter group.

When writing KijiExpress jobs, your methods will often take a first
argument group in parentheses that specifies a mapping from input field names to output field
names.  You can then define a function in curly braces `{}` immediately following that defines how
to map from the input fields to the output fields. The syntax looks like this:

{% highlight scala %}
input.method ('input-field -> 'output-field) {x => function(x) }
{% endhighlight %}

For example, in the line:

{% highlight scala %}
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }
{% endhighlight %}

We call the `map` method on `userDataInput`, from the input field `line` to the output fields
`username`, `stateId`, and `totalSpent`. Remember that fields are marked with the single quote.
Then to indicate how to map the input to output, we pass the function
`{ line: String => (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }` as another argument.
This function returns a 3-tuple; the elements of the output tuple are used to populate the
output fields `username`, `stateId`, and `totalSpent`.
When using the
`map` method, this function is called on the `line` field to populate the `username`,
`stateId`, and `totalSpent` fields.

### A Simple Example Job

For a demonstration of some common methods on pipes, consider this simple KijiExpress job.  At
each step, fields in the tuple can be created and operated on.

This script reads customer data from "user-file.txt" and cleans it up. It keeps only users who have
spent more than $2, cleans up the data by joining it with side data in "state-names.txt", counts
the number of spendy users by state, and writes the result of that to an output file.

This script expects two files in the directory you run it from:
  1. "user-file.txt" which contains user data in the form "username stateID totalSpent" on each
      line.
  2. "state-names.txt" which contains a mapping from state IDs to state names in the form
      "numericalID stateName" on each line, for example "1 California".

A detailed description of each part follows the script.

{% highlight scala %}

// Read data from a text file.
val input = TextLine("user-file.txt")

// Split each line on spaces into the fields username, stateId, and totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }

// Only keep the username, stateId, and totalSpent fields.
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

// Join the pipelines on the field stateId from "importantCustomerData" and numericalStateId
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

First, we read our input with `TextLine`, which is a predefined Scalding "source" that reads lines of
text from a file.
`TextLine` views a file (in this case the file `sideData.txt` in HDFS) as a collection of tuples
with one tuple corresponding to each line of text.  Each tuple has a field named `line` that
contains a line of text read from the file.  Although unused here, the tuples also contain a field
named `offset` that holds the byte offset in the file where the line read appears.

Once we have a view of the data set as a collection of tuples, we can use different operations to
derive results that can be stored in new tuple fields.

#### Map

{% highlight scala %}
// Split each line on spaces into the fields username, stateId, and totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }
{% endhighlight %}

After this line, `userDataInput` contains the fields `line`, `username`, `stateId`, and `totalSpent`.
Notice that doing a `map` operation on `input` keeps the field `line` around, and adds the
`username`, `stateId`, and `totalSpent` fields. You can think of `userData` as a collection of named
tuples, where each has 4 fields.

#### Project

{% highlight scala %}
// Only keep the username, stateId, and totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)
{% endhighlight %}

We no longer need the `line` field.  The `project` method projects the tuples onto the specified fields,
discarding any unspecified fields. `userData` contains the same tuples as `userDataInput`, but
without the `line` and `offset` fields that `TextLine` provided.

#### Filter

{% highlight scala %}
// Keep only the customers who spent more than $2.00.
val importantCustomerData = userData.filter('totalSpent) { totalSpent: String =>
    totalSpent.toDouble > 2.0 }
{% endhighlight %}

After this line, "importantCustomerData" can be thought of as a collection of named tuples,
where each has the same 3 fields as "userData" does: `username`, `stateId`, and `totalSpent`.  The
difference is that not all the tuples from "userData" are included: only the ones for which the
function we provide to the "filter" operation evaluates to "true" are included.  So,
"importantCustomerData" includes only the data for the users who have spent more than 2 dollars on
our service.

#### Join

{% highlight scala %}
// Create a new pipeline containing state ID to state name mappings.
val sideData = TextLine("state-names.txt")
    .map('line -> ('numericalStateId, 'stateName)) { line: String =>
        // Split the line up on the spaces, and create a tuple with the first and second words in
        // the line.
        (line.split(" ")(0), line.split(" ")(1))
    }

// Join the pipelines on the field 'stateId from "importantCustomerData" and 'numericalStateId
// from "sideData".
val importantCustomerDataWithStateNames =
      importantCustomerData.joinWithSmaller('stateId -> 'numericalStateId, sideData)
{% endhighlight %}

First we define the pipeline to join with. "sideData" is a pipe containing tuples with fields
"line", "numericalStateId", and "stateName". You've seen `TextLine` and `.map` before.
Notice that instead of defining all the intermediate values as we have been, you can just chain calls
such as "`.map`" on pipes, so that your pipeline can look like
`TextLine(inputfile).map(...).filter(...)`.

We now join our main pipeline with the sideData pipe.  When we join two pipelines, we specify which field
from the main pipeline (in this case, the field "stateId") should be joined with which field from the side
pipeline (the field "numericalStateId" from the "sideData").  For
all tuples in the main pipeline, we've added all the fields from "sideData", in this case a single field
`stateName`.

Since "sideData" is smaller than "importantCustomerData" (sideData contains only 50 tuples, one for each state in the United
States, while "importantCustomerData" could be very big), we use the "joinWithSmaller"
operation on "importantCustomerData".  This lets Scalding optimize the MapReduce jobs.

#### Group by

{% highlight scala %}
// Group by the states customers are from and compute the size of each group.
val importantCustomersPerState =
    importantCustomerDataWithStateNames.groupBy('stateName) { group =>
        group.size('customersPerState) }
{% endhighlight %}

This step groups the tuples from the previous step by their stateName, and for each group,
puts the size of the group in a new field called "customersPerState".

#### Output

{% highlight scala %}
// Output to a file in tab-separated form.
importantCustomersPerState.write(Tsv("important-customers-by-state.txt"))
{% endhighlight %}

`Tsv` is one of the predefined Scalding sources. It writes the tuples out to a
file in tab-separated form.
KijiExpress provides sources to read
from and write to Kiji tables, which you will see later in the tutorial.

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

### Scala Quick Reference<a id="summary"> </a>

To summarize the Scala phrases that were critical to the basic job:

+ **Indicate Fields**

Precede field names with a single quote:

{% highlight scala %}
<object>.map(('<input-field>, '<input-field> ...) -> ('<mapped-field>, '<mapped-field>, ..))
{% endhighlight %}

+ **Input From File**

{% highlight scala %}
val <variable-name> = TextLine("<filename>")
{% endhighlight %}

+ **Map**

    Include the input and output fields.

{% highlight scala %}
val <variable-name> = <object>.map('<input-field> -> ('<output-field1>, '<output-field2>, ...)) { <map function> }
{% endhighlight %}

    Include only the output fields:

{% highlight scala %}
val <variable-name> = <object>.mapTo('<input-field> -> ('<output-field1>, '<output-field2>, ...)) { <map function> }
{% endhighlight %}

+ **Split Tuple at Blanks**

{% highlight scala %}
{ <object>: String => (<object>.split(" ")(0), <object>.split(" ")(1)) }
{% endhighlight %}

+ **Project**
{% highlight scala %}
val <variable-name> = <object>.project('<field1>, '<field2>, ...)
{% endhighlight %}

+ **Filter**

{% highlight scala %}
val <variable-name> = <object>.filter('<field>, '<field>, ...) { function }
{% endhighlight %}

+ **Join**

    In addition, there are methods `joinWithLarger` and `joinWithTiny`. See [Scalding Join
Operations](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference#wiki-join-functions).

{% highlight scala %}
val <variable-name> = <object>.joinWithSmaller('<field-from-this-data-set> -> '<field-from-other-data-set>, <other-data-set>)
{% endhighlight %}

+ **Group By**

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { <group function> }
{% endhighlight %}

+ **Group By Value**

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { x => x }
{% endhighlight %}

+ **Calculate Size**

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { <group> => <group>.size('<field>) }
{% endhighlight %}

+ **Output TSV**

For other sources in addition to Tsv, see [Scalding Sources](https://github.com/twitter/scalding/wiki/Scalding-Sources).
{% highlight scala %}
<object>.write(Tsv("<filename>"))
{% endhighlight %}


### Scalding Resources
There are many resources available to learn more about the Scalding library.

* [The Fields Based API
  Reference](http://github.com/twitter/scalding/wiki/Fields-based-API-Reference) contains details on
  the operations you can use with Scalding to derive results and transform collections of tuples.
* [The Scalding Github Page](http://github.com/twitter/scalding) contains a copy of the project,
  including an informative README file.
* [The Scalding Wiki](http://github.com/twitter/scalding/wiki) contains links to many resources
  about Scalding, including [Scalding Sources](https://github.com/twitter/scalding/wiki/Scalding-Sources)
* [Scala Functions and Methods](http://jim-mcbeath.blogspot.com/2009/05/scala-functions-vs-methods.html) describes
  how Scala distinguishes functions and methods, which might help you understand what's behind some
  of Scala's syntax choices.
