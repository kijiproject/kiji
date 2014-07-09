---
layout: post
title: Example KijiExpress Job
categories: [userguides, express, 2.0.4]
tags : [express-ug]
version: 2.0.4
order : 5
description: Example KijiExpress Job.
---

## A Simple Example Job

For a demonstration of some common methods on pipes, consider this simple KijiExpress job.  At
each step, fields in the tuple can be created and operated on.

This script reads customer data from "user-file.txt" and cleans it up. It keeps only users who have
spent more than $2, cleans up the data by joining it with side data in "state-names.txt", counts
the number of spendy users by state, and writes the result of that to an output file.

This script expects two files in the directory you run it from:

1.  "user-file.txt," which contains user data in the form "username stateID totalSpent" on each
    line.
2.  "state-names.txt" which contains a mapping from state IDs to state names in the form
    "numericalID stateName" on each line, for example "1 California".

A detailed description of each part follows the script.

{% highlight scala %}

// Read data from a text file.
val input = TextLine("user-file.txt").read

// Split each line on spaces into the fields username, stateId, and totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }

// Keep only the username, stateId, and totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)

// Keep only the customers who spent more than $2.00.
val importantCustomerData = userData
    .filter('totalSpent) { totalSpent: String => totalSpent.toDouble > 2.0 }

// Create a new pipeline containing state ID to state name mappings.
val sideData = TextLine("state-names.txt").read
    .map('line -> ('numericalStateId, 'stateName)) { line: String =>
        // Split the line up on the spaces, and create a tuple with the first and second words in
        // the line.
        (line.split(" ")(0), line.split(" ")(1))
    }

// Join the pipelines on the field stateId from "importantCustomerData" and numericalStateId
// from "sideData".
val importantCustomerDataWithStateNames = importantCustomerData
    .joinWithSmaller('stateId -> 'numericalStateId, sideData)
    // Keepy only the userId and stateId fields
    .project('userId, 'stateId)

// Group by the states customers are from and compute the size of each group.
val importantCustomersPerState = importantCustomerDataWithStateNames
    .groupBy('stateName) { group => group.size('customersPerState) }

// Output to a file in tab-separated form.
importantCustomersPerState.write(Tsv("important-customers-by-state.txt"))
{% endhighlight %}

See the [results](#Results) section for the expected results, or continue reading for a detailed
description of the steps in this pipeline.

### Input

{% highlight scala %}
// Read data from a text file.
val input = TextLine("user-file.txt").read
{% endhighlight %}

First, we read our input with `TextLine`, which is a predefined Scalding [Source](https://github.com/twitter/scalding/wiki/Scalding-Sources) that reads lines
of text from a file.  `TextLine` views a file (in this case the file `user-file.txt` in HDFS) as a
collection of tuples, one per line of text.  Each tuple has a field named `line`, which contains the
corresponding line of text read from the file, as well as a field named `offset` (not used here),
which holds the byte offset of the line within the file.

Once we have a view of the data set as a collection of tuples, we can use different operations to
derive results that we can store in new tuple fields.

### Map

{% highlight scala %}
// Split each line on spaces into the fields username, stateId, and totalSpent.
val userDataInput = input.map('line -> ('username, 'stateId, 'totalSpent)) { line: String =>
    // Split the line up on the spaces, and create a tuple with the first, second, and third words
    // in the line, in that order.
    (line.split(" ")(0), line.split(" ")(1), line.split(" ")(2)) }
{% endhighlight %}

This statement creates `userDataInput`, which contains the fields `line`, `offset`, `username`,
`stateId`, and `totalSpent`.  Notice that doing a `map` operation on `input` keeps the fields `line`
and `offset` around, and adds the `username`, `stateId`, and `totalSpent` fields.

### Project

{% highlight scala %}
// Keep only the username, stateId, and totalSpent fields.
val userData = userDataInput.project('username, 'stateId, 'totalSpent)
{% endhighlight %}

We no longer need the `line` or `offset` fields.  The `project` method projects
the tuples onto the specified fields, discarding any unspecified fields.
`userData` contains the same tuples as `userDataInput`, but without the `line`
and `offset` fields that `TextLine` provided.

### Filter

{% highlight scala %}
// Keep only the customers who spent more than $2.00.
val importantCustomerData = userData
    .filter('totalSpent) { totalSpent: String => totalSpent.toDouble > 2.0 }
{% endhighlight %}

This statement creates `importantCustomerData`, a stream of named tuples, each of which has the same
three fields as `userData` does: `username`, `stateId`, and `totalSpent`.  `importantCustomerData`,
however, contains only the tuples from `userData` for which the function we provide to the `filter`
operation evaluates to `true`, e.g., users who have spent more than two dollars on our service.

### Join

{% highlight scala %}
// Create a new pipeline containing state ID to state name mappings.
val sideData = TextLine("state-names.txt").read
    .map('line -> ('numericalStateId, 'stateName)) { line: String =>
        // Split the line up on the spaces, and create a tuple with the first and second words in
        // the line.
        (line.split(" ")(0), line.split(" ")(1))
    }

// Join the pipelines on the field 'stateId from "importantCustomerData" and 'numericalStateId
// from "sideData".
val importantCustomerDataWithStateNames = importantCustomerData
    .joinWithSmaller('stateId -> 'numericalStateId, sideData)
    // Keepy only the userId and stateId fields
    .project('userId, 'stateId)
{% endhighlight %}

In this step, we perform a join operation to add state names to `importantCustomerData.` First we
define the pipe, `sideData`, with which to join `importantCustomerData.` `sideData` contains tuples
with fields `line`, `numericalStateId`, and `stateName` (as well as `offset`). You've seen
`TextLine` and `.map` before.  Notice that we have chained calls such as `.map` on pipes, creating a
pipeline that looks like `TextLine(inputfile).read.map(...)`.

We now join our main pipeline with the sideData pipe, specifying the fields (in this case, the field
`stateId` from `importantCustomerData` and `numericalStateId` from `sideData`.  The join operation
adds all of the fields (`line`, `offset`, `numericalStateId`, and `stateName`) from `sideData` to
every tuple in `importantCustomerData` for which `numericalStateId` equals `stateId`.

Since `sideData` is smaller than `importantCustomerData` (sideData contains only 50 tuples, one for
each state in the United States, while `importantCustomerData` could be very big), we use the
`joinWithSmaller` operation on `importantCustomerData`.  Specifying which pipe we expect to be
smaller enables Scalding optimize the MapReduce jobs.

Finally, we apply another projection to our pipe, retaining only the fields `userId` and `stateId`
(since our goal is to obtain per-state counts of customers who have spent more than two dollars).

### Group by

{% highlight scala %}
// Group by the states customers are from and compute the size of each group.
val importantCustomersPerState = importantCustomerDataWithStateNames
    .groupBy('stateName) { group => group.size('customersPerState) }
{% endhighlight %}

This step groups the tuples from the previous step by their `stateName`, and for each group, puts
the size of the group in a new field called `customersPerState`.

### Output

{% highlight scala %}
// Output to a file in tab-separated form.
importantCustomersPerState.write(Tsv("important-customers-by-state.txt"))
{% endhighlight %}

`Tsv` is a predefined Scalding source. It writes the tuples out to a file in tab-separated form.
KijiExpress provides sources to read from and write to Kiji tables, which you will see later in the
tutorial.

### Results

If you run this script with the file "user-file.txt":
{% highlight scala %}
daisy 1 3
robert 4 0
kiyan 2 5
juliet 1 4
renuka 2 2
{% endhighlight %}

and "state-names.txt":
{% highlight scala %}
1 California
2 Washington
{% endhighlight %}

Then the output in the file `important-customers-by-state.txt` will be:
{% highlight scala %}
California  2
Washington  1
{% endhighlight %}

This result show that there are two customers in California and one in Washington who spent more
than two dollars.

## Scala Quick Reference<a id="summary"> </a>

Below we summarize the Scala commands we used in our example Scalding script.

### Indicate Fields

Precede field names with a single quote:

{% highlight scala %}
<object>.map(('<input-field>, '<input-field> ...) -> ('<mapped-field>, '<mapped-field>, ..))
{% endhighlight %}

### Input From File

{% highlight scala %}
val <variable-name> = TextLine("<filename>")
{% endhighlight %}

### Map

Include the input and output fields.

{% highlight scala %}
val <variable-name> = <object>.map('<input-field> -> ('<output-field1>, '<output-field2>, ...)) { <map function> }
{% endhighlight %}

Include only the output fields:

{% highlight scala %}
val <variable-name> = <object>.mapTo('<input-field> -> ('<output-field1>, '<output-field2>, ...)) { <map function> }
{% endhighlight %}

### Split String at Blanks into Tuple

{% highlight scala %}
{ <object>: String => (<object>.split(" ")(0), <object>.split(" ")(1)) }
{% endhighlight %}

### Project

{% highlight scala %}
val <variable-name> = <object>.project('<field1>, '<field2>, ...)
{% endhighlight %}

### Filter

{% highlight scala %}
val <variable-name> = <object>.filter('<field>, '<field>, ...) { function }
{% endhighlight %}

### Join

In addition, to `joinWithSmaller`, there are methods `joinWithLarger` and `joinWithTiny`. See
[Scalding Join
Operations](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference#wiki-join-functions).

{% highlight scala %}
val <variable-name> = <object>.joinWithSmaller('<field-from-this-data-set> -> '<field-from-other-data-set>, <other-data-set>)
{% endhighlight %}

### Group By

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { <group function> }
{% endhighlight %}

### Group By Value

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { x => x }
{% endhighlight %}

### Calculate Size

{% highlight scala %}
val <variable-name> = <object>.groupBy('<field>) { <group> => <group>.size('<field>) }
{% endhighlight %}

### Output TSV

For other sources in addition to Tsv, see [Scalding Sources](https://github.com/twitter/scalding/wiki/Scalding-Sources).

{% highlight scala %}
<object>.write(Tsv("<filename>"))
{% endhighlight %}


## Scalding Resources
There are many resources available to learn more about the Scalding library.

* [The Fields Based API
  Reference](http://github.com/twitter/scalding/wiki/Fields-based-API-Reference) contains details on
  the operations you can use with Scalding to derive results and transform collections of tuples.
* [The Scalding Github Page](http://github.com/twitter/scalding) contains a copy of the project,
  including an informative README file.
* [The Scalding Wiki](http://github.com/twitter/scalding/wiki) contains links to many resources
  about Scalding, including [Scalding Sources](https://github.com/twitter/scalding/wiki/Scalding-Sources)
