---
layout: post
title: Data Flow Operations in KijiExpress
categories: [userguides, express, devel]
tags : [express-ug]
version: devel
order : 5
description: Data Flow Operations in KijiExpress.
---
##DRAFT##

You go through the following steps to create a working KijiExpress job:

* Write an “importer” for each Kiji table you want to fill with source data.
* Perform some logic to manipulate the source data.
* Write the resulting data into Kiji tables.

### KijiSources

?TBW KijiInput, KijiOutput, Tsv, Csv?

[Scalding Sources](https://github.com/twitter/scalding/wiki/Scalding-Sources)

Syntax for KijiInput/KijiOutput:
* basics: how they are specified
* writing to map-type column families with a qualifier from a field in the tuple
* specifying filters columns on KijiInputs
* Syntax for timestamp field and entity id field when writing. Special entity id field when
reading.

### Using Avro Types
?TBW?

### Reading Data

For each column, a minimal “reader” schema is specified; this provides multiple team members
working with a large data set with a common data dictionary; it also enables validation of
cell data against a reference schema.

#### Using a Map Family as Input

KijiInput(uri, column -> field)

KijiInput(args("user-table"), Map(ColumnFamilyRequestInput("rating") -> 'ratings))


#### Writing an Importer for the Source Data

KijiExpress provides a stock importer you can use to import JSON data. We recommend you use
this ?when?.

If the stock importer isn’t appropriate for what you are doing (especially when you first
start out and are looking for something with a little less overhead), you can write your
own importer using Scala. The contents of the importer are as follows:

* Identify the source data location
* Identify the target Kiji table
* Define any functions you want to operate on the data before it is written to the target
* Define the mapping between the source data and the target columns

The fastest way to write an importer may be to copy one of the Scala importer files from a
Kiji tutorial project and modify it to meet your needs. Find an example here:

    ${KIJI_HOME}/examples/express-music/src/main/scala/org/kiji/express/music/SongMetadataimporter.scala

For a walkthrough of Scala syntax, see [Writing Basic KijiExpress Jobs](../basic-jobs).

### Processing Data

?TBW: More thorough run-through of the things you can do to data in a pipeline: emphasis
not on the line-by-line syntax.?

#### map/flatMap/mapTo/flatMapTo

* `map` - Applies a function over all tuples in a pipe and produce extra fields. (e.g.
take the “name” field and produce “first_name” and “last_name” fields).

* `flatMap` - Similar to map but will produce new “rows” with one or more additional fields.
(e.g. take a single string and produce new “rows” each with a single “word” field.)

* `mapTo`/`flatMapTo` variants do the exact same thing except discard any fields in the pipe
that are not part of the output of the `mapTo` or `flatMapTo` call. Same as calling `project`
after `map` or `flatMap` but more efficient.

#### filter

Apply a function over all tuples that will restrict the pipe to only those tuples where
some condition is true. (e.g., include tuples where the phone number field is a valid number).

#### groupBy/aggregation

* Group on one or more columns and execute reduce functions on each group. (e.g., group by
email address and count the number of messages received).

* Invoking groupBy will add a MapReduce reduce phase to your flow. These can be costly!

#### joins

Join one or more pipes together to produce a new pipe. Standard join semantics (inner, left,
right) along with special joins to handle asymmetries in the size of the pipes. (e.g.,
join a large pipe of user data with a smaller pipe containing IP => location data)

#### packAvro

?TBW?

### Writing Logic to Manipulate the Source Data

In KijiExpress, you read data from a Kiji table (or other source), perform operations on
the data, then write the data to a Kiji table (or other target). The data moves between
operations as fluid through a pipe: the same data flows through the process unless specifically
augmented or discarded. Scala commands are designed to specify these operations on the
pipeline succinctly: each statement describes the pipeline stage (input), any data mapping,
any data manipulation, and the output to the next stage of the pipeline. When you are
writing modeling logic, think in terms of defining the flow of data to the result you are
trying to achieve.

When you are writing Express jobs, define your logic in Scala classes. Typically you would
include a single classes in a .scala file and specify that class when you run the job.
Multiple classes can be compiled into a single JAR file.

### Writing Data
?TBW?

