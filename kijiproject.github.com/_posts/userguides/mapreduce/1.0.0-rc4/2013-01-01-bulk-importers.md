---
layout: post
title: Bulk Importers
categories: [userguides, mapreduce, 1.0.0-rc4]
tags : [mapreduce-ug]
version: 1.0.0-rc4
order : 2
description: Bulk Importers.
---

### Motivation

Before we can analyze any data in a Kiji table, we have to get that data into the Kiji table.

Using Kiji Schema alone, one can load data into a Kiji table from a single machine using a simple program with a while loop. For very small jobs, the speed of one machine may be fast enough, but for bigger jobs, a distributed approach is needed. With a little elaboration, one could distribute the simple program's work in the form of a MapReduce job whose mappers write to the Kiji table in parallel. However, writing directly to Kiji Schema's underlying HBase from a MapReduce job can introduce heavy load to a cluster making things sluggish or even unstable.

To more efficiently import data into Kiji tables, Kiji MapReduce includes _BulkImporters_. A bulk importer is a MapReduce job that processes its input into files that can be bulk loaded directly into Kiji. The format of the input and how it translates into Kiji table entity IDs and columns are details particular to each concrete subclass of `KijiBulkImporter`.

Kiji MapReduce Lib contains a set of sample parsers and bulk importers that enable users to import data for many common data formats (including CSV, JSON, and the Apache Common Log Format) into a Kiji table using an Avro description of how the data maps into the table's layout. See the _Provided Library Classes_ section below for more information.

### Classes Overview

Kiji bulk importers rely on two classes:
* _KijiBulkImporter_ - the base class for concrete bulk importers.  Subclasses must implement the `produce()` method for converting the raw data into Kiji puts, as well as the `setup()` and `teardown()` methods if they wish to perform any special configuration at task startup or shutdown respectively.
* _KijiBulkImportJobBuilder_ - a MapReduce job builder that allows the creation of bulk import jobs using concrete bulk importers.

Bulk-importers inherit from `KijiBulkImporter` and must implement their bulk-importing logic in the `produce()` method.
Optionally, Bulk-importers may use the `setup()` and `cleanup()` methods to initialize and finalize resources that can be shared across input records.
These methods will be called once by each task, `setup()` before processing input records and `cleanup()` after the task is done processing input.

### Using the API

If one of the precanned bulk importers listed below is insufficient for the data that is to be imported,
the KijiBulkImporter can be extended to support parsing and importing the desired data.
The `produce()` method needs to be implemented to handle the extraction of data from the import text.

Once you have the requisite bulk-importer, importing the data can simply follow these steps:

*   Define any necessary configuration for the bulk importer.
    For example, the bulk importers in Kiji MapReduce Library that use the DescribedInputTextBulkImporter all require a table import descriptor to map between source data and destination data.

*   Once the proper configuration file has been written, the data can be bulk imported into Kiji via commands like:

### Example

{% highlight java %}
/**
 * Example of a Bulk-importer.
 *
 * Reads a text file formatted as "rowKey;integerValue",
 * and emits the integer value in the specified row into the column:
 *     "imported_family:int_value_column"
 *
 * Each line from the input text file is converted into an input key/value pair
 * by the Hadoop text input format, where:
 *   <li> the key is the offset of the line in the input file,
 *        as a LongWritable named 'filePos';
 *   <li> the value is the line content, as a Text object named 'value'.
 */
public class BulkImporterExample extends KijiBulkImporter<LongWritable, Text> {
  public static enum Counters {
    INVALID_INPUT_LINE,
    INVALID_INTEGER,
  }

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable filePos, Text value, KijiTableContext context)
      throws IOException {
    // Process one line from the input file (filePos is not used in this example):
    final String line = value.toString();

    // Line is expected to be formatted as "rowKey;integerValue":
    final String[] split = line.split(";");
    if (split.length != 2) {
      // Record the invalid line and move on:
      context.incrementCounter(Counters.INVALID_INPUT_LINE);
      return;
    }
    final String rowKey = split[0];
    try {
      final int integerValue = Integer.parseInt(split[1]);

      // Write a cell in row named 'rowKey', at column 'imported:int_value':
      final EntityId eid = context.getEntityId(rowKey);
      context.put(eid, "imported_family", "int_value_column", integerValue);

    } catch (NumberFormatException nfe) {
      // Record the invalid integer and move on:
      context.incrementCounter(Counters.INVALID_INTEGER);
      return;
    }
  }
}
{% endhighlight %}

This bulk-importer may be run from the console with a command like the following. This command assumes that our input lives as a text file in HDFS and that our output table, named `number_table` here, has already been installed with the correct layout on the default Kiji instance.

{% highlight bash %}
kiji bulk-import \
    --importer=my.application.package.BulkImporterExample \
    --input="format=text file=hdfs://cluster/path/to/text-input-file \
    --output="format=kiji table=kiji://.env/default/table nsplits=1" \
{% endhighlight %}

This will launch a mapreduce job to bulk load the table.

See the commandline section of this userguide for a more comprehensive list of options on the command-line interface.

### Provided Library Classes

Within the `org.kiji.mapreduce.lib.bulkimport` package of the Kiji MapReduce Library, there is a variety of useful parsers for building your own bulk importer:
* `CSVParser` - parses delimited CSV(Comma Separated Value) data into the component fields.  This parser also handles TSV(Tab Separated Value) data.
* `CommonLogParser` - parses Common Log Format data(used by Apache web server) into the relevant fields for each request in the log.

There are several associated bulk importers that parse data into rows:
* `CSVBulkImporter` - takes in CSV files and produces a row for each line in the file.
* `CommonLogBulkImporter` - takes in an Apache web server log and produces a row for each client request.
* `JSONBulkImporter` - takes an a text file with a JSON object on each line and produces a row for each object.

All of these bulk importers extend `DescribedInputTextBulkImporter` which contains helper functions and can be configured via a `KijiTableImportDescriptor` object. `KijiTableImportDescriptor` is an Avro-based specification that translates from the inferred schemas inside of the input files to the existing Kiji table layout.

See the javadoc for these classes for instructions and examples on using them.
