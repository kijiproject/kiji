---
layout: post
title: Bulk Importers
categories: [userguides, mapreduce, 1.0.0-rc6]
tags : [mapreduce-ug]
version: 1.0.0-rc6
order : 2
description: Bulk Importers.
---

Before we can analyze any data in a Kiji table, we have to get that data into the Kiji table.

Using KijiSchema alone, you can load data into a Kiji table from a single machine using a simple
program looping over the input. For very small jobs the speed of one machine may be sufficient, but
for larger jobs a distributed approach is needed. With a little elaboration, the simple program's
work can be distributed in the form of a MapReduce job whose mappers write to the Kiji table in
parallel. However, writing directly to KijiSchema's underlying HBase from a MapReduce job can
introduce heavy load to a cluster making things sluggish or even unstable.

To more efficiently import data into Kiji tables, KijiMR includes _Bulk Importers_. A bulk importer
is a MapReduce job that processes its input into files that can that can be loaded
directly into Kiji. The format of the input and how it translates into Kiji table entity ids and
columns are details particular to each concrete subclass of
[`KijiBulkImporter`]({{site.api_mr_rc5}}/bulkimport/KijiBulkImporter.html).

### Classes Overview

Kiji bulk importers rely on two classes: all bulk importers extend the abstract class
`org.kiji.mapreduce.bulkimport.KijiBulkImporter` and override its abstract methods as described
below.  Clients should be familiar with the `org.kiji.mapreduce.KijiTableContext` class, which is
used to output key-value pairs from the bulk importer. Finally, while bulk import jobs can be
launched from the command line with `kiji bulk-import`, the class
`org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder` can be used to programatically construct
and launch a bulk import job. 

### Using the API

All bulk importers must extend the parameterized class
[`KijiBulkImporter`]({{site.api_mr_rc5}}/bulkimport/KijiBulkImporter.html) with the types of the key
and value of their input.  Concrete bulk importers must implement the following method:

* `void produce(K key, V value, KijiTableContext context)` contains the logic to produce the content
for the output Kiji table from the input.  It will be called once per key-value pair from the
input (for many input text file types this is once per line).  The `produce()` method can use its
`context` argument to output to Kiji table columns as detailed below.

Optionally, bulk importers can override the `setup()` and `cleanup()` methods to initialize and
finalize resources that can be reused during the bulk import job.  These methods will be called once
by each task: `setup()` before processing any input and `cleanup()` after the task is done
processing.

As mentioned above, a bulk importer's `produce()` method has an `org.kiji.mapreduce.KijiTableContext`
argument.  This class has methods needed to write data to Kiji: 

* `EntityId getEntityId(String kijiRowKey)` returns the
  [`EntityId`]({{site.api_schema_rc5}}/EntityId.html) for a row in a Kiji table given a string
  identifier.

* `void put(EntityId entityId, String family, String qualifier, T value)` writes data to a column
  (identified with a family and qualifier) in the row with a particular entity id. The data is
  written with a timestamp equal to the time `put` is called. The type of `value` must be compatible
  with the output column's schema as declared in the table layout.

* `void put(EntityId entityId, String family, String qualifier, long timestamp , T value)` similar
  to the put call above, but writes data with the specified timestamp.  This is only recommended for
  doing the initial import of data as this affects HBase's internal workings.  See <a
  href="http://www.kiji.org/2013/02/13/common-pitfalls-of-timestamps-in-hbase">Common Pitfalls of
  Timestamps in HBase</a> for more information.

### Example

{% highlight java %}
/**
 * Example of a bulk importer class.
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

This bulk importer may be run from the console with a command like the following. This command
assumes that our input lives as a text file in HDFS and that our output table, named `number_table`
here, has already been created with the correct layout in the default Kiji instance.

{% highlight bash %}
kiji bulk-import \
    --importer=my.application.package.BulkImporterExample \
    --input="format=text file=hdfs://cluster/path/to/text-input-file" \
    --output="format=kiji table=kiji://.env/default/number_table nsplits=1" \
{% endhighlight %}

This will launch a MapReduce job to bulk import data into the table.

See the command line section of this userguide for a more comprehensive list of options available
when running bulk import jobs from the command line.

### Provided Library Classes

Within the `org.kiji.mapreduce.lib.bulkimport` package of the KijiMR Library, there is a variety of
useful parsers for building your own bulk importer:

* [`CSVParser`]({{site.api_mrlib_rc5}}/util/CSVParser.html) - parses delimited CSV (Comma Separated
  Value) data into component fields.  This parser also handles TSV (Tab Separated Value) data.

* [`CommonLogParser`]({{site.api_mrlib_rc5}}/util/CommonLogParser.html) - parses Common Log Format
  data (used by Apache web server) into the relevant fields for each request in the log.

There are several associated bulk importers that parse data into rows:

* [`CSVBulkImporter`]({{site.api_mrlib_rc5}}/bulkimport/CSVBulkImporter.html) - takes in CSV files
  and writes a row for each line in the file.

* [`CommonLogBulkImporter`]({{site.api_mrlib_rc5}}/bulkimport/CommonLogBulkImporter.html) - takes in
  an Apache web server log and produces a row for each client request.

* [`JSONBulkImporter`]({{site.api_mrlib_rc5}}/bulkimport/JSONBulkImporter.html) - takes in a text
  file with a JSON object on each line and produces a row for each object.

All of these bulk importers extend
[`DescribedInputTextBulkImporter`]({{site.api_mrlib_rc5}}/bulkimport/DescribedInputTextBulkImporter.html)
which contains helper functions and can be configured via a
[`KijiTableImportDescriptor`]({{site.api_mrlib_rc5}}/bulkimport/KijiTableImportDescriptor.html).
[`KijiTableImportDescriptor`]({{site.api_mrlib_rc5}}/bulkimport/KijiTableImportDescriptor.html) is
an Avro-based specification that maps data in input files onto an existing Kiji table layout. 

See the javadoc for these classes for instructions and examples on using them.

