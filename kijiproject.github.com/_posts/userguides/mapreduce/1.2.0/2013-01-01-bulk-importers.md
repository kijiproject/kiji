---
layout: post
title: Bulk Importers
categories: [userguides, mapreduce, 1.2.0]
tags : [mapreduce-ug]
version: 1.2.0
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
[`KijiBulkImporter`]({{site.api_mr_1_2_0}}/bulkimport/KijiBulkImporter.html).

### Bulk Imports from the DDL Shell

KijiMR includes standard bulk importers in its library. It also includes an extension for
the KijiSchema Shell that allows you to run bulk imports using a SQL-like syntax.
If you downloaded the Bento Box, the KijiMR library and the shell extensions are already
included in your `$KIJI_HOME/lib` directory.

To use a bulk importer, you should first enter the schema shell, then load the `bulkimport`
module.

{% highlight text %}
$ kiji-schema-shell
Kiji schema shell v1.0.0
Enter 'help' for instructions (without quotes).
Enter 'quit' to quit.
DDL statements must be terminated with a ';'
schema> MODULE bulkimport;
Loading module "bulkimport"
{% endhighlight %}

You can then run a bulk importer by specifying the data to load, the table
to load it to, and the importer to run. Most importers will require some sort of configuration.
The `bulkimport` module provides direct support for configuring the "described text" format used by
several of KijiMR's stock bulk importers (CSV, JSON, and XML). These formats rely on
mappings from named fields in the input file to columns in the target table.

Without specifying the specific importer to run, this will fall back on the CSV bulk importer.
As an example, you could load a CSV file full of info about soldiers as:

{% highlight text %}
LOAD DATA INFILE '/users/patton/army.csv' INTO TABLE troops
THROUGH PATH '/users/patton/load_troops'
FIELDS TERMINATED BY ','
MAP FIELDS (enlistment_date, name, rank, serial_number) AS (
  name => info:name,
  rank => info:rank,
  serial_number => info:serial_number,
  serial_number => $ENTITY,
  enlistment_date => $TIMESTAMP);
{% endhighlight %}

The input file (or directory) specified by `INFILE` should be in HDFS; it will be
loaded in parallel by a MapReduce job.

The input file would look something like:

{% highlight text %}
1362282822000,John Doe,Sgt,123456
1352282822000,George Patton,General,021415
...
{% endhighlight %}


The `FIELDS TERMINATED BY ','` is optional; a comma is the default separator. You could
also write `FIELDS TERMINATED BY '\t'` to use tab-separated data.

The `MAP FIELDS` clause lists the fields in the CSV file and describes how to map
them to columns in the table. Each field to include is mapped to a specific output
column.

The magic tokens `$ENTITY` and `$TIMESTAMP` allow you to specify particular
fields of the input that are used as the entity id, and a consistent timestamp
for the row. The latter of these is optional; it can simply use the system
timestamp of the insert. The `$TIMESTAMP` field must be specified in milliseconds
since 1970.

For convenience, you can also map named fields into a single column family. To
operate on the previous example, if the field names and qualifiers line up, you
could express the above as:

{% highlight text %}
LOAD DATA INFILE '/users/patton/army.csv' INTO TABLE troops
THROUGH PATH '/users/patton/load_troops'
MAP FIELDS  (enlistment_date, name, rank, serial_number) AS (
  DEFAULT FAMILY info,
  serial_number => info:serial_number,
  serial_number => $ENTITY,
  enlistment_date => $TIMESTAMP
);
{% endhighlight %}

Note that fields mentioned explicitly (e.g., `$ENTITY` and `$TIMESTAMP`) are not
part of the default target family; `serial_number` is explicitly mapped into a column
as well as the entity Id, whereas `enlistment_date` is only used as a timestamp.

#### Load Strategies

When specifying how to run a bulk import, you must pick a strategy to use when
loading.

You can specify `THROUGH PATH <path>`, meaning to write HFiles in the directory
specified by `<path>`, and then use HBase's bulk-load mechanism on them. This is
generally higher performance and provides greater isolation, but interacts less
predictably with compactions in production clusters. The specified path should not
exist ahead of time. It will be used to hold temporary data while processing the bulk
import. The directory will be removed at the end of the job.

You could also specify `DIRECT`, which means to write directly into the specified
table using HBase `put` operations.  Due to the different performance characteristics
of each, you must explicitly choose the load strategy.

#### Running Other Bulk Importers

You could also use this mapped-fields syntax to read from other file formats.
For example, to read from JSON:

{% highlight text %}
LOAD DATA INFILE '/users/patton/army.json' INTO TABLE troops
THROUGH PATH '/users/patton/load_troops'
USING 'org.kiji.mapreduce.lib.bulkimport.JSONBulkImporter'
MAP FIELDS (enlistment_date, name, rank, serial_number) AS (
  DEFAULT FAMILY info,
  serial_number => info:serial_number,
  serial_number => $ENTITY,
  enlistment_date => $TIMESTAMP
);
{% endhighlight %}


You can also run other bulk importers that you write (a description of the API is later in
this section). You can set key-value properties to include in the MapReduce job's `Configuration`
with the `PROPERTIES` keyword:

{% highlight text %}
LOAD DATA INFILE '/users/patton/army.custom.fmt.txt' INTO TABLE troops
THROUGH PATH '/users/patton/load_troops'
USING 'org.example.CustomFormatBulkImporter'
PROPERTIES (
  'custom.timestamp' = 'enlistment_date',
  'foo' = 'bar',
  ...);
{% endhighlight %}

The above will configure a custom bulk importer with some properties to include in the
JobConf.

If the specified importer subclasses `DescribedInputTextBulkImporter` and
supports a JSON field-mapping control file, you may use a `MAP FIELDS`
clause to specify field names to map (before the optional `PROPERTIES` clause).

#### Specifying the InputFormat

When loading data, you can specify an individual file to load, or a directory
full of files. You may specify a different input format after the input path
with the syntax:

{% highlight text %}
LOAD DATA INFILE '/path/to/files' FORMAT '<somefmt>' INTO TABLE ...
{% endhighlight %}

For instance:

{% highlight text %}
LOAD DATA INFILE '/path/to/sequencefiles' FORMAT 'seq' INTO TABLE ...
{% endhighlight %}

The format strings this tool accepts are the same as are used by the
<a href="../command-line-tools/">command line tools</a> to describe input data
(see "Input/output formats" on that page).

Not all bulk importers work with all formats; for instance, the CSV and JSON importers
only work with text files. (`FORMAT 'text'` is the implied default.)
Each bulk importer class may require a particular input format.

#### Bulk Import Shell Grammar

The complete grammar for valid statements accepted by the `bulkimport` module is as
follows:

{% highlight text %}
field_mapping ::= MAP FIELDS [ (field_name, field_name, ...) ] AS (<mapping_elem>, ...)

mapping_elem ::= DEFAULT FAMILY family_name
               | field_name => family:column
               | field_name => $ENTITY
               | field_name => $TIMESTAMP

csv_import ::= LOAD DATA INFILE 'hdfs://uri/here' INTO TABLE table_name
    [ DIRECT | THROUGH PATH 'hdfs://uri/to/tmp/dir' ]
    [ FIELDS TERMINATED BY { '\t' | ',' } ]
    <field_mapping>;

generic_import ::= LOAD DATA INFILE 'hdfs://uri/here' [FORMAT 'fmt']
    INTO TABLE table_name
    [ DIRECT | THROUGH PATH 'hdfs://uri/to/tmp/dir' ]
    USING 'com.example.ImporterClassName'
    [ <field_mapping> ]
    [ PROPERTIES ( 'propname' = 'propvalue', ... ) ];

bulk_import ::= csv_import | generic_import
{% endhighlight %}


The rest of this section describes how bulk importers are implemented, how you
can extend these with your own bulk importers for custom data formats, and what
bulk importer classes are available in the KijiMR library.

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
[`KijiBulkImporter`]({{site.api_mr_1_2_0}}/bulkimport/KijiBulkImporter.html) with the types of the key
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

* `EntityId getEntityId(Object... components)` returns the
  [`EntityId`]({{site.api_schema_1_3_0}}/EntityId.html) for a row in a Kiji table given a string
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

* [`CSVParser`]({{site.api_mrlib_1_1_0}}/util/CSVParser.html) - parses delimited CSV (Comma Separated
  Value) data into component fields.  This parser also handles TSV (Tab Separated Value) data.

* [`CommonLogParser`]({{site.api_mrlib_1_1_0}}/util/CommonLogParser.html) - parses Common Log Format
  data (used by Apache web server) into the relevant fields for each request in the log.

There are several associated bulk importers that parse data into rows:

* [`CSVBulkImporter`]({{site.api_mrlib_1_1_0}}/bulkimport/CSVBulkImporter.html) - takes in CSV files
  and writes a row for each line in the file.

* [`CommonLogBulkImporter`]({{site.api_mrlib_1_1_0}}/bulkimport/CommonLogBulkImporter.html) - takes in
  an Apache web server log and produces a row for each client request.

* [`JSONBulkImporter`]({{site.api_mrlib_1_1_0}}/bulkimport/JSONBulkImporter.html) - takes in a text
  file with a JSON object on each line and produces a row for each object.

* [`XMLBulkImporter`]({{site.api_mrlib_1_1_0}}/bulkimport/XMLBulkImporter.html) - takes a text encoded
  XML file and a user specified record delimiting tag and produces a row for each XML record.

All of these bulk importers extend
[`DescribedInputTextBulkImporter`]({{site.api_mrlib_1_1_0}}/bulkimport/DescribedInputTextBulkImporter.html)
which contains helper functions and can be configured via a
[`KijiTableImportDescriptor`]({{site.api_mrlib_1_1_0}}/bulkimport/KijiTableImportDescriptor.html).
[`KijiTableImportDescriptor`]({{site.api_mrlib_1_1_0}}/bulkimport/KijiTableImportDescriptor.html) is
an Avro-based specification that maps data in input files onto an existing Kiji table layout. 

See the javadoc for these classes for instructions and examples on using them.

