---
title: Accessing Data
layout: post
categories: [userguides, schema, 1.0.0-rc5]
tags: [schema-ug]
version: 1.0.0-rc5
order : 4
description: How to access data using KijiSchema.
---

The [`KijiTableReader`]({{site.api_schema_rc5}}/KijiTableReader.html) class provides a `get(...)` method to read typed data from a Kiji table row.
The row is addressed by its [`EntityId`]({{site.api_schema_rc5}}/EntityId.html)
(which can be retrieved from the [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html) instance
using the [`getEntityId()`]({{site.api_schema_rc5}}/KijiTable.html#getEntityId%28java.lang.String%29) method).
Specify the desired cells from the rows with a [`KijiDataRequest`]({{site.api_schema_rc5}}/KijiDataRequest.html).
See the [`KijiDataRequest`]({{site.api_schema_rc5}}/KijiDataRequest.html) documentation for details.

In general, [`Kiji`]({{site.api_schema_rc5}}/Kiji.html) and [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html) instances should only be opened once over the life of an application
([`EntityIdFactory`]({{site.api_schema_rc5}}/EntityIdFactory.html)s should also be reused).
[`KijiTablePool`]({{site.api_schema_rc5}}/KijiTablePool.html) can be used to maintain a pool of opened [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html) objects for reuse.
To initially open a [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html):

{% highlight java %}
// URI for Kiji instance « kiji_instance_name » in your default HBase instance:
final KijiURI kijiURI = KijiURI.newBuilder().withInstanceName("kiji_instance_name").build();
final Kiji kiji = Kiji.Factory.open(kijiURI);
try {
  final KijiTable table = kiji.openTable("table_name");
  try {
    // Use the opened table:
    // …
  } finally {
    // Always close the table you open:
    table.close();
  }
} finally {
  // Always release the Kiji instances you open:
  kiji.release();
}
{% endhighlight %}

To read from an existing [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html),
create a [`KijiDataRequest`]({{site.api_schema_rc5}}/KijiDataRequest.html) specifying the columns of data to return.
Then, query for the desired [`EntityId`]({{site.api_schema_rc5}}/EntityId.html),
using a [`KijiTableReader`]({{site.api_schema_rc5}}/KijiTableReader.html).
You can get a [`KijiTableReader`]({{site.api_schema_rc5}}/KijiTableReader.html) for a [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html) using the [`openTableReader()`]({{site.api_schema_rc5}}/KijiTable.html#openTableReader%28%29) method.

For example:

{% highlight java %}
final KijiTableReader reader = table.openTableReader();
try {
  // Select which columns you want to read:
  final KijiDataRequest dataRequest = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create().add("some_family", "some_qualifier"))
      .build();
  final EntityId entityId = table.getEntityId("your-row");
  final KijiRowData rowData = reader.get(entityId, dataRequest);
  // Use the row:
  // …
} finally {
  // Always close the reader you open:
  reader.close();
}
{% endhighlight %}

The [`KijiTableReader`]({{site.api_schema_rc5}}/KijiTableReader.html) also implements a [`bulkGet(...)`]({{site.api_schema_rc5}}/KijiTableReader.html#bulkGet%28java.util.List%2C%20org.kiji.schema.KijiDataRequest%29) method
for retrieving data for a list of [`EntityId`]({{site.api_schema_rc5}}/EntityId.html)s.
This is more efficient than a series of calls to `get(...)` because it uses a single RPC instead of one for each get.

## Row scanners<a id="scanner"> </a>

If you need to process a range of row, you may use a row [`KijiRowScanner`]({{site.api_schema_rc5}}/KijiRowScanner.html):

{% highlight java %}
final KijiTableReader reader = table.openTableReader();
try {
  final KijiDataRequest dataRequest = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create().add("family", "qualifier"))
      .build();
  final KijiScannerOptions scanOptions = new KijiScannerOptions()
      .setStartRow(table.getEntityId("the-start-row"))
      .setStopRow(table.getEntityId("the-stop-row"));
  final KijiRowScanner scanner = reader.getScanner(dataRequest, scanOptions);
  try {
    // Scan over the requested row range, in order:
    for (KijiRowData row : scanner) {
      // Process the row:
      // …
    }
  } finally {
    // Always close scanners:
    scanner.close();
  }
} finally {
  // Always close table readers:
  reader.close();
}
{% endhighlight %}

## Modifying Data<a id="modifying-data"> </a>

The [`KijiTableWriter`]({{site.api_schema_rc5}}/KijiTableWriter.html) class provides a `put(...)` method to write or update cells in a Kiji table.
The cell is addressed by its entity ID, column family, column qualifier, and timestamp.
You can get a [`KijiTableWriter`]({{site.api_schema_rc5}}/KijiTableWriter.html) for a [`KijiTable`]({{site.api_schema_rc5}}/KijiTable.html) using the [`openTableWriter()`]({{site.api_schema_rc5}}/KijiTable.html#openTableWriter%28%29) method.

{% highlight java %}
final KijiTableWriter writer = table.openTableWriter();
try {
  // Write a string cell named "a_family:some_qualifier" to the row "the-row":
  final long timestamp = System.currentTimeMillis();
  final EntityId eid = table.getEntityId("the-row");
  writer.put(eid, "a_family", "some_qualifier", timestamp, "Some value!");
  writer.flush();
} finally {
  // Always close the writers you open:
  writer.close();
}
{% endhighlight %}

Note: the type of the value being written to the cell must match the type of the column declared in the table layout.

## Counters<a id="counters"> </a>

Incrementing a counter value stored in a Kiji cell would normally require a
"read-modify-write" transaction using a client-side row lock. Since row
locks can cause contention, Kiji exposes a feature of HBase to do this more
efficiently by pushing the work to the server side. To increment a counter value in
a Kiji cell, the column must be declared with a schema of type
"counter". See [Managing Data]({{site.userguide_schema_rc5}}/managing-data#layouts)
for details on how to declare a counter in your table layout.

Columns containing counters may be accessed like other columns; counters are exposed as long integers.
In particular, the counter value may be retrieved using `KijiTableReader.get(...)` and written using `KijiTableWriter.put(...)`.
In addition to that, the [`KijiTableWriter`]({{site.api_schema_rc5}}/KijiTableWriter.html) class also provides a method to atomically increment counter values.

{% highlight java %}
final KijiTableWriter writer = table.openTableWriter();
try {
  // Incrementing the counter type column "a_family:some_counter_qualifier" by 2:
  final EntityId eid = table.getEntityId("the-row");
  writer.increment(eid, "a_family", "some_counter_qualifier", 2);
  writer.flush();
} finally {
  // Always close the writer you open:
  writer.close();
}
{% endhighlight %}

## MapReduce<a id="mapreduce"> </a>

<div class="row">
  <div class="span2">&nbsp;</div>
  <div class="span8" style="background-color:#eee; border-radius: 6px; padding: 10px">
    <h3>Deprecation Warning</h3>
    <p>
      This section refers to classes in the <tt>org.kiji.schema.mapreduce</tt> package
      that may be removed in the future. Please see the <a href="/userguides/mapreduce/1.0.0-rc5/kiji-mr-overview/">
      KijiMR Userguide</a> for information on using MapReduce with Kiji.
    </p>
  </div>
</div>

The [`KijiTableInputFormat`]({{site.api_schema_rc5}}/mapreduce/KijiTableInputFormat.html) provides the necessary functionality to read from a Kiji table in a
MapReduce job. To configure a job to read from a Kiji table, use [`KijiTableInputFormat`]({{site.api_schema_rc5}}/mapreduce/KijiTableInputFormat.html)'s
static `setOptions` method. For example:

{% highlight java %}
Configuration conf = HBaseConfiguration.create();
Job job = new Job(conf);

// * Setup jars to ship to the hadoop cluster.
job.setJarByClass(YourClassHere.class);
GenericTableMapReduceUtil.addAllDependencyJars(job);
DistributedCacheJars.addJarsToDistributedCache(job,
    new File(System.getenv("KIJI_HOME"), "lib"));
job.setUserClassesTakesPrecedence(true);
// *

KijiDataRequest request = new KijiDataRequest()
    .addColumn(new KijiDataRequest.Column("your-family", "your-qualifier"));

// Setup the InputFormat.
KijiTableInputFormat.setOptions(job, "your-kiji-instance-name", "the-table-name", request);
job.setInputFormatClass(KijiTableInputFormat.class);
{% endhighlight %}
The code contained within "// \*" is responsible for shipping Kiji resources to the DistributedCache.
This is so that all nodes within your hadoop cluster will have access to Kiji dependencies.

[`KijiTableInputFormat`]({{site.api_schema_rc5}}/mapreduce/KijiTableInputFormat.html) outputs keys of type [`EntityId`]({{site.api_schema_rc5}}/EntityId.html) and values of type [`KijiRowData`]({{site.api_schema_rc5}}/KijiRowData.html). This
data can be accessed from within a mapper:

{% highlight java %}
@Override
public void map(EntityId entityId, KijiRowData row, Context context) {
  // ...
}
{% endhighlight %}

To write to a Kiji table from a MapReduce job, you should use
[`KijiTableWriter`]({{site.api_schema_rc5}}/KijiTableWriter.html) as before. You should also set
your OutputFormat class to `NullOutputFormat`, so MapReduce doesn't expect to create
a directory full of text files on your behalf.

To configure a job to write to a Kiji table, refer to the following example:

{% highlight java %}
Configuration conf = HBaseConfiguration.create();
Job job = new Job(conf);

// Setup jars to ship to the hadoop cluster.
job.setJarByClass(YourClassHere.class);
GenericTableMapReduceUtil.addAllDependencyJars(job);
DistributedCacheJars.addJarsToDistributedCache(job,
    new File(System.getenv("KIJI_HOME"), "lib"));
job.setUserClassesTakesPrecedence(true);

// Setup the OutputFormat.
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(NullWritable.class);
job.setOutputFormatClass(NullOutputFormat.class);
{% endhighlight %}

And then, from within a Mapper:

{% highlight java %}
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, KijiOutput> {
  private KijiTableWriter writer;
  private Kiji kiji;
  private KijiTable table;

  @Override
  public void setup(Context context) {
    // Open a KijiTable for generating EntityIds.
    kiji = Kiji.open("your-kiji-instance-name");
    table = kiji.openTable("the-table-name");

    // Create a KijiTableWriter that writes to a MapReduce context.
    writer = table.openTableWriter();
  }

  @Override
  public void map(LongWritable key, Text value, Context context) {
    // ...

    writer.put(table.getEntityId("your-row"), "your-family", "your-qualifier", value.toString());
  }

  @Override
  public void cleanup(Context context) {
    writer.close();
    kiji.close();
    table.close();
  }
}
{% endhighlight %}
