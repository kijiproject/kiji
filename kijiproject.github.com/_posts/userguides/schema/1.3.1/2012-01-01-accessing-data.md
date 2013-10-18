---
title: Accessing Data
layout: post
categories: [userguides, schema, 1.3.1]
tags: [schema-ug]
version: 1.3.1
order : 4
description: How to access data using KijiSchema.
---

The [`KijiTableReader`]({{site.api_schema_1_3_1}}/KijiTableReader.html) class provides a `get(...)` method to read typed data from a Kiji table row.
The row is addressed by its [`EntityId`]({{site.api_schema_1_3_1}}/EntityId.html)
(which can be retrieved from the [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html) instance
using the [`getEntityId()`]({{site.api_schema_1_3_1}}/KijiTable.html#getEntityId%28java.lang.String%29) method).
Specify the desired cells from the rows with a [`KijiDataRequest`]({{site.api_schema_1_3_1}}/KijiDataRequest.html).
See the [`KijiDataRequest`]({{site.api_schema_1_3_1}}/KijiDataRequest.html) documentation for details.

In general, [`Kiji`]({{site.api_schema_1_3_1}}/Kiji.html) and [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html) instances should only be opened once over the life of an application
([`EntityIdFactory`]({{site.api_schema_1_3_1}}/EntityIdFactory.html)s should also be reused).
[`KijiTablePool`]({{site.api_schema_1_3_1}}/KijiTablePool.html) can be used to maintain a pool of opened [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html) objects for reuse.
To initially open a [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html):

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
    // Always release the table you open:
    table.release();
  }
} finally {
  // Always release the Kiji instances you open:
  kiji.release();
}
{% endhighlight %}

To read from an existing [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html),
create a [`KijiDataRequest`]({{site.api_schema_1_3_1}}/KijiDataRequest.html) specifying the columns of data to return.
Then, query for the desired [`EntityId`]({{site.api_schema_1_3_1}}/EntityId.html),
using a [`KijiTableReader`]({{site.api_schema_1_3_1}}/KijiTableReader.html).
You can get a [`KijiTableReader`]({{site.api_schema_1_3_1}}/KijiTableReader.html) for a [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html) using the [`openTableReader()`]({{site.api_schema_1_3_1}}/KijiTable.html#openTableReader%28%29) method.

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

The [`KijiTableReader`]({{site.api_schema_1_3_1}}/KijiTableReader.html) also implements a [`bulkGet(...)`]({{site.api_schema_1_3_1}}/KijiTableReader.html#bulkGet%28java.util.List%2C%20org.kiji.schema.KijiDataRequest%29) method
for retrieving data for a list of [`EntityId`]({{site.api_schema_1_3_1}}/EntityId.html)s.
This is more efficient than a series of calls to `get(...)` because it uses a single RPC instead of one for each get.

## Row scanners<a id="scanner"> </a>

If you need to process a range of rows, you may use a [`KijiRowScanner`]({{site.api_schema_1_3_1}}/KijiRowScanner.html):

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

The [`KijiTableWriter`]({{site.api_schema_1_3_1}}/KijiTableWriter.html) class provides a `put(...)` method to write or update cells in a Kiji table.
The cell is addressed by its entity ID, column family, column qualifier, and timestamp.
You can get a [`KijiTableWriter`]({{site.api_schema_1_3_1}}/KijiTableWriter.html) for a [`KijiTable`]({{site.api_schema_1_3_1}}/KijiTable.html) using the [`openTableWriter()`]({{site.api_schema_1_3_1}}/KijiTable.html#openTableWriter%28%29) method.

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

### Atomic Modifications<a id="atomic-modifications"> </a>

The [`AtomicKijiPutter`]({{site.api_schema_1_3_1}}/AtomicKijiPutter.html) class provides the ability
to perform atomic operations on Kiji tables.  The atomic putter uses a begin, put, commit workflow
to construct transactions which are executed atomically by the underlying HBase table.  Addressing
puts in the atomic writer is very similar to KijiTableWriter, except you do not need to specify an
entityId for every put, because they all must target the same row.  When your transaction is ready,
write it by using
[`commit()`]({{site.api_schema_1_3_1}}/AtomicKijiPutter.html#commit%28%29).

{% highlight java %}
  KijiTable table = ...
  AtomicKijiPutter putter = table.getWriterFactory().openAtomicWriter();
  try {
    // Begin a transaction on the specified row:
    putter.begin(entityId);

    // Accumulate a set of puts to write atomically:
    putter.put(family, qualifier, "value");
    putter.put(family, qualifier2, "value2");
    putter.put(family2, qualifier3, "value3");
    // More puts...

    // Write all puts atomically:
    putter.commit();
  } finally {
    putter.close();
  }
{% endhighlight %}

If you want to ensure that the table has not been modified while you accumulated puts, you can use
[`checkAndCommit(family, qualifier, value)`]({{site.api_schema_1_3_1}}/AtomicKijiPutter.html#checkAndCommit%28java.lang.String%2C%20java.lang.String%2C%20T)
instead of commit().  This will write your puts and return true if the value in the
specified cell matches the value to check, otherwise it will return false and your puts will remain
in place so that you may attempt a new check.

{% highlight java %}
  KijiTable table = ...
  final AtomicKijiPutter putter = table.getWriterFactory().openAtomicWriter();
  final KijiTableReader reader = table.openTableReader();
  final KijiDataRequest request = KijiDataRequest.create("meta", "modifications");
  final long currentModificationCount =
      reader.get(entityId, request).getMostRecentValue("meta", "modifications");
  try {
    // Begin a transaction on the specified row:
    putter.begin(entityId);

    // Accumulate a set of puts to write atomically:
    putter.put(family, qualifier, "value");
    // Increment a cell indicating the number of times the row has been modified.
    putter.put("meta", "modifications", currentModificationCount + 1L);
    // More puts...

    // Ensure the row has not been modified while preparing the transaction.
    if (putter.checkAndCommit("meta", "modifications", currentModificationCount)) {
      LOG.info("Write successful.");
    } else {
      LOG.info("Write failed");
    }
  } finally {
    putter.close();
  }
{% endhighlight %}

### Batched Modifications<a id="batched-modifications"> </a>

When working with high traffic tables, minimization of remote procedure calls is critical.  The
[`KijiBufferedWriter`]({{site.api_schema_1_3_1}}/KijiBufferedWriter.html) class provides local buffering
to reduce RPCs and improve table performance.  Using the buffered writer is very similar to the standard
KijiTableWriter with a few small differences.  The buffered writer does not write put or delete operations
until the buffer becomes full or the user calls
[`flush`]({{site.api_schema_1_3_1}}/KijiBufferedWriter.html#flush%28%29) or
[`close`]({{site.api_schema_1_3_1}}/KijiBufferedWriter.html#close%28%29). The size of the local
buffer can be managed using
[`setBufferSize`]({{site.api_schema_1_3_1}}/KijiBufferedWriter.html#setBufferSize%28long%29). The buffered
writer cannot buffer increment operations because increment requires an immediate connection to a live
table.
{% highlight java %}

  KijiTable table = ...
  KijiBufferedWriter bufferedWriter = table.getWriterFactory().openBufferedWriter();

  // Set the buffer size to one megabyte
  bufferedWriter.setBufferSize(1024L * 1024L);

  try {
    // Accumulate puts and deletes in the local buffer.
    bufferedWriter.put(entityId, family, qualifier, timestamp, "value");
    bufferedWriter.deleteCell(entityId, family, qualifier);
    // Buffered operations may be written at any time when the buffer becomes full or on calls to
    // flush() on this writer.

    // Manually flush to ensure writes are committed.
    flush();
  } finally {
    // Always close writers.
    bufferedWriter.close();
  }

{% endhighlight %}

## Counters<a id="counters"> </a>

Incrementing a counter value stored in a Kiji cell would normally require a
"read-modify-write" transaction using a client-side row lock. Since row
locks can cause contention, Kiji exposes a feature of HBase to do this more
efficiently by pushing the work to the server side. To increment a counter value in
a Kiji cell, the column must be declared with a schema of type
"counter". See [Managing Data]({{site.userguide_schema_1_3_1}}/managing-data#layouts)
for details on how to declare a counter in your table layout.

Columns containing counters may be accessed like other columns; counters are exposed as long integers.
In particular, the counter value may be retrieved using `KijiTableReader.get(...)` and written using `KijiTableWriter.put(...)`.
In addition to that, the [`KijiTableWriter`]({{site.api_schema_1_3_1}}/KijiTableWriter.html) class also provides a method to atomically increment counter values.

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
      that may be removed in the future. Please see the <a href="/userguides/mapreduce/1.0.0/kiji-mr-overview/">
      KijiMR Userguide</a> for information on using MapReduce with Kiji.
    </p>
  </div>
</div>

The [`KijiTableInputFormat`]({{site.api_schema_1_3_1}}/mapreduce/KijiTableInputFormat.html) provides the necessary functionality to read from a Kiji table in a
MapReduce job. To configure a job to read from a Kiji table, use [`KijiTableInputFormat`]({{site.api_schema_1_3_1}}/mapreduce/KijiTableInputFormat.html)'s
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

[`KijiTableInputFormat`]({{site.api_schema_1_3_1}}/mapreduce/KijiTableInputFormat.html) outputs keys of type [`EntityId`]({{site.api_schema_1_3_1}}/EntityId.html) and values of type [`KijiRowData`]({{site.api_schema_1_3_1}}/KijiRowData.html). This
data can be accessed from within a mapper:

{% highlight java %}
@Override
public void map(EntityId entityId, KijiRowData row, Context context) {
  // ...
}
{% endhighlight %}

To write to a Kiji table from a MapReduce job, you should use
[`KijiTableWriter`]({{site.api_schema_1_3_1}}/KijiTableWriter.html) as before. You should also set
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
    table.release();
    kiji.release();
  }
}
{% endhighlight %}
