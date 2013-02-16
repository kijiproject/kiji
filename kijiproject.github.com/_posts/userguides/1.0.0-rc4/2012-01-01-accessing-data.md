---
title: Accessing Data
layout: post
categories: [userguides, schema, 1.0.0-rc4]
tags: [schema-ug]
version: 1.0.0-rc4
order : 4
description: How to access data using KijiSchema.
---

The [`KijiTableReader`]({{site.api_url}}KijiTableReader.html) class provides a `get(...)` method to read typed data from a Kiji table row.
The row is addressed by its [`EntityId`]({{site.api_url}}EntityId.html) (which can be retrieved from the [`KijiTable`]({{site.api_url}}KijiTable.html) instance using the [`getEntityId()`]({{site.api_url}}KijiTable.html#getEntityId%28java.lang.String%29) method).
Specify the desired cells from the rows with a [`KijiDataRequest`]({{site.api_url}}KijiDataRequest.html). See the 
[`KijiDataRequest`]({{site.api_url}}KijiDataRequest.html) documentation for details.

In general, [`Kiji`]({{site.api_url}}Kiji.html) and [`KijiTable`]({{site.api_url}}KijiTable.html) instances should only be opened once over the life of an
application. ([`EntityIdFactory`]({{site.api_url}}EntityIdFactory.html)s should also be reused). [`KijiTablePool`]({{site.api_url}}KijiTablePool.html) can be used to maintain a
pool of opened [`KijiTable`]({{site.api_url}}KijiTable.html) objects for reuse. To initially open a [`KijiTable`]({{site.api_url}}KijiTable.html):

{% highlight java %}
Configuration conf = HBaseConfiguration.create();
KijiURI kijiURI = KijiURI.newBuilder().withInstanceName("your-kiji-instance-name");
Kiji kiji = Kiji.open(kijiURI, conf);
KijiTable table = kiji.openTable("the-table-name");
{% endhighlight %}

To read from an existing [`KijiTable`]({{site.api_url}}KijiTable.html) instance, create a [`KijiDataRequest`]({{site.api_url}}KijiDataRequest.html) specifying the columns of data to
return. Then, query for the desired [`EntityId`]({{site.api_url}}EntityId.html), using a [[`KijiTableReader`]({{site.api_url}}KijiTableReader.html)]({{site.api_url}}KijiTableReader.html). You can get a [`KijiTableReader`]({{site.api_url}}KijiTableReader.html) for a [`KijiTable`]({{site.api_url}}KijiTable.html) using the [`openTableReader()`]({{site.api_url}}KijiTable.html#openTableReader%28%29) method. For example:

{% highlight java %}
KijiTableReader reader = table.openTableReader();

// Select which columns you want to read.
KijiDataRequest dataRequest = new KijiDataRequest()
    .addColumn(new KijiDataRequest.Column("your-family", "your-qualifier"));

// Try to reuse EntityIds when possible.
// If a need for this entityId comes up again, reuse
// this same entityId object.
EntityId entityId = table.getEntityId("your-row");
KijiRowData rowData = reader.get(entityId, dataRequest);

// Make sure to close the reader once you're finished.
reader.close();
{% endhighlight %}

The [[`KijiTableReader`]({{site.api_url}}KijiTableReader.html)]({{site.api_url}}KijiTableReader.html) also implements a `bulkGet(...)` method for retrieving data for a list
of [`EntityId`]({{site.api_url}}EntityId.html)s.  This is more efficient than a series of calls to `get(...)` because it uses a single
RPC instead of one for each get.

## Modifying Data<a id="modifying-data"> </a>

The [`KijiTableWriter`]({{site.api_url}}KijiTableWriter.html) class provides a `put(...)` method to write/update cells to a Kiji table. The
cell is addressed by its entity ID, column family, column qualifier, and timestamp.  You can get a [`KijiTableWriter`]({{site.api_url}}KijiTableWriter.html) for a [`KijiTable`]({{site.api_url}}KijiTable.html) using the [`openTableWriter()`]({{site.api_url}}KijiTable.html#openTableWriter%28%29) method.

{% highlight java %}
KijiTableWriter writer = table.openTableWriter();

long timestamp = System.currentTimeMillis();
writer.put(table.getEntityId("your-row"), "your-family", "your-qualifier", timestamp,
    "your-string-value");
writer.flush();
writer.close();
{% endhighlight %}

## Counters<a id="counters"> </a>

Incrementing a counter value stored in a Kiji cell would normally require a
"read-modify-write" transaction using a client-side row lock. Since row
locks can cause contention, Kiji exposes a feature of HBase to do this more
efficiently by pushing the work to the server side. To increment a counter value in
a Kiji cell, the column must be declared with a schema of type
"counter". See [Managing Data]({{site.userguide_url}}/managing-data#layouts)
for details on how to declare a counter in your table layout.

The [`KijiTableWriter`]({{site.api_url}}KijiTableWriter.html) class provides methods for incrementing
counter values. Non-counter columns can not be incremented, and counter columns
support only the increment operation. In other words, attempting to increment a
column value that is not declared to be a counter will throw an exception. Likewise,
attempting to `put(...)` a value into a column that is declared
to be a counter will also throw an exception.  The `setCounter(...)` method should be used when
setting a counter value.

{% highlight java %}
KijiTableWriter writer = table.openTableWriter();

// Incrementing a counter type column by 1.
// The column represented by counter-type-qualifier must
// of type counter otherwise an exception will be thrown.
writer.increment(table.getEntityId("your-row"), "your-family", "counter-type-qualifier", 1);

writer.flush();
writer.close();
{% endhighlight %}

## MapReduce<a id="mapreduce"> </a>

The [`KijiTableInputFormat`]({{site.api_url}}mapreduce/KijiTableInputFormat.html) provides the necessary functionality to read from a Kiji table in a
MapReduce job. To configure a job to read from a Kiji table, use [`KijiTableInputFormat`]({{site.api_url}}mapreduce/KijiTableInputFormat.html)'s
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

[`KijiTableInputFormat`]({{site.api_url}}mapreduce/KijiTableInputFormat.html) outputs keys of type [`EntityId`]({{site.api_url}}EntityId.html) and values of type [`KijiRowData`]({{site.api_url}}KijiRowData.html). This
data can be accessed from within a mapper:

{% highlight java %}
@Override
public void map(EntityId entityId, KijiRowData row, Context context) {
  // ...
}
{% endhighlight %}

To write to a Kiji table from a MapReduce job, you should use
[`KijiTableWriter`]({{site.api_url}}KijiTableWriter.html) as before. You should also set
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

    writer.put(table.getEntityId("your-row"), "your-family, "your-qualifier", value.toString());
  }

  @Override
  public void cleanup(Context context) {
    writer.close();
    kiji.close();
    table.close();
  }
}
{% endhighlight %}
