---
layout: post
title: Gatherers
categories: [userguides, mapreduce, 1.2.6]
tags : [mapreduce-ug]
version: 1.2.6
order : 4
description: Gatherers.
---

### Motivation

A Kiji Gatherer scans over the rows of a Kiji table using the MapReduce framework and output key-value pairs. Gatherers are a flexible job type and can be used to extract or aggregate information into a variety of formats based on the output specification and reducer used.
Common tasks for gatherers include calculating sums across an entire table, extracting features to train a model, and pivoting information from one table into another. You should use a gatherer when you need to pull data out of a Kiji table into another format or to feed it into a Reducer.

### Classes Overview

There are three classes for an application that wants to use gatherers:

All gatherers extend the abstract class
`org.kiji.mapreduce.gather.KijiGatherer` and override its abstract methods as
described below. Clients should be familiar with the
`org.kiji.mapreduce.gather.GatherContext` class, which is used to output the
Gatherer's key-value pairs. Finally, while gather jobs can be launched from the
command line with `kiji gather`,
`org.kiji.mapreduce.gather.KijiGatherJobBuilder` can be used to construct a
MapReduce job that runs a given gather with configured input, output, and
reducer. The job can then be launched programmatically.

## Using the API

All gatherers must extend the parameterized class [`KijiGatherer`]({{site.api_mr_1_2_6}}/gather/KijiGatherer.html) with the types
of the key and value of their output. Concrete gatherers must implement these methods:

 * `Class<?> getOutputKeyClass()` and `Class<?> getOutputValueClass()`. These methods should return the
classes of the output keys and values emitted by the gatherer.
 * `KijiDataRequest getDataRequest()`. This methods specifies the columns retrieved while scanning rows from the input table. It should construct and return a [`KijiDataRequest`]({{site.api_schema_1_4_0}}/KijiDataRequest.html).
 * `void gather(KijiRowData input, GathererContext<K, V> context)`. This methods contains the gatherer's logic that translates its input into key-value pairs. It will be called once per row processed by the gatherer task. `input` will contain the columns from the row as requested by the [`KijiDataRequest`]({{site.api_schema_1_4_0}}/KijiDataRequest.html) returned by `getDataRequest()`. The gatherer should use its `context` parameter to emit key-value pairs as detailed below.

While processing a row, a gatherer may access data from external stores through
[`KeyValueStore`]({{site.api_mr_1_2_6}}/kvstore/KeyValueStore.html)s by implementing `Map<String,
KeyValueStore<?, ?>> getRequiredStores()`. This method should construct and return a map specifying
all the [`KeyValueStore`]({{site.api_mr_1_2_6}}/kvstore/KeyValueStore.html)s that the gatherer wants
to access. The [`KeyValueStore`]({{site.api_mr_1_2_6}}/kvstore/KeyValueStore.html)s may then later be
accessed from the `gather()` method through the
[`GathererContext`]({{site.api_mr_1_2_6}}/gather/GathererContext.html). For more details, you may
check the [KeyValue Stores]({{site.userguide_mapreduce_1_2_6}}/key-value-stores) section in this guide.

Optionally, a gatherer may implement `setup()` and `cleanup()` to initialize and
finalize resources that can be reused during the gather task. These methods
will be called once by each task, `setup()` before processing input rows and
`cleanup()` after the task is done processing. If you wish to use a
[`KeyValueStore`]({{site.api_mr_1_2_6}}/kvstore/KeyValueStore.html), it should be opened once with `context.getStore(storeName)` in
`setup()`, saved in a member variable, and closed in `cleanup()`.

The class of a gatherer's output key and value may have restrictions
depending on the output format used with this gatherer. For example, if used with a sequence file output format, classes must either be Avro types
or implement the `org.apache.hadoop.io.Writable` interface. See the Command Line Tools section of this guide for more about output format options.

As mentioned above, a gatherer's `gather` method has a `org.kiji.mapreduce.gather.GatherContext` argument. This class has a number of methods which are relevant to a gatherer author:

 * `void write(K key, V value)`. Emits a key-value pair. The key-value pair will be later processed by
the reduce step of this MapReduce job. The type of `key` and `value` should match the
parameter types of the gatherer's class.
 * `void incrementCounter(Enum<?> counter)`. Increments a mapreduce job counter. This can be useful for calculating aggregate counts about the full MapReduce job (for example, you can use a counter to calculate the total number of input rows containing malformed data). This method is common to all KijiContexts.

### Example

{% highlight java %}
/**
 * Example of a gatherer class that extracts the size of households per zip-code.
 *
 * Processes entities from an input table that contains households with:
 *   <li> a column 'info:zip_code' with the zip code of a household;
 *   <li> a column 'members:*' with the household's members names;
 * and emits one key/value pair per household to a sequence file on HDFS, where:
 *   <li> the key is the zip code of the household, as an IntWritable,
 *   <li> the value is the number of persons in the household, as an IntWritable.
 */
public class FamilySizeGatherer extends KijiGatherer<IntWritable, IntWritable> {
  public static enum Counters {
    MISSING_ZIP_CODE,
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    // Zip code encoded as an IntWritable:
    return IntWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    // Family size (number of persons) encoded as an IntWritable:
    return IntWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Fetch columns 'info:zip_code' and map-family 'members':
    return KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .add("info", "zip_code")
            .addFamily("members"))
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData input, GathererContext<IntWritable, IntWritable> context)
      throws IOException {
    // Extract the required data from the input row:
    final Integer zipCode = input.getMostRecentValue("info", "zip_code");
    if (zipCode == null) {
      // Zip code is missing from the input row, report the bad input and move on:
      context.incrementCounter(Counters.MISSING_ZIP_CODE);
      return;
    }

    // Since we only care about the size of the family,
    // rather than the data stored for each member,
    // we only extract the qualifiers for the "members" family.
    final Set<String> members = input.getQualifiers("members");

    // Some computation: for this simple example,
    // we compute the family size from the set of its members:
    final int familySize = members.size();

    // Emit a pair (zip code, family size) to the configured output through the context:
    context.write(new IntWritable(zipCode), new IntWritable(familySize));
  }
}
{% endhighlight %}

The gatherer described above may be used on the command line as follows.
It expects a Kiji input table whose rows represent households,
with a column `'info:zip_code'` that contains the Zip code of each household
and with a column family `'members'` that lists the household members.

{% highlight bash %}
kiji gather \
    --gatherer=my.application.package.FamilySizePerZipCodeGatherer \
    --input="format=kiji table=kiji://.env/default/household_table_name" \
    --output="format=seq file=hdfs://cluster/path/to/sequence-file nsplits=1"
{% endhighlight %}

With this command, the gather job will write its output key-value pairs to sequence files
in the directory `hdfs://cluster/path/to/sequence-file/`. Each entry in the sequence file
will correspond to one household and will contain the household Zip code as a key and
the number of household members as a value.

Each gather task will produce a single sequence file `hdfs://cluster/path/to/sequence-file/part-m-<gather task #>`.
The number of gatherer tasks is currently set to be the number of regions in the input table: each gather task
processes one region from the input table.
Therefore, the `nsplits` parameter of the job output specification is not used in this context when no reducer is specified.
