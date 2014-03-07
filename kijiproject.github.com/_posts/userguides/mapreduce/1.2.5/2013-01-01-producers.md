---
layout: post
title: Producers
categories: [userguides, mapreduce, 1.2.5]
tags : [mapreduce-ug]
version: 1.2.5
order : 3
description: Producers.
---

### Motivation

A [`KijiProducer`]({{site.api_mr_1_2_5}}/produce/KijiProducer.html) executes a function over a subset
of the columns in a table row and produces output to be injected back into a column of that row.
Producers can be run in a MapReduce job that operates over a range of rows from a Kiji table.
Common tasks for producers include parsing, profiling, recommending, predicting, and classifying.
For example, you might run a LocationIPProducer to compute and store the location of each user into
a new column, or a PersonalizationProfileProducer to compute a personalization profile.

Whereas gatherers generally run over the rows of a Kiji table to generate key-value pairs for the
purposes of conversion, analysis, or filling in information in a different table, producers can only
write back information directly into the row they're presently processing. Producers are the most
appropriate tool when you want to update a row with information calculated from the existing
contents of that row and KeyValueStores.

### Classes Overview

There are three important classes for an application that wants to use producers. All producers must
extend the abstract class `org.kiji.mapreduce.produce.KijiProducer` and override its abstract
methods as described below. Clients should be familiar with the
`org.kiji.mapreduce.producer.ProducerContext` class, which is the interface producers use to output
data. Finally, while produce jobs can be launched with `kiji produce` from the command line,
`org.kiji.mapreduce.produce.KijiProduceJobBuilder` can be used to construct and launch produce jobs
programatically.

### Using the API

Each producer must extend [`KijiProducer`]({{site.api_mr_1_2_5}}/produce/KijiProducer.html) and must
implement the following three methods:

 * `KijiDataRequest getDataRequest()`. This method specifies the columns retrieved while scanning
   from the input table. It should construct and return a
   [`KijiDataRequest`]({{site.api_schema_1_3_7}}/KijiDataRequest.html).
 * `String getOutputColumn()`. This method specifies the fully-qualified column or the column family
   being produced. It should return a string of the form "family:qualifier" or "family".
   Family-only output columns are only valid for map-type families (see the KijiSchema user guide).
   The `produce()` method can use its `context` argument to output to this column as detailed below.
 * `void produce(KijiRowData input, ProducerContext context)`. This method contains the logic to
   produce the content for the output column for each input row. It will be called once per row
   processed by the task. `input` contains columns from the row as requested by the
   [`KijiDataRequest`]({{site.api_schema_1_3_7}}/KijiDataRequest.html) returned from
   `getDataRequest()`.

When producing new content for a row, the producer may combine the input row data with data from
external stores through [`KeyValueStore`]({{site.api_mr_1_2_5}}/kvstore/KeyValueStore.html)s by
implementing `Map<String, KeyValueStore<?, ?>> getRequiredStores()`. This method should construct
and return a map specifying all the
[`KeyValueStore`]({{site.api_mr_1_2_5}}/kvstore/KeyValueStore.html)s that the producer wants to
access. The [`KeyValueStore`]({{site.api_mr_1_2_5}}/kvstore/KeyValueStore.html)s may then be accessed
from the `produce()` method through the
[`ProducerContext`]({{site.api_mr_1_2_5}}/produce/ProducerContext.html). For more details, you may
check the [Key-Value Stores]({{site.userguide_mapreduce_1_2_5}}/key-value-stores) section in this
guide.

Optionally, a producer may implement `setup()` and `cleanup()` to initialize and finalize resources
that can be reused during the produce task.  These methods will be called once by each task,
`setup()` before processing any rows and `cleanup()` after the task is done processing. If you wish
to use a [`KeyValueStore`]({{site.api_mr_1_2_5}}/kvstore/KeyValueStore.html), it should be opened once
with `context.getStore(storeName)` in `setup()`, saved in a member variable, and closed in
`cleanup()`.

As mentioned above, a Producer's `produce()` method has a
`org.kiji.mapreduce.producer.ProducerContext` argument. This class has a number of methods which are
important to a producer author:

* `void put(T value)`, `void put(long timestamp, T value)`, `void put(String qualifier, T value)`,
  and `void put(String qualifier, long timestamp, T value)`. Each of these calls put data into the
  current row in the column specified by the producer's `getOutputColumn()`. The type of `value`
  must be compatible with the output column's schema as declared in the table layout. The `timestamp`
  parameter is optional; if ommitted the current time will be used. A `qualifier` argument should
  only be used if the producer's `getOutputColumn()` specified a map-type family.
* `void incrementCounter(Enum<?> counter)`. Increments a MapReduce job counter. This can be useful
  for calculating aggregate counts about the full MapReduce job (for example, you can use a counter
  to calculate the total number of input rows containing malformed data). This method is common to
  all KijiContexts.

### Example

The following is a minimal example of a Kiji producer class. This producer is designed to calculate
and save the zodiac signs of people in a user table, and could be part of something like a horoscope
application. We assume the user table has a column `info:birthday` containing the user's birthdate
as a string and a map-type family of strings, `produced`. Our example producer will output the
zodiac sign as a string into `produced:zodiac_sign`.

For brevity, the calculation of a zodiac sign from a birthday string has been omitted.

{% highlight java %}
/**
 * Example of a producer class. Calculates zodiac signs.
 */
public static class ZodiacProducer extends KijiProducer {
  public static enum Counters {
    MISSING_BIRTHDAY,
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Fetch all columns in family 'info' from the input table:
    return KijiDataRequest.create("info");
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    // Configure the producer to emit to a single column 'produced:zodiac_sign':
    return "produced:zodiac_sign";
  }

  /** Compute the zodiac sign from a birthday. */
  private String zodiacSignFromBirthday(String birthday) {
    // Implementation left to the reader
    // …
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData row, ProducerContext context) throws IOException {
    // Extract the required data from the input row:
    final CharSequence birthday = input.getMostRecentValue("info", "birthday");
    if (birthday == null) {
      // Input row contains no birthday, report and move on:
      context.incrementCounter(Counters.MISSING_BIRTHDAY);
      return;
    }

    // Some computation:
    final String zodiacSign = zodiacSignFromBirthday(birthday.toString());

    // Emits the generated content to the configured output column:
    context.put(zodiacSign);
  }
}
{% endhighlight %}

Our example producer may be used on the command line with something like the following. This assumes
that our table is named `user_table` on the default Kiji instance

{% highlight bash %}
kiji produce \
    --producer=my.application.package.ZodiacProducer \
    --input="format=kiji table=kiji://.env/default/user_table" \
    --output="format=kiji table=kiji://.env/default/usertable nsplits=1"
{% endhighlight %}

Note: the output table of a producer must match the input table. The `nsplits` parameter is ignored
for producers.

### Provided Library Classes

The `org.kiji.mapreduce.lib.produce` package of the KijiMR Library contains a number of
producer implementations that might be of use to application developers:

* [`AllVersionsSingleInputProducer`]({{site.api_mrlib_1_1_4}}/produce/AllVersionsSingleInputProducer.html)
  and [`SingleInputProducer`]({{site.api_mrlib_1_1_4}}/produce/SingleInputProducer.html) are
  convenience classes. Subclasses of these abstract classes only have to implement `String
  getInputColumn()` instead of constructing an entire
  [`KijiDataRequest`]({{site.api_schema_1_3_7}}/KijiDataRequest.html) in `getDataRequest()`. The
  `produce()` method will receive all the versions of that column (if the parent class is
  [`AllVersionsSingleInputProducer`]({{site.api_mrlib_1_1_4}}/produce/AllVersionsSingleInputProducer.html))
  or the most recent (if the parent class is
  [`SingleInputProducer`]({{site.api_mrlib_1_1_4}}/produce/SingleInputProducer.html)).
* [`RegexProducer`]({{site.api_mrlib_1_1_4}}/produce/RegexProducer.html) is an abstract subclass of
  [`SingleInputProducer`]({{site.api_mrlib_1_1_4}}/produce/SingleInputProducer.html). Subclasses must
  implement `String getInputColumn()` to specify a column and `String getRegex()`, which should be a
  regular expression that matches the contents of the input column. The regex should have one
  capturing group. [`RegexProducer`]({{site.api_mrlib_1_1_4}}/produce/RegexProducer.html) contains a
  `produce()` method which will write that group to the output column. Both input and output should
  be strings.
* [`ConfiguredRegexProducer`]({{site.api_mrlib_1_1_4}}/produce/ConfiguredRegexProducer.html) is a
  concrete implementation of [`RegexProducer`]({{site.api_mrlib_1_1_4}}/produce/RegexProducer.html)
  which uses Configuration keys to specify its input, output, and regular expression. You can use
  this class to copy substrings from one column to another without writing Java code.
* [`IdentityProducer`]({{site.api_mrlib_1_1_4}}/produce/IdentityProducer.html) is another concrete
  producer. It copies data directly from its input column to its output column, both of which may be
  specified using configuration keys on the command line. This can be useful for clients who want to
  copy one column's data to another without writing code.

See the javadoc of these classes for more information about using them.
