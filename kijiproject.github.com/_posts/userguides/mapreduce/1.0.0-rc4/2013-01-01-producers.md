---
layout: post
title: Producers
categories: [userguides, mapreduce, 1.0.0-rc4]
tags : [mapreduce-ug]
version: 1.0.0-rc4
order : 3
description: Producers.
---

### Motivation

A KijiProducer executes a function over a subset of the columns in a table row and produces output to be injected back into a column of that row.
Producers can be run in the context of a MapReduce over entire Kiji tables, or on-demand over a single row at a time.
Common tasks for producers include parsing, profiling, recommending, predicting, and classifying.
For example, you might run a LocationIPProducer to compute and store the location of each user into a new column,
or a PersonalizationProfileProducer to compute a personalization profile.

Whereas Gatherers generally run over the rows of a Kiji table to generate key value pairs for the purposes of conversion, analysis, or filling in information in a different table, Producers can only write back information directly into the row they're presently processing. Producers are the most appropriate tool when you want to update a row with information calculated from the existing contents of that row and KeyValueStores.

### Classes Overview

There are three important classes for an application that wants to use producers:

All producers must extend the abstract class `org.kiji.mapreduce.produce.KijiProducer` and override its abstract methods as described below. Clients should be familiar with the `org.kiji.mapreduce.producer.ProducerContext` class, which is the interface producers use to output data. Finally, while produce jobs can be launched with `kiji produce` from the commandline, `org.kiji.mapreduce.produce.KijiProduceJobBuilder` can be used to construct a  mapreduce job that uses a given producer. The job can then be launched programmatically.

### Using the API

Each producer must extend `KijiProducer` and must implement the following three methods:

 * `KijiDataRequest getDataRequest()`. This method specifies the columns retrieved while scanning from the input table. It should construct and return a `KijiDataRequest`.
 * `String getOutputColumn()`. This method specifies the fully qualified column or the column family 'being produced'. It should return a string of the form "family:qualifier" or "family". Family-only output columns are only valid for map-type families (see the KijiSchema user guide).
 * `void produce(KijiRowData input, ProducerContext context)`. This method contains the logic to produce the content for the output column for each input row. It will be called once per row processed by the task. `input` contains columns from the row as requested by the `KijiDataRequest` returned from `getDataRequest()`. The `produce()` method can use its `context` argument to output to this column as detailed below.

When producing new content for a row, the producer may combine the input row data with data from external stores through `KeyValueStore`s by implementing `Map<String, KeyValueStore<?, ?>> getRequiredStores()`. This method should construct and return a map specifying all the `KeyValueStore`s that this producer wants to access. The `KeyValueStore`s may then be accessed from the `produce()` method through the `ProducerContext`. For more details, you may check the [KeyValue Stores]({{site.userguide_mapreduce_rc4}}/key-value-stores) section in this guide.

Optionally, a producer may implement `setup()` and `cleanup()` to initialize and finalize resources that can be reused during the produce task.
These methods will be called once by each task, `setup()` before processing input row and `cleanup()` after the task is done processing. If you wish to use a `KeyValueStore`, it should be opened once with `context.getStore(storeName)` in `setup()`, saved in a member variable, and closed in `cleanup()`.

As mentioned above, a Producer's `produce()` method has a `org.kiji.mapreduce.producer.ProducerContext` argument. This class has a number of methods which are important to a producer author:

* `void put(T value)`, `void put(long timestamp, T value)`, `void put(String qualifier, T value)`, and `void put(String qualifier, long timestamp, T value)`. Each of these calls put data into the current row in the column specified by the producer's `getOutputColumn()`. The type of `value` must be compatible with the output column's type as declared in the table layout. The `timestamp` parameter is optional; if ommitted the current time will be used. A `qualifier` argument should only be used if the producer's `getOutputColumn()` specified a map-type family.
* `void incrementCounter(Enum<?> counter)`. Increments a mapreduce job counter. This can be useful for calculating aggregate counts about the full map reduce job (for example, you can use a counter to calculate the total number of input rows containing malformed data). This method is common to all KijiContexts.

### Example

Following is a minimal example of a Kiji producer class. This producer is designed to calculate and save the zodiac signs of people in a user table, and could be part of something like horoscope application. We assume the user table has a column `info:birthday` containing the user's birthdate as a string and a map-type family of strings, `produced`. Our example producer will output the zodiac sign as a string into `produced:zodiac_sign`.

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

Our example producer may be used on the command-line with something like the following. This assumes that our table is named `user_table` installed on the default Kiji instance

{% highlight bash %}
kiji produce \
    --producer=my.application.package.ZodiacProducer \
    --input="format=kiji table=kiji://.env/default/user_table" \
    --output="format=kiji table=kiji://.env/default/usertable nsplits=1"
{% endhighlight %}

Note: the output table of a producer must match the input table. The `nsplits` parameter is ignored for producers.

### Provided Library Classes

The `org.kiji.mapreduce.lib.produce` package of the Kiji MapReduce Library contains a number of producer implementations that might be of use to application developers:

* `AllVersionsSingleInputProducer` and `SingleInputProducer` are convenience classes. Subclasses of these abstract classes only have to implement `String getInputColumn()` instead of constructing an entire `KijiDataRequest` in `getDataRequest()`. The `produce()` method will receive all the versions of that column (if the parent class is `AllVersionsSingleInputProducer`) or the most recent (if the parent class is `SingleInputProducer`).
* `RegexProducer` is an abstract subclass of `SingleInputProducer`. Subclasses must implement `String getInputColumn()` to specify a column and `String getRegex()`, which should be a regular expression that matches the contents of the input column. The regex should have one capturing group. `RegexProducer` contains a `produce()` method which will write that group to the output column. Both input and output should be strings.
* `ConfiguredRegexProducer` is a concrete implementation of `RegexProducer` which uses Configuration keys to specify its input, output, and regular expression. You can use this class to copy substrings from one column to another without writing java code.
* `IdentityProducer` is another concrete producer. It copies data directly from its input column to its output column, both of which may be specified using configuration keys on the commandline. This can be useful for clients who want to copy one column's data to another without writing code.

See the javadoc of these classes for more information about using them.
