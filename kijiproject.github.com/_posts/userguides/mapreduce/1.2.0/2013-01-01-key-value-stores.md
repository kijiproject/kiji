---
layout: post
title: Key-Value Stores
categories: [userguides, mapreduce, 1.2.0]
tags : [mapreduce-ug]
version: 1.2.0
order : 8
description: Key-Value Stores.
---

### Motivation

KeyValueStores are used to provide MapReduce programs and other operators processing Kiji datasets with the ability to join datasets. One data set can be specified as a key-value store using the KeyValueStore API. The program can use a KeyValueStoreReader to look up values associated with keys. These keys are often driven by records of a dataset being processed by MapReduce.

This can be used, for example, to provide a [`KijiProducer`]({{site.api_mr_1_2_0}}/produce/KijiProducer.html) with the means to apply the results of a trained machine learning model to the main data set. The output of a machine learning model might be expressed as (key, value) pairs stored in files in HDFS, or in a secondary Kiji table. For each user in a users table, you may want to compute a new recommendation for the user by applying the model to the information in the user's row. A value in the user's row may be a key into some arbitrary key-value store representing the model; the returned value is the recommendation.

You may also need to perform "ordinary" map-side joins in a MapReduce program, e.g., for denormalization of data. The smaller dataset can be held in RAM in each map task in the form of a KeyValueStore. For each record in the larger dataset, you can look up the corresponding small-side record, and emit the concatenation of the two to the reducer.

`KeyValueStores` also allow non-MapReduce applications to read key-value pairs from HDFS-backed datasets in a file-format-agnostic fashion. For example, you may run a MapReduce program that emits output as a `SequenceFile` that your frontend application needs to consume. You could use a `SequenceFile.Reader` directly, but if you ever change your MapReduce pipeline to emit to text files or Avro files, you will need to rewrite your client-side logic. Using the KeyValueStoreReader API in your client allows you to decouple the act of using a key-value map from what format you need to use to read the data.

### Classes Overview

The main classes in the KeyValueStore API are `org.kiji.mapreduce.kvstore.KeyValueStore` and `org.kiji.mapreduce.kvstore.KeyValueStoreReader`.

A [`KeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStore.html) specifies all the resources needed to surface key-value pairs from some backing store. This may be defined on files, a Kiji table, or some other resource like a different NoSQL database.

Several KeyValueStore implementations are made available in the `org.kiji.mapreduce.kvstore.lib` package that cover common use cases for file- or Kiji-backed datasets. You could write your own [`KeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStore.html) implementation that accesses a foreign system (e.g., a Redis database).

A [`KeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStore.html) class specifies how to read data into the store. For example, [`TextFileKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/TextFileKeyValueStore.html) expects to parse delimited text files, while [`SeqFileKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/SeqFileKeyValueStore.html) reads SequenceFiles. Both of these stores are configured with an HDFS path to read from. A [`KijiTableKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/KijiTableKeyValueStore.html) requires different configuration.

The KeyValueStore implementations provided in the library are immutable; they’re all created through builder classes. They each have a method named `builder()` that returns a new instance of the associated builder class.

The [`KeyValueStoreReader`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStoreReader.html) API is used to actually look up values by key, from some KeyValueStore. You cannot directly instantiate any concrete implementations of [`KeyValueStoreReader`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStoreReader.html) yourself; use a given KeyValueStore's `open()` method to open an associated reader object. The client is agnostic to the backing store; one [`KeyValueStoreReader`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStoreReader.html) should act the same as the next, given equivalent backing data.

By default, a KeyValueStoreReader's data is presented to you as a read-only, non-iterable map. Only `get()` requests for an explicit key are supported by default, though some implementations may offer iteration.

An opened KeyValueStoreReader may contain state or connect to external resources; you should call the `close()` method when you are finished using it.

#### Simple Example for Opening a KeyValueStore

{% highlight java %}
    final KeyValueStore<String, String> csvKeyValues = TextFileKeyValueStore.builder()
        .withInputPath(new Path("some-file.txt"))
        .withDistributedCache(false)
        .build();

    final KeyValueStoreReader<String, String> reader = csvKeyValues.open();
    try {
      String theValue = reader.get("some-key");
      System.out.println("Contained a mapping: some-key -> " + theValue);
    } finally {
      reader.close();
    }
{% endhighlight %}

### Using the API

The distinction between KeyValueStore and KeyValueStoreReader is intentional. A given MapReduce program may need to access some set of KeyValueStoreReaders. When the job is configured, you must configure it with any associated KeyValueStores. For example, file-backed stores often use the DistributedCache to efficiently copy files to all map tasks. This must be configured before the job begins.

Your MapReduce task class (KijiMapper, KijiReducer, KijiProducer, KijiGatherer, etc) can implement [`KeyValueStoreClient`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStoreClient.html) to help specify to a Kiji job builder (e.g., [`KijiGatherJobBuilder`]({{site.api_mr_1_2_0}}/gather/KijiGatherJobBuilder.html)) that it requires stores. This will require that you implement a method named `getRequiredStores()` that returns a mapping from names to KeyValueStores. These are the default _bindings_ for KeyValueStores.

The use of Java generic types makes constructing the return value from your `getRequiredStores()` method to be cumbersome. Some static factory methods have been added for your convenience in the [`RequiredStores`]({{site.api_mr_1_2_0}}/kvstore/RequiredStores.html) class. For example, to require no stores:

{% highlight java %}
    @Override Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.none();
    }
{% endhighlight %}

To require one empty store named `"mystore"`:

{% highlight java %}
    @Override Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      RequiredStores.just("mystore", EmptyKeyValueStore.get());
    }
{% endhighlight %}

The `RequiredStores.with()` method will return a `Map` object augmented with a `with(String, KeyValueStore)` method, so you can call `RequiredStores.with("foo", store1).with("bar", store2);`.

Within the `KijiGatherer.gather()` method, the [`KijiContext`]({{site.api_mr_1_2_0}}/KijiContext.html) object provided as an argument will provide you with access to KeyValueStoreReaders. Since you may need multiple KeyValueStores, you can refer to each by the name you bound it to in `getRequiredStores()`.

Since each call to `gather()` is likely to require the same stores, it would be high-overhead to open and close a KeyValueStoreReader in every call. The KijiContext itself uses a class called [`KeyValueStoreReaderFactory`]({{site.api_mr_1_2_0}}/kvstore/KeyValueStoreReaderFactory.html), which is responsible for instantiating KeyValueStores from the `Configuration` associated with the job, and maintaining a pool of lazily-opened KeyValueStoreReader instances organized by name binding. The gatherer itself does not close individual KeyValueStoreReader instances. The KeyValueStoreReaderFactory will close all of them as the task is being torn down.

#### Overriding Default Bindings in Job Builders

While the `getRequiredStores()` method allows you to define bindings between names and implementations, specific invocations of the MapReduce job may require that you override these implementations. For example, you may want to use one HDFS path as input in production, but a different HDFS path in local tests.

The [`MapReduceJobBuilder`]({{site.api_mr_1_2_0}}/framework/MapReduceJobBuilder.html) subclasses all support a method called `withStore(String name, KeyValueStore store)`. This method allows you to specify a different KeyValueStore implementation for that particular job. You will need to override a store name that the job will expect to read; i.e., if `getRequiredStores()` returned a binding from `"mystore"` to a particular [`TextFileKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/TextFileKeyValueStore.html), you should call `myJobBuilder.withStore("mystore", myDifferentKeyValueStore);`.

#### Overriding Default Bindings on the Command Line

You can also override KeyValueStore bindings on the command line with the `--kvstores` argument. This argument specifies an XML file that should look like the following:

{% highlight xml %}
<stores>
  <store name="mystore" class="org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore">
    <configuration>
      <property>
        <name>paths</name>
        <value>/path/to/foo.txt,/path/to/bar.txt</value>
      </property>
      <property>
        <name>delim</name>
        <value>,</value>
      </property>
    </configuration>
  </store>
</stores>
{% endhighlight %}


This example defines a delimited-text-file store, bound to the name `"mystore"`. It reads from `foo.txt` and `bar.txt` and expects a comma between the key and value fields on each line.

You can define multiple name-to-store bindings with different `<store>` blocks with unique names. The properties available within each store's configuration is specified in the Javadoc for each KeyValueStore class.

#### Requiring Runtime Configuration of KeyValueStores

If you know that your KijiProducer requires a store named `"mystore"`, but do not know at compile time where the physical resource that backs it will be, you may want to force your clients to specify this at runtime using one of the above two methods. A mapping of `RequiredStores.just("mystore", UnconfiguredKeyValueStore.get());` will throw an exception when it is serialized to the `Configuration`, which ensures that your job cannot start unless you override the definition with an XML file or a binding in a job builder.


### Example

In this example, suppose we have computed a set of movie sequels, which we will use to naively recommend to users on our web site that if they've watched the first one, they watch the sequel. Suppose we stored the movie sequel dataset in pipe-delimited text files in HDFS, that look something like:

    Star Wars: A New Hope|Star Wars: The Empire Strikes Back
    Raiders of the Lost Ark|Indiana Jones and the Temple of Doom
    ...

We could write a KijiProducer that read from the `info:recently_watched` column and produce a field named `info:recommended_movie` as follows:

{% highlight java %}
    import java.io.IOException;

    import org.apache.hadoop.fs.Path;

    import org.kiji.mapreduce.*;
    import org.kiji.mapreduce.kvstore.*;
    import org.kiji.mapreduce.kvstore.lib.*;
    import org.kiji.schema.*;

    /**
     * Produce a movie recommendation based on the theory that if they liked
     * the first one, they should watch the sequel.
     */
    public class MoveRecProducer extends KijiProducer {
      @Override public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
        // Note that it's ok to specify a path to an HDFS dir, not just one file.
        return RequiredStores.just("sequels", TextFileKeyValueStore.builder()
            .withDelimiter("|")
            .withInputPath(new Path("/path/to/movie-sequels.txt")).build());
      }

      @Override KijiDataRequest getDataRequest() {
        return KijiDataRequest.create("info", "recently_watched");
      }

      @Override String getOutputColumn() {
        return "info:recommended_movie";
      }

    @Override void produce(KijiRowData input, ProducerContext context)
        throws IOException {

        if (!input.containsColumn("info", "recently_watched")) {
          // No basis for recommendation.
          return;
        }

        // Note that we don't call our own getRequiredStores() method. That was used
        // in the configuration phase of the job, not within each task. Its output may have
        // been overridden at run-time by the user. We take the deserialized store from the
        // ProducerContext instead.
        KeyValueStoreReader<String, String> sequelStore = context.getStore("sequels");

        String lastWatched = input.getMostRecentValue("info", "recently_watched").toString();
        String nextMovie = sequelStore.get(lastWatched);
        if (null != nextMovie) {
          // We found a match! Write to info:recommended_movie
          context.write(nextMovie);
        }
      }
    }
{% endhighlight %}


### Provided Library Classes

Several implementations of KeyValueStore are available in the `org.kiji.mapreduce.kvstore.lib` package:

Several file-backed KeyValueStore implementations provide access to stores in different file formats:

* [`AvroKVRecordKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/AvroKVRecordKeyValueStore.html) - Key-Value pairs specified in Avro records containing two fields, `key` and `value`
* [`AvroRecordKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/AvroRecordKeyValueStore.html) - Avro records in an Avro file, to be indexed by a configurable field of each record.
* [`SeqFileKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/SeqFileKeyValueStore.html) - Key-Value pairs in SequenceFiles
* [`TextFileKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/TextFileKeyValueStore.html) - string key-value pairs in delimited text files

You can also access a specific column of a Kiji table by the row's entityId using the [`KijiTableKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/KijiTableKeyValueStore.html).

If you want to declare a name binding on a KeyValueStore whose exact configuration cannot be determined before runtime, use the [`UnconfiguredKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/UnconfiguredKeyValueStore.html). It will throw an IOException in its `storeToConf()` method, ensuring that your [`MapReduceJobBuilder`]({{site.api_mr_1_2_0}}/framework/MapReduceJobBuilder.html) must call `withStore()` to override the definition before launching the job.

The [`EmptyKeyValueStore`]({{site.api_mr_1_2_0}}/kvstore/lib/EmptyKeyValueStore.html) is a good default choice when you plan to override the configuration at runtime, but find it acceptable to operate without this information.
