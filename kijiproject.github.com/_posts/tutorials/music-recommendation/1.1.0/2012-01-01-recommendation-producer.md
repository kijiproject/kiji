---
layout: post
title : Music Recommendation Producer
categories: [tutorials, music-recommendation, 1.1.0]
tags: [music]
order : 7
description: Read and write to the same row of a table.
---

Now, to generate recommendations for each user, we define a map-only MapReduce job that will process
each row in the user table and apply our recommendation strategy to it.

<div id="accordion-container">
  <h2 class="accordion-header"> NextSongRecommender.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/kiji-music-1.1.0/src/main/java/org/kiji/examples/music/produce/NextSongRecommender.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> NextSongRecommender </h3>
The NextSongRecommender is an example of a [KijiProducer]({{site.userguide_mapreduce_1_2_0}}/producers).
A producer operates on a single row of input data and generates new outputs that are written to the
same row.  It can also refer to external sources of data via KeyValueStores in addition to the input from the row.
For every row this producer processes, it will:

* Read the most recent value from the "info:track_plays" column of the users table. This is the song
  ID of the most recently played song by the user.
* Look up a list of the songs most frequently played next from the songs table.
* Use external data sources (in this case the list of songs most frequently played next that we computed and wrote
to the "songs" table) to generate a recommendation for each user.
* Write that recommendation to the "info:next_song_rec" column of the users table.

#### Get The Most Recent Song Played
Like in a gatherer, you specify the required columns for your producer in the `getDataRequest` method. We
only want the most recent value from this column, so we can use the `create()` convenience method.
{% highlight java %}
  public KijiDataRequest getDataRequest() {
    // Only request the most recent version from the "info:track_plays" column.
    return KijiDataRequest.create("info", "track_plays");
  }
{% endhighlight %}

In our `produce()` method, we then access our requested data through the [`KijiRowData`]({{site.api_schema_1_3_0}}/KijiRowData.html):

{% highlight java %}
  String mostRecentSong = input.<CharSequence>getMostRecentValue("info", "track_plays")
      .toString();  // Avro strings get deserialized to CharSequences, so .toString() the result.
{% endhighlight %}

#### Join External Data Sources
[KeyValueStores]({{site.userguide_mapreduce_1_2_0}}/key-value-stores) allow you to access external data sources in a MapReduce job.
This is a common pattern in MapReduce jobs, as it allows us to integrate two sources of data. In this case, we will use the
"top_next_songs" column of our "songs" table as a KeyValueStore.

In order to access KeyValueStores in a KijiMR Job, the class that needs the external data must
implement KeyValueStoreClient. This interface requires that you implement getRequiredStores().
The value that you must return from getRequiredStores is a map from the name of a KeyValueStore to
the default implementation.

For reasons pertaining to [KijiMR-91](https://jira.kiji.org/browse/KIJIMR-91) we leave our default
implementation unconfigured.

{% highlight java %}
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return RequiredStores.just("nextPlayed", UnconfiguredKeyValueStore.builder().build());
  }
{% endhighlight %}

This default implementation must be overriden when this producer is run.
In the unit test, it is programmatically overriden using a job builder. When we run it from the
command line, we will override the default implementation using the KVStoreConfig.xml file.

#### Generate a Recommendation
To generate a recommendation from the list of songs that are most likely to be played next, we do
the simplest thing possible; choose the first element of the list.

{% highlight java %}
  private CharSequence recommend(List<SongCount> topNextSongs) {
    return topNextSongs.get(0).getSongId(); // Do the simplest possible thing.
  }
{% endhighlight %}

#### Write the Output to a Column
To write our recommendation to the table, we need to declare what column we are writing to.

{% highlight java %}
  public String getOutputColumn() {
    return "info:next_song_rec";
  }
{% endhighlight %}

Since the column is already declared, to write a value to it, we simply call context.put() with
the value we want to write as the parameter.

{% highlight java %}
  context.put(recommend(popularNextSongs));
{% endhighlight %}

<div id="accordion-container">
  <h2 class="accordion-header"> TestNextSongRecommender.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/kiji-music-1.1.0/src/test/java/org/kiji/examples/music/TestNextSongRecommender.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> TestNextSongRecommender </h3>
To test NextSongRecommender, we need specify which KijiTable we want to use to back our
KeyValueStore. We do this by constructing the KeyValueStore we want to use, via the KeyValueStore's
builder method. We then override the KeyValueStore binding in this job configuration by using the
withStore() method of JobBuilders.

{% highlight java %}
  KijiTableKeyValueStore.Builder kvStoreBuilder = KijiTableKeyValueStore.builder();
  kvStoreBuilder.withColumn("info", "top_next_songs").withTable(mSongTableURI);

  // Configure first job.
  final KijiMapReduceJob mrjob = KijiProduceJobBuilder.create()
      .withStore("nextPlayed", kvStoreBuilder.build())

  // ...
{% endhighlight %}

### Running the Example
When we run this example, we again need to specify which
[`KijiTable`]({{site.api_schema_1_3_0}}/KijiTable.html) we want to use to back our
KeyValueStore. This time, we will override the KeyValueStore binding from
the command line using an XML configuration file (located at ${KIJI_HOME}/examples/music/KVStoreConfig.xml).
The contents of the file are displayed below. If you are not using BentoBox, you may need to modify this
XML file so that the URI points to the songs table you would like to use.


{% highlight xml %}
<?xml version="1.0" encoding="UTF-8"?>
<stores>
  <store name="nextPlayed" class="org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore">
    <configuration>
      <property>
        <name>table.uri</name>
        <!-- This URI can be replace with the URI of a different 'songs' table to use. -->
        <value>kiji://.env/kiji_music/songs</value>
      </property>
      <property>
        <name>column</name>
        <value>info:top_next_songs</value>
      </property>
    </configuration>
  </store>
</stores>
{% endhighlight %}

Now, run the command:

<div class="userinput">
{% highlight bash %}
kiji produce \
    --producer=org.kiji.examples.music.produce.NextSongRecommender \
    --input="format=kiji table=${KIJI}/users" \
    --output="format=kiji table=${KIJI}/users nsplits=2" \
    --lib=${LIBS_DIR} \
    --kvstores=${MUSIC_HOME}/KVStoreConfig.xml
{% endhighlight %}
</div>

The input and output for the producer come from the Kiji table "users",
and the KeyValueStores are specified by the KVStoreConfig.xml file.

#### Verify

<div class="userinput">
{% highlight bash %}
kiji scan ${KIJI}/users/info:next_song_rec --max-rows=3
{% endhighlight %}

These are our recommendations for the next song to play for each user!
</div>

    entity-id='user-41' [1361564713968] info:next_song_rec
                                 song-41

    entity-id='user-3' [1361564713980] info:next_song_rec
                                 song-2

    entity-id='user-13' [1361564713990] info:next_song_rec
                                 song-27

