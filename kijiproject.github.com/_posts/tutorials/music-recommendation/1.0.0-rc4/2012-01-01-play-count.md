---
layout: post
title : PlayCount
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order : 4
description: Like WordCount, for songs.
---

### The 'Hello World!' of MapReduce
To quote Scalding Developers
[Hadoop is a distributed system for counting words.](https://github.com/twitter/scalding)
Unfortunately, we here at Pandorify are fresh out of words, but have the play history of
bajillions of users listening to bazillions of different songs.

This MapReduce job uses the listening history of our users that we have stored in the "users" Kiji
table to calculate the total number of times each song has been played. The result of this computation
is written to a text file in HDFS.

<div id="accordion-container">
  <h2 class="accordion-header"> SongPlayCounter.java </h2>
     <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/kiji-music-1.0.0-rc4/src/main/java/org/kiji/examples/music/gather/SongPlayCounter.java"> </script>
     </div>
 <h2 class="accordion-header"> LongSumReducer.java </h2>
   <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-mapreduce-lib/raw/kiji-mapreduce-lib-root-1.0.0-rc4/kiji-mapreduce-lib/src/main/java/org/kiji/mapreduce/lib/reduce/LongSumReducer.java"> </script>
    </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> SongPlayCounter </h3>

The SongPlayCounter is an example of a [Gatherer]({{site.userguide_mapreduce_rc4}}/gatherers). A
gatherer is essentially a mapper that gets input from a [`KijiTable`]({{site.api_schema_rc4}}/KijiTable.html).

SongPlayCounter proceeds through discrete stages:
* Setup reusable resources.
* Read all values from column: "info:track_plays".
* Process the data from "info:track_plays" and emit a key-value pair for each track ID each time
  it occurs.

#### Initialize Resources
First, SongPlayCounter prepares any resources that may be needed by the gatherer.  In Hadoop,
reusable objects are commonly instantiated only once to protect against long garbage collection
pauses. This is particularly important with Kiji because long garbage collection pauses can cause
MR jobs to fail because various underlying HBase resources timeout or cannot be found.

Since setup() is an overriden method, we call super.setup() to ensure that all resources are
initialized properly.  If you open resources in setup(), be sure to close them in the corresponding
cleanup() method.

{% highlight java %}
  public void setup(GathererContext<Text, LongWritable> context) throws IOException {
    super.setup(context); // Any time you override setup, call super.setup(context);
    mText = new Text();
  }
{% endhighlight %}

#### Read track play data from the table
A gatherer takes input from a table, so it must declare what data it will need. It does this in the
form of a [`KijiDataRequest`]({{site.api_schema_rc4}}/KijiDataRequest.html), which is defined in getDataRequest().
For the song count job, we want to request all songs that have been played, for every user. In order
to get *all* of the values written to the "info:track_plays" column, we must specify that the maximum
number of versions we want. The special constant that specifies that you want all versions of data
in a column is `HConstants.ALL_VERSIONS`. Otherwise, we will only get the most recent
version by default.

{% highlight java %}
public KijiDataRequest getDataRequest() {
  // This method is how we specify which columns in each row the gatherer operates on.
  // In this case, we need all versions of the info:track_plays column.
  final KijiDataRequestBuilder builder = KijiDataRequest.builder();
  builder.newColumnsDef()
    .withMaxVersions(HConstants.ALL_VERSIONS) // Retrieve all versions.
    .add("info", "track_plays");
  return builder.build();
}
{% endhighlight %}

#### Process track play data into key-value pairs for occurrences
Called once for each row in the Kiji table, gather() retrieves all the values in the
"info:track_plays" column, and for each value, sets the Text object we are resuing to contain the
current value, writes the key-value pairs using `context.write(mText, ONE)` and then clears the Text
object before the next call to gather.
{% highlight java %}
  public void gather(KijiRowData row, GathererContext<Text, LongWritable> context)
      throws IOException {
    // The gather method operates on one row at a time.  For each user, we iterate through
    // all their track plays and emit a pair of the track ID and the number 1.
    NavigableMap<Long, CharSequence> trackPlays = row.getValues("info", "track_plays");
    for (CharSequence trackId : trackPlays.values()) {
      mText.set(trackId.toString());
      context.write(mText, ONE);
      mText.clear();
    }
  }
{% endhighlight %}

### LongSumReducer
The key-value pairs emitted from the gatherer are shuffled and sorted by the MapReduce framework,
so each call to the reducer is given a key and an iterator of all values associated with
a key. The LongSumReducer calls reduce() for each key and sums all of the associated values to produce a
total play count for each song ID. The LongSumReducer has three stages:
* Setup reusable resources.
* Sum the values associated with a key.
* Output the key paired with the sum.

Because summing values is such a common MapReduce operation, LongSumReducer is provided by the KijiMR
library.

#### Initialize Resources
It is common practice to avoid instantiating new objects in map or reduce methods as
Hadoop developers have a (perhaps now outdated) skepticism of garbage collection in the JVM.

{% highlight java %}
  protected void setup(Context context) {
    mValue = new LongWritable();
  }
{% endhighlight %}

#### Sum Values and Output Total
Called once for each key, reduce() combines the
all of the values associated with a key by adding then together and writing the total for each key
to the output collector.

{% highlight java %}
  public void reduce(K key, Iterator<LongWritable> values,
                     OutputCollector<K, LongWritable> output,
                     Reporter reporter)
    throws IOException {

    // sum all values for this key
    long sum = 0;
    while (values.hasNext()) {
      sum += values.next().get();
    }

    // output sum
    output.collect(key, new LongWritable(sum));
  }
{% endhighlight %}

### TestSongPlayCounter
To verify that SongPlayCounter performs as expected, SongPlayCounter's test:
* Creates and populates an in-memory Kiji instance.
* Runs a MapReduce job with SongPlayCounter as the gatherer and LongSumReducer as the reducer.
* Verifies that the output is as expected.

<div id="accordion-container">
  <h2 class="accordion-header"> TestSongPlayCounter.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/kiji-music-1.0.0-rc4/src/test/java/org/kiji/examples/music/TestSongPlayCounter.java"> </script>
    </div>
</div>

<h4 style="margin-top:0px;padding-top:10px;"> Create an in-memory Kiji instance </h4>
The InstanceBuilder class provides methods for populating a test Kiji instance. Once the test
instance has been defined, its build method is called, creating the in-memory instance and
table.

{% highlight java %}
  public final void setup() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String tableName = layout.getName();
    mTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(tableName).build();

    new InstanceBuilder(getKiji())
        .withTable(tableName, layout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-1")
                .withValue(2L, "song-2")
                .withValue(3L, "song-3")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-1")
                .withValue(2L, "song-3")
                .withValue(3L, "song-4")
                .withValue(4L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-5")
        .build();
  }
{% endhighlight %}

#### Run and verify SongPlayCounter
KijiGatherJobBuilder is used to create a test MapReduce job. This job builder can be used outside
the context of a test to configure and run jobs programmatically. The job is then run using Hadoop's
local job runner. The resulting output sequence file is then validated.

{% highlight java %}
  final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
  final MapReduceJob mrjob = KijiGatherJobBuilder.create()
      .withConf(getConf())
      .withGatherer(SongPlayCounter.class)
      .withReducer(LongSumReducer.class)
      .withInputTable(mTableURI)
      // Note: the local map/reduce job runner does not allow more than one reducer:
      .withOutput(new SequenceFileMapReduceJobOutput(new Path("file://" + outputDir), 1))
      .build();
  assertTrue(mrjob.run());

  final Map<String, Long> counts = Maps.newTreeMap();
  readSequenceFile(new File(outputDir, "part-r-00000"), counts);
  LOG.info("Counts map: {}", counts);
  assertEquals(5, counts.size());
  assertEquals(3L, (long) counts.get("song-1"));
  assertEquals(1L, (long) counts.get("song-2"));
  assertEquals(2L, (long) counts.get("song-3"));
  assertEquals(1L, (long) counts.get("song-4"));
  assertEquals(1L, (long) counts.get("song-5"));
{% endhighlight %}

### Running the Example

<div class="userinput">
{% highlight bash %}
kiji gather \
      --gatherer=org.kiji.examples.music.gather.SongPlayCounter \
      --reducer=org.kiji.mapreduce.lib.reduce.LongSumReducer \
      --input="format=kiji table=${KIJI}/users" \
      --output="format=text file=output.txt_file nsplits=2" \
      --lib=${LIBS_DIR}
{% endhighlight %}
</div>

#### Verify

To confirm that the gather job worked, examine the output using hadoop filesystem command line tools:

<div class="userinput">
{% highlight bash %}
hadoop fs -text output.txt_file/part-r-00000 | head -3
{% endhighlight %}
</div>

    song-1  100
    song-10 272
    song-12 101
