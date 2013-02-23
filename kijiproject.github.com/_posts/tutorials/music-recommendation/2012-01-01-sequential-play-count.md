---
layout: post
title : SequentialPlayCount
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order : 5
description: Includes info on working with Avro
---

Instead of recommending the most popular songs to everyone using our service, we want to tailor our
recommendations based on user's listening history. For every user, we will lookup the most recent
song they have listened to and then recommend the song most frequently played after it. In order
to do that, we need to create an index so that for each song, we can quickly look up what the
most popular songs to listen to afterwards are.

So, we need to count the number of times two songs have been played, one after another. The
SequentialSongCount mapper and reducer allow us to do that.

<div id="accordion-container">
  <h2 class="accordion-header"> SequentialPlayCounter.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/main/java/org/kiji/examples/music/gather/SequentialPlayCounter.java"> </script>
    </div>
  <h2 class="accordion-header"> SequentialPlayCountReducer.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/main/java/org/kiji/examples/music/reduce/SequentialPlayCountReducer.java"> </script>
    </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> SequentialPlayCounter.java </h3>
SequentialPlayCounter.java operates in much the same way that SongPlayCounter.java does, but is
requires a more complex key structure to store both the song played and the song that followed.
The easiest way work with complex keys in Kiji is to use [Avro](http://avro.apache.org).
We define a SongBiGram, that will serve as our key, as a pair of songs played sequentially by a
single user.

{% highlight js %}
  /** Song play bigram. */
  record SongBiGram {
    /** The id of the first song played in a sequence. */
    string first_song_played;

    /** The id of the song played immediately after it. */
    string second_song_played;
  }
{% endhighlight %}

Where SongPlayCounter's output value class was Text.class, SequentialPlayCounter uses AvroKey.class
which requires that we also implement AvroKeyWriter and override getAvroKeyWriterSchema() to
fully define the Avro key format.

SequentialPlayCounter executes the same basic stages as SongPlayCounter, but with a more complex
gather operation.

#### Read track play data and compose complex keys
SequentialPlayCounter reads the same data as SongPlayCounter, but maintains a "sliding window"
of the most recent two track ids.  For each song after the first, gather() emits a key/value pair
where the key is a SongBiGram of the two most recently played songs, and the value is one (1) as a
tally.

{% highlight java %}
  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData input, GathererContext<AvroKey<SongBiGram>, LongWritable> context)
      throws IOException {
    CharSequence firstSong = null;
    CharSequence nextSong = null;
    NavigableMap<Long, CharSequence> trackPlays = input.getValues("info", "track_plays");
    for (CharSequence trackId : trackPlays.values()) { // Iterate through this user's track plays.
      // Slide the window one song over.
      firstSong = nextSong;
      nextSong = trackId;
      // If firstSong is null, we are at the beginning of the list and our sliding window
      // only contains one song, so don't output it. Otherwise...
      if (null != nextSong) {
        // Create the bigram of these two songs.
        mBiGram.setFirstSongPlayed(firstSong);
        mBiGram.setSecondSongPlayed(nextSong);
        // Emit the bigram of these two songs.
        context.write(new AvroKey<SongBiGram>(mBiGram), ONE);
      }
    }
  }
{% endhighlight %}

### SequentialPlayCountReducer.java
This reducer takes in pairs of songs that have been played sequentially and the number one.
It then computes the number of times those songs have been played together, and emits the id of
the first song as the key, and a SongCount record representing the song played after the first as
the value. A SongCount record has a field containing the id of the subsequent song and a field
for the number of times it has been played after the initial song.

This reducer takes AvroKeys as input, and writes AvroKeys and AvroValues as output, so it must
implement AvroKeyReader, AvroKeyWriter, and AvroValueWriter. The keys we are emiting are just strings
so we could use a [Text](link-to-text-key-docs) key. Instead, we made the choice to use an AvroKey
so that we could use the Kiji defined [AvroKeyValue output format]({{site.userguide_mapreduce_rc4}}/command-line-tools/#output), which
requires that you output AvroKeys and AvroValues.

The schema for our avro key is so simple that we don't have to add a record to our .avdl file
in order to return the correct schema in getWriterSchema(). Instead, we can use the static methods
avro provides for creating schemas of primitive types.

{% highlight java %}
  public Schema getAvroKeyWriterSchema() throws IOException {
    // Programmatically retrieve the avro schema for a String.
    return Schema.create(Schema.Type.STRING);
  }
{% endhighlight %}

#### Sum Sequential Plays
SequentialPlayCountReducer starts with the same reduction operation that LongSumReducer used to
count track plays in the Song Count example, but diverges when emitting key/value pairs.  Instead
of passing the keys through the reducer, SequentialPlayCountReducer creates new keys based on the
track ids in the SongBiGram keys.  The new keys are simply the first track id from each biGram,
while the second track id becomes part the SongCount value.

{% highlight java %}
  protected void reduce(AvroKey<SongBiGram> key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    // Initialize sum to zero.
    long sum = 0L;
    // Add up all the values.
    for (LongWritable value : values) {
      sum += value.get();
    }

    // Set values for this count.
    final SongBiGram songPair = key.datum();

    final SongCount nextSongCount = SongCount.newBuilder()
         .setCount(sum)
         .setSongId(songPair.getSecondSongPlayed())
         .build();
    // Write out result for this song.
    context.write(
        new AvroKey<CharSequence>(songPair.getFirstSongPlayed().toString()),
        new AvroValue<SongCount>(nextSongCount));
  }
{% endhighlight %}

### TestSequentialSongPlayCounter.java
To verify that SequentialPlayCounter and SequentialPlayCountReducer function as expected, their
test:
* Creates and populates an in-memory Kiji instance
* Runs a MapReduce job with SequentialPlayCounter as the gatherer and SequentialPlayCountReducer as the reducer
* Verifies that the output is as expected

<div id="accordion-container">
  <h2 class="accordion-header"> TestSequentialSongPlayCounter.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/test/java/org/kiji/examples/music/TestSequentialSongPlayCounter.java"> </script>
  </div>
</div>

<h4 style="margin-top:0px;padding-top:10px;"> Create an in-memory Kiji instance </h4>
The InstanceBuilder class provides methods for populating a test Kiji instance. Once the test
instance has been defined, its build method is called, creating the in-memory instance and
table.

{% highlight java %}
  public final void setup() throws Exception {
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();

    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-3")
                .withValue(3L, "song-2")
                .withValue(4L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-5")
        .build();
  }
{% endhighlight %}

#### Run and verify SequentialPlayCounter and SequentialPlayCountReducer
KijiGatherJobBuilder is used to create a test MapReduce job. This job builder can be used outside
the context of a test to configure and run jobs programatically. The job is then run using Hadoop's
local job runner. The resulting output sequence file is then validated.

{% highlight java %}
  // Configure and run job.
  final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
  final Path path = new Path("file://" + outputDir);
  final MapReduceJob mrjob = KijiGatherJobBuilder.create()
      .withConf(getConf())
      .withGatherer(SequentialPlayCounter.class)
      .withReducer(SequentialPlayCountReducer.class)
      .withInputTable(mUserTableURI)
      // Note: the local map/reduce job runner does not allow more than one reducer:
      .withOutput(new AvroKeyValueMapReduceJobOutput(new Path("file://" + outputDir), 1))
      .build();
  assertTrue(mrjob.run());
{% endhighlight %}

Reading back files is easy with normal file or table readers, currently avrokv files can be read
in a limited way using a KeyValueStoreReader.

{% highlight java %}
  AvroKVRecordKeyValueStore.Builder kvStoreBuilder = AvroKVRecordKeyValueStore.builder()
      .withInputPath(path).withConfiguration(getConf());
  final AvroKVRecordKeyValueStore outputKeyValueStore = kvStoreBuilder.build();
  KeyValueStoreReader reader = outputKeyValueStore.open();

  // Check that our results are correct.
  assertTrue(reader.containsKey("song-1"));
  SongCount song1Result = (SongCount) reader.get("song-1");
  assertEquals(2L, song1Result.getCount().longValue());
  // Avro strings are deserialized to CharSequences in Java, .toString() allows junit to correctly
  // compare the expected and actual values.

  assertEquals("song-2", song1Result.getSongId().toString());
  assertTrue(reader.containsKey("song-2"));
  SongCount song2Result = (SongCount) reader.get("song-2");
  assertEquals(1L, song2Result.getCount().longValue());
  // Avro strings are deserialized to CharSequences in Java, .toString() allows junit to correctly
  // compare the expected and actual values.
  assertEquals("song-3", song2Result.getSongId().toString());
{% endhighlight %}

### Running the Example

<div class="userinput">
{% highlight bash %}
kiji gather \
      --gatherer=org.kiji.examples.music.gather.SequentialPlayCounter \
      --reducer=org.kiji.examples.music.reduce.SequentialPlayCountReducer \
      --input="format=kiji table=${KIJI}/users" \
      --output="format=avrokv file=output.sequentialPlayCount nsplits=2" \
      --lib=${LIBS_DIR}
{% endhighlight %}
</div>

#### Verify
Because this job outputs avrokv files, which are binary and hard to read directly, we can use the
Hadoop job tracker to verify the success of the job.  Using your favorite browser, navigate to
the jobtracker page ([localhost:50030 by default](http://localhost:50030)).  This is where you can
monitor all your Hadoop jobs. Locate the Kiji gather: SequentialPlayCounter /
SequentialPlayCountReducer job and navigate to the job page by clicking on the Job ID.  On the
job page, check that Map output records is roughly 7,000.
