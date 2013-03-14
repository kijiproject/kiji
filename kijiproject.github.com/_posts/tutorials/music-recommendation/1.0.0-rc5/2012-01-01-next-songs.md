---
layout: post
title : NextTopSong
categories: [tutorials, music-recommendation, 1.0.0-rc5]
tags: [music]
order : 6
description: Outputing to a Kiji table in a MapReduce job.
---

This MapReduce job processes the result of the SequentialSong MapReduce job and writes a list of
the top songs played after each song (the key) to the corresponding row in the songs table.

<div id="accordion-container">
  <h2 class="accordion-header"> IdentityMapper.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/main/java/org/kiji/examples/music/map/IdentityMapper.java"> </script>
    </div>
  <h2 class="accordion-header"> TopNextSongsReducer.java </h2>
   <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/main/java/org/kiji/examples/music/reduce/TopNextSongsReducer.java"> </script>
    </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> IdentityMapper.java </h3>
This is a stunning homage to Java boilerplate. This mapper is the identity function; it just
emits the same keys and values as it receives without changing them.


### TopNextSongsReducer.java
The keys passed into this reducer are song ids and the values are SongCount records. In order to
find the songs most frequently played after a given song, we need to identify the SongCount
records with the largest number of counts, for every key.

To do this efficiently, we will maintain an ordered collection of SongCount records, that has a maximum
size. As we iterate through all the values, we will keep the top SongCount records seen so far
in our ordered collection.

This reducer
* Creates an ordered collection that will maintain a list of the top SongCount records, for each key.
* Examines each value for a key, and maintains a running list of the top SongCount records seen so
  far.
* Write a TopNextSongs record to the songs table.


#### Create an ordered Collection
In out setup method, we instantiate a TreeSet that will be reused. TreeSets use their comparator
(as opposed to a class' equals method) to determine if an element is already in the set. In order
for our TreeSet to contain multiple SongCount records with the same count, we must make sure
that our comparator differentiates SongCount records with the same number of counts, but with
different song ids.

{% highlight java %}
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context); // Any time you override setup, call super.setup(context);
    mTopSongs = new TopSongs();
    // This TreeSet will keep track of the "largest" SongCount objects seen so far. Two SongCount
    // objects, song1 and song2, can be compared and the object with the largest value in the field
    // count will the declared the largest object.
    mTopNextSongs = new TreeSet<SongCount>(new Comparator<SongCount>() {
      @Override
      public int compare(SongCount song1, SongCount song2) {
        if (song1.getCount().compareTo(song2.getCount()) == 0) {
          return song1.getSongId().toString().compareTo(song2.getSongId().toString());
        } else {
          return song1.getCount().compareTo(song2.getCount());
        }
      }
    });
  }
{% endhighlight %}

#### Maintain a collection of the top SongCount records
To find the top N songs, we iterate through the values associated with a given key, adding that
value to our set, and then removing the smallest value if our set is larger than the number of top
SongCount records we want to find.

It is worth pointing out that when you call value.datum(), the *same* SongCount record, with
different fields, will be returned.  Many Hadoop projects reuse objects, so be aware! To get around
the problem that this creates with trying to use a set, we create a new SongCount record for each
value using SongCount's builder method.

{% highlight java %}
  protected void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<SongCount>> values,
      KijiTableContext context) throws IOException {
    // We are reusing objects, so we should make sure they are cleared for each new key.
    mTopNextSongs.clear();

    // Iterate through the song counts and track the top ${mNumberOfTopSongs} counts.
    for (AvroValue<SongCount> value : values) {
      // Remove AvroValue wrapper.
      SongCount currentSongCount = SongCount.newBuilder(value.datum()).build();

      mTopNextSongs.add(currentSongCount);
      // If we now have too many elements, remove the element with the smallest count.
      if (mTopNextSongs.size() > mNumberOfTopSongs) {
        mTopNextSongs.pollFirst();
      }
    }
    // Set the field of mTopSongs to be a list of SongCounts corresponding to the top songs played
    // next for this key/song.
    mTopSongs.setTopSongs(Lists.newArrayList(mTopNextSongs));
{% endhighlight %}

#### Write TopNextSongs to the songs table.
We can write the list of top next songs to the "info:top_next_songs" column using context.put(). The
only thing to remember witht his method, is that the first arguement is expected to be an entityId.
Luckily, context also contains methods for generating EntityIds.

{% highlight java%}
    ...
    // Write this to the song table.
    context.put(context.getEntityId(key.datum().toString()), "info", "top_next_songs", mTopSongs);
  }
{% endhighlight %}

<div id="accordion-container">
  <h2 class="accordion-header"> TestTopNextSongsPipeline.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-music/raw/master/src/test/java/org/kiji/examples/music/TestTopNextSongsPipeline.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> TestTopNextSongsPipeline.java </h3>
Two jobs are constructed during this test and run one after another. The first job outputs to an
intermediate Avro container file (Add link to relevant userguide section) written to the local file system which is used as input by the
second job. Each of the jobs is configured using a job builder:

{% highlight java %}
  // Configure and run job.
  final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
  final Path path = new Path("file://" + outputDir);
  // Configure first job.
  final MapReduceJob mrjob1 = KijiGatherJobBuilder.create()
      .withConf(getConf())
      .withGatherer(SequentialPlayCounter.class)
      .withReducer(SequentialPlayCountReducer.class)
      .withInputTable(mUserTableURI)
      // Note: the local map/reduce job runner does not allow more than one reducer:
      .withOutput(new AvroKeyValueMapReduceJobOutput(path, 1))
      .build();
  // Configure second job.
  final MapReduceJobOutput tableOutput = new DirectKijiTableMapReduceJobOutput(mSongTableURI, 1);
  final MapReduceJob mrjob2 = KijiMapReduceJobBuilder.create()
      .withConf(getConf())
      .withInput(new AvroKeyValueMapReduceJobInput(path))
      .withMapper(IdentityMapper.class)
      .withReducer(TopNextSongsReducer.class)
      .withOutput(tableOutput).build();

  // Run both jobs and confirm that they are successful.
  assertTrue(mrjob1.run());
  assertTrue(mrjob2.run());
{% endhighlight %}

The results of these two jobs end up being written to a Kiji table. To validate the output data
a KijiTableReader is used to read the records in question.

{% highlight java %}
  mSongTable = getKiji().openTable(songTableName);
  mSongTableReader = mSongTable.openTableReader();

  // ...

  KijiDataRequest request = KijiDataRequest.builder()
      .addColumns(ColumnsDef.create()
          .withMaxVersions(Integer.MAX_VALUE)
          .add("info", "top_next_songs"))
      .build();

  TopSongs valuesForSong1 = mSongTableReader.get(mSongTable.getEntityId("song-1"), request)
      .getMostRecentValue("info", "top_next_songs");
  assertEquals("Wrong number of most popular songs played next for song-1", 3,
      valuesForSong1.getTopSongs().size());

  TopSongs valuesForSong2 = mSongTableReader.get(mSongTable.getEntityId("song-2"), request)
      .getMostRecentValue("info", "top_next_songs");
  LOG.info("the list of song counts {}", valuesForSong2.getTopSongs().toString());
  assertEquals("Wrong number of most popular songs played next for song-2", 2,
      valuesForSong2.getTopSongs().size());

  TopSongs valuesForSong8 = mSongTableReader.get(mSongTable.getEntityId("song-8"), request)
      .getMostRecentValue("info", "top_next_songs");
  LOG.info("the list of song counts {}", valuesForSong2.getTopSongs().toString());
  assertEquals("Wrong number of most popular songs played next for song-8", 1,
      valuesForSong8.getTopSongs().size());
  assertEquals("The onyl song played aftert song-8 is song-1.", "song-1",
      valuesForSong8.getTopSongs().get(0).getSongId().toString());
{% endhighlight %}

### Running the Example

<div class="userinput">
{% highlight bash %}
kiji mapreduce \
    --mapper=org.kiji.examples.music.map.IdentityMapper \
    --reducer=org.kiji.examples.music.reduce.TopNextSongsReducer \
    --input="format=avrokv file=output.sequentialPlayCount" \
    --output="format=kiji table=${KIJI}/songs nsplits=1" \
    --lib=${LIBS_DIR}
{% endhighlight %}
</div>

#### Verify
Since we write TopNextSongs back to the Kiji table, we can use the Kiji command-line tools
to inspect our Kiji tables.

<div class="userinput">
{% highlight bash %}
kiji ls --kiji=${KIJI}/songs --columns=info:top_next_songs --max-rows=3
{% endhighlight %}
</div>

    entity-id='song-32' [1361564627564] info:top_next_songs
                                 {"topSongs": [{"song_id": "song-37", "count": 10}, {"song_id": "song-34", "count": 11}, {"song_id": "song-30", "count": 18}]}

    entity-id='song-49' [1361564627741] info:top_next_songs
                                 {"topSongs": [{"song_id": "song-46", "count": 10}, {"song_id": "song-41", "count": 20}, {"song_id": "song-40", "count": 27}]}

    entity-id='song-36' [1361564627591] info:top_next_songs
                                 {"topSongs": [{"song_id": "song-32", "count": 11}, {"song_id": "song-35", "count": 15}, {"song_id": "song-30", "count": 27}]}
