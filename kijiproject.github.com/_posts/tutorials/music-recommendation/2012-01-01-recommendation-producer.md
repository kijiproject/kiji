---
layout: post
title : Music Recommendation Producer
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order : 7
description: Read and write to the same row of a table.
---

<div id="accordion-container">
  <h2 class="accordion-header"> NextSongRecommender.java </h2>
  <div class="accordion-content">
{% highlight java %}
   public class NextSongRecommender extends KijiProducer implements KeyValueStoreClient {

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Only request the most recent version from the "info:track_plays" column.
    return KijiDataRequest.create("info", "track_plays");
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return "info:next_song_rec";
  }

    /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context) throws IOException {
    // Open the key value store reader
    KeyValueStoreReader<String, TopSongs> topNextSongsReader = null;
    try {
      topNextSongsReader = context.getStore("nextPlayed");
    } catch (InterruptedException ex) {
      // It is pointless to continue if we can't open the key value store.
      throw new RuntimeException(ex);
    }
    // Get the most recent song the user has listened to:
    String mostRecentSong = input.<CharSequence>getMostRecentValue("info", "track_plays")
        .toString(); // Avro strings get deserialized to CharSequences.
    // Read the most popular songs played after mostRecentSong, from the song table.
    TopSongs topSongs = topNextSongsReader.get(mostRecentSong);
    // Read the array of song counts stored in field ""
    List<SongCount> popularNextSongs = topSongs.getTopSongs();
    // Write our recommended next song to "info:next_song_rec"
    context.put(recommend(popularNextSongs));
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return RequiredStores.just("nextPlayed", UnconfiguredKeyValueStore.builder().build());
  }

  /**
   * This method uses a list of song counts to determine the next song a user should listen to.
   *
   * @param topNextSongs A list of most popular songs.
   * @return CharSequence The id of the recommended song.
   */
  private CharSequence recommend(List<SongCount> topNextSongs) {
    return topNextSongs.get(0).getSongId(); // Do the simplest possible thing.
  }
}
{% endhighlight %}
  </div>
  <h2 class="accordion-header"> TestNextSongRecommender.java </h2>
  <div class="accordion-content">
{% highlight java %}
public class TestNextSongRecommender extends KijiClientTest {

  private KijiURI mUserTableURI;
  private KijiURI mSongTableURI;
  private KijiTable mUserTable;
  private KijiTableReader mUserTableReader;

  /** Initialize our environment. */
  @Before
  public final void setup() throws Exception {
    // Create layouts and URIs to use during the test.
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();
    final KijiTableLayout songLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/songs.json");
    final String songTableName = songLayout.getName();
    mSongTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(songTableName).build();

    SongCount songFour = new SongCount();
    songFour.setCount(10L);
    songFour.setSongId("song-4");
    List<SongCount> listOfSongFour = Lists.newArrayList(songFour);
    TopSongs topSongsForSong1 = new TopSongs();
    topSongsForSong1.setTopSongs(listOfSongFour);

    SongCount songFive = new SongCount();
    songFive.setCount(9L);
    songFive.setSongId("song-5");
        List<SongCount> listOfSongFive = Lists.newArrayList(songFive);
    TopSongs topSongsForSong2 = new TopSongs();
    topSongsForSong2.setTopSongs(listOfSongFive);
    // Initialize a kiji instance with relevant tables to use during tests.
    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(8L, "song-1")
                .withValue(9L, "song-3")
                .withValue(10L, "song-2")
        .withTable(songLayout.getName(), songLayout)
            .withRow("song-1").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong1)
            .withRow("song-2").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong2)
        .build();
    // Open table and table reader.
    mUserTable = getKiji().openTable(userTableName);
    mUserTableReader = mUserTable.openTableReader();
  }

  /**  Close resources we open for the test. */
  @After
  public final void cleanup() {
    // Close table and table reader in the reverse order.
    IOUtils.closeQuietly(mUserTableReader);
    IOUtils.closeQuietly(mUserTable);
  }

  @Test
  public void testProducer() throws IOException, ClassNotFoundException, InterruptedException {
     MapReduceJobOutput tableOutput = new DirectKijiTableMapReduceJobOutput(mUserTableURI, 1);
         KijiTableKeyValueStore.Builder kvStoreBuilder = KijiTableKeyValueStore.builder();
    // Our default implementation will use the default kiji instance, and a table named songs.
    kvStoreBuilder.withColumn("info", "top_next_songs").withTable(mSongTableURI);
        // Configure first job.
    final MapReduceJob mrjob = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(NextSongRecommender.class)
        .withInputTable(mUserTableURI)
        .withOutput(tableOutput)
        .withStore("nextPlayed", kvStoreBuilder.build())
        .build();

    // Run the job and confirm success.
    assertTrue(mrjob.run());

    KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(KijiDataRequestBuilder.ColumnsDef.create()
            .withMaxVersions(Integer.MAX_VALUE)
            .add("info", "next_song_rec"))
        .build();

    CharSequence valueForSong1 = mUserTableReader.get(mUserTable.getEntityId("user-1"), request)
        .getMostRecentValue("info", "next_song_rec");
    assertEquals("User-1 just listened to son-1, so their next song rec should be song-4", "song-4",
      valueForSong1.toString());

  }
}
{% endhighlight %}
  </div>
  <h2 class="accordion-header"> KVStoreConfig.xml </h2>
  <div class="accordion-content">
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
  </div>
</div>

### NextSongRecommender.java
The NextSongRecommender is an example of a [KijiProducer](link-to-userguide). A producer operates on
a single row of input data and generates new outputs that are written to the same row. For every row
this producer processes, it will:

* Read the most recent value from the "info:track_plays" column of the users table. This is the song
  id of the most recently played song by the user
* Lookup a list of the songs most frequently played next from the songs table.
* Generate a recommendation from the list of songs most frequently played next.
* Write that recommendation to the "info:next_song_rec" column of the users table.

#### Get The Most Recent Song Played
Like Gatherers, you specify the required columns for your producer in the getDataRequest method. We
only want the most recent value from this column, so we can use the create convenience method.
{% highlight java %}
  public KijiDataRequest getDataRequest() {
    // Only request the most recent version from the "info:track_plays" column.
    return KijiDataRequest.create("info", "track_plays");
  }
{% endhighlight %}

In our produce method, we then access our requested data through the KijiRowData:

{% highlight java %}
  String mostRecentSong = input.<CharSequence>getMostRecentValue("info", "track_plays")
      .toString();// Avro strings get deserialized to CharSequences, so .toString() the result.
{% endhighlight %}

#### Join External Data Sources
KeyValueStores allow you to access external data sources in a MapReduce job.
In this case, we will use the "top_next_songs" column of our songs table as a KeyValueStore. In
order to access KeyValueStores in a KijiMR Job, the class that needs the external data must
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

When we run this producer in a test, we will override the default implementation programmatically
using a job builder. When you run this producer from the command line, you will override the
default implementation using the KVConfig.xml file.

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

### TestNextSongRecommender.java
To test NextSongRecommender, we need to build a job that is configured with a Kiji table column
as a KeyValueStore. This is easily accomplished by using KijiTableKeyValueStore's builder:

{% highlight java %}
  KijiTableKeyValueStore.Builder kvStoreBuilder = KijiTableKeyValueStore.builder();
  kvStoreBuilder.withColumn("info", "top_next_songs").withTable(mSongTableURI);

  // Configure first job.
  final MapReduceJob mrjob = KijiProduceJobBuilder.create()
      .withStore("nextPlayed", kvStoreBuilder.build())

  // ...
{% endhighlight %}

### Running the Example
When we run this example, we need to give the 

<div class="userinput">
{% highlight bash %}
kiji produce \
--producer=org.kiji.examples.music.produce.NextSongRecommender \
--input="format=kiji table=$KIJI/users" \
--output="format=kiji table=$KIJI/users nsplits=2" \
--lib=${LIBS_DIR} \
--kvstores=KVStoreConfig.xml
{% endhighlight %}
</div>

#### Verify

<div class="userinput">
{% highlight bash %}
kiji ls --kiji=$KIJI/users --columns=info:next_song_rec --max-rows=3
{% endhighlight %}
</div>


