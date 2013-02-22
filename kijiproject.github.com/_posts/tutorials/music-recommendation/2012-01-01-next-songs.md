---
layout: post
title : NextTopSong
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order : 6
description: Outputing to a Kiji table in a MapReduce job.
---

This MapReduce job processes the result of the SequentialSong MR job and writes a list of the top
songs played after a song (the key) to the corresponding row in the songs table.

<div id="accordion-container"> 
  <h2 class="accordion-header"> IdentityMapper.java </h2> 
     <div class="accordion-content"> 
{% highlight java %}
public class IdentityMapper
    extends KijiMapper<
        AvroKey<CharSequence>, AvroValue<SongCount>, AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyWriter, AvroValueWriter, AvroKeyReader, AvroValueReader {

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

   /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }

  /** {@inheritDoc} */
  @Override
  public void map(AvroKey<CharSequence> key, AvroValue<SongCount> value, Context context)
    throws IOException, InterruptedException {
    context.write(key, value);
  }
}
{% endhighlight %}
     </div> 
 <h2 class="accordion-header"> TopNextSongsReducer.java </h2> 
   <div class="accordion-content"> 
{% highlight java %}
public class TopNextSongsReducer
    extends KijiTableReducer<AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyReader, AvroValueReader {
    private static final Logger LOG = LoggerFactory.getLogger(TopNextSongsReducer.class);

  /** An ordered set used to track the most popular songs played after the song being processed. */
  private TreeSet<SongCount> mTopNextSongs;

  /** The number of most popular next songs to keep track of for each song. */
  private final int mNumberOfTopSongs = 3;

  /** A list of SongCounts corresponding to the most popular next songs for each key/song. */
  private TopSongs mTopSongs;

  /** {@inheritDoc} */
  @Override
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

  /** {@inheritDoc} */
  @Override
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
    // Write this to the song table.
    context.put(context.getEntityId(key.datum().toString()), "info", "top_next_songs", mTopSongs);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }
}
{% endhighlight %}
    </div> 
</div>

### IdentityMapper.java
This is a stunning homage to Java boiler plate. This mapper just passes through all keys and values
passed into it.


### TopNextSongsReducer.java 
The keys passed into this reducer are song ids and the values are SongCount records. In order to
find the songs most frequently played after a given song, we need to identify the SongCount
records with the largest number of counts, for every key.

To do this efficiently, we will maintain an ordered collection of SongCount records, that has a maximum
size. As we iterate through all the values, we will keep the top SongCount records seen so far
in our ordered collection.

This reducer
* Creates an ordered collection that will maintain a list of the top SongCount record, for each key.
* Examines each value for a key, and maintains a running list of the top SongCount records seen so
  far.
* Writes a TopNextSongs record to the songs table.


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

It is worht pointing out that when you call value.datum(), the *same* SongCount record, with
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

### Describe Tests
Talk about how cool it is that we can test the result of a sequence of jobs in a unit test.
Explain verifying the output to tables using a table reader.

### Running the Example

<div class="userinput">
{% highlight bash %}
$ kiji mapreduce \
      --mapper=org.kiji.examples.music.map.IdentityMapper \
      --reducer=org.kiji.examples.music.reduce.TopNextSongsReducer \
      --input="format=avrokv file=${HDFS_ROOT}/output.sequentialPlayCount" \
      --output="format=kiji table=${KIJI}/songs nsplits=1" \
      --lib=${LIBS_DIR}
{% endhighlight %}
</div>

#### Verify

<div class="userinput">
{% highlight bash %}
kiji ls --kiji=${KIJI}/songs --columns=info:top_next_songs --max-rows=3
{% endhighlight %}
</div>
