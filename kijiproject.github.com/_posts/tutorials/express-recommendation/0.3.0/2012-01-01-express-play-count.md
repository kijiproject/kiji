---
layout: post
title: PlayCount
categories: [tutorials, express-recommendation, 0.3.0]
tags: [express-music]
order: 4
description: A job that counts song plays.
---

<div id="accordion-container">
  <h2 class="accordion-header"> SongPlayCounter.scala </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/kiji-express-music-0.3.0/src/main/scala/org/kiji/express/music/SongPlayCounter.scala"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;">The 'Hello World!' of MapReduce</h3>

To quote Scalding Developers
[Hadoop is a distributed system for counting words.](https://github.com/twitter/scalding)
While we may not have words to count, we do have the play history of users listening to many different songs.
Unfortunately, we are fresh out of words, but have the play history of
bajillions of users listening to bazillions of songs.

This MapReduce job uses the listening history of our users that we have stored in the "users" Kiji
table to calculate the total number of times each song has been played. The result of this
computation is written to a text file in HDFS.

SongPlayCounter is an example of a KijiExpress flow that reads from a Kiji table and writes to a
file on HDFS. SongPlayCounter proceeds through discrete stages:

* Read the column "info:track_plays" from rows in a Kiji table.
* Break each user's track play history into individual songs.
* Count the number of times each song has been played.
* Write each song id and play count to a file in HDFS.

The entire pipeline put together looks like this:
{% highlight scala %}
/**
 * Counts the number of times a song has been played by users.
 *
 * This importer expects to receive two command line arguments: `--table-uri` and `--output`. The
 * argument `--table-uri` should be set to the Kiji URI of a users table that contains a column
 * `info:track_plays` that contains a song id for each song a user has listened to. The argument
 * `--output` should be the HDFS path where a tab-delimited file listing song ids and play counts
 * should be written.
 *
 * @param args passed in from the command line.
 */
class SongPlayCounter(args: Args) extends Job(args) {

  /**
   * Gets the ids of songs a user has listened to.
   *
   * @param slice from the column `info:track_plays` that records all the songs a user has
   *     listened to.
   * @return the song ids that a user has listened to.
   */
  def songsListenedTo(slice: KijiSlice[String]): Seq[String] = {
    slice.cells.map { cell => cell.datum }
  }

  // This Scalding pipeline does the following.
  // 1. Reads the column "info:track_plays" from rows in a Kiji table.
  // 2. Breaks each user's track plays history into individual songs.
  // 3. Counts the number of times each song has been played.
  // 4. Writes each song id and play count to a file in HDFS.
  KijiInput(args("table-uri"))(Map(Column("info:track_plays", all) -> 'playlist))
      .flatMapTo('playlist -> 'song) { songsListenedTo }
      .groupBy('song) { _.size('songCount) }
      .write(Tsv(args("output")))
}
{% endhighlight %}


#### Read "info:track_plays" from a Kiji table

Data can be read from a Kiji table by using `KijiInput`. This factory method takes options specific
to requesting slices of data from a Kiji table such as:

* Which columns to retrieve from the table and the field names they should be given.
* Number of versions of each cell to return.
* Filters to apply to the requested data.

More information about `KijiInput` can be found in the ScalaDocs of KijiExpress'
[`DSL`]({{site.api_express_0_3_0}}/DSL$.html).

For our purposes, we will read all versions of the column "info:track_plays" from provided the Kiji
table and bind the resulting value to the field named `'playlist` by calling `KijiInput`:

{% highlight scala %}
KijiInput(args("table-uri"))(Map(Column("info:track_plays", all) -> 'playlist))
{% endhighlight %}

#### Split each user's track play history into individual songs

Each cell in the "info:track_plays" column may contain multiple songs that the user represented by
this row has listened to. This data is manifested as a
[`KijiSlice`]({{site.api_express_0_3_0}}/KijiSlice.html). For our purposes, we can imagine a
[`KijiSlice`]({{site.api_express_0_3_0}}/KijiSlice.html) as a list of cells, each one a different
version of the "info:track_plays" column. To unpack the data contained within each cell:

{% highlight scala %}
/**
 * Gets the ids of songs a user has listened to.
 *
 * @param slice from the column `info:track_plays` that records all the songs a user has
 *     listened to.
 * @return the song ids that a user has listened to.
 */
def songsListenedTo(slice: KijiSlice[String]): Seq[String] = {
  slice.cells.map { cell => cell.datum }
}
{% endhighlight %}

Once the cells have been unpacked, we can then flatten the resulting list by calling `flatMapTo`.
`flatMapTo` is very similar to the more common `map` operation. Instead of taking a function that
produces one value per input value, `flatMapTo` takes a function that returns multiple values of the
same type in a list. These values are all returned in a single, unnested "list." For more about the
difference between map and flatMap, check out [this blog post](http://www.brunton-spall.co.uk/post/2011/12/02/map-map-and-flatmap-in-scala/)
`flatMapTo` differs from the `flatMap` operation but discards all fields not generated by
`flatMapTo`.

{% highlight scala %}
.flatMapTo('playlist -> 'song) { songsListenedTo }
{% endhighlight %}

After this operation, the virtual "list" being operated on now contains all of the songs listened to
by users.

#### Count the occurrences of each song.
Now that each played song has been seperated from the user that listened to it, we can calculate the
play count for each song. To achieve this, we will use the `groupBy` operation. `groupBy` takes two
arguments:

* A field to group by
* A function that aggregates the resulting tuples that shared the same value bound to the provided
  field.

In our case we want to group on the song name which will provide a list of tuples that contained the
same song. This group will then be used to calculate its size which then gets bound to the field
name `'songCount`:

{% highlight scala %}
.groupBy('song) { _.size('songCount) }
{% endhighlight %}

After this operation, the virtual "list" being operated on now contains a mapping between song names
(stored in the `'song` field) and its corresponding play count (stored in the `'songCount` field).

#### Write the results to a file
The last step to this stage is to calculate write the play counts to a TSV (Tab Seperated Value)
file on HDFS:

{% highlight scala %}
.write(Tsv(args("output")))
{% endhighlight %}

Running it:

<div class="userinput">
{% highlight bash %}
express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-0.3.0.jar \
    org.kiji.express.music.SongPlayCounter \
    --table-uri ${KIJI}/users \
    --output express-tutorial/songcount-output \
    --hdfs
{% endhighlight %}
</div>

Or run it as a script:

<div class="userinput">
{% highlight bash %}
express script --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/scripts/SongPlayCounter.express --hdfs
{% endhighlight %}
</div>

Verify:

<div class="userinput">
{% highlight bash %}
hadoop fs -tail express-tutorial/songcount-output/part-00000 | head -n 3
{% endhighlight %}
</div>

    song-0	260
    song-1	100
    song-10	272

