---
layout: post
title: Top Next Songs
categories: [tutorials, express-recommendation, 0.10.0]
tags: [express-music]
order: 6
description: Find the most popular song played after each song.
---

Now, for each song, we want to compute a list of the songs that users most frequently play
after that song.

The entire pipeline for this computation is a little more complex.  It includes the following steps:

* Read the column "info:track_plays" from rows in a Kiji table.
* Transform songs histories into "bigrams" of songs.
* Count the occurrences of every unique song pair.
* Pack the pair of songId and count fields into a SongCount record.
* Sort the nextSongs associated with each firstSong.
* Do some final processing on the tuples.
* Write topNextSongs to the “info:top_next_songs” column in the Kiji table.

The next sections walk through the pipeline line-by-line and describe the custom functions as they appear.
The entire file is available at the [end of the page](#top-next-full-code).

#### Read "info:track_plays" from a Kiji table

The first line reads all values in the “info:track_plays” column from the Kiji table “users-table”
into a tuple field called ‘playlist.  Recall that the “info:track_plays” column contains, for each
user row, each track played by that user, at the timestamp it was played.

{% highlight scala %}
    KijiInput(args("users-table"))(Map(Column("info:track_plays", all) -> 'playlist))
{% endhighlight %}

#### Transform songs histories into bigrams of songs

The next line maps ‘playlist (all songs a user has played) to bigrams of songs that were played in
that order.  `bigrams` is our first user-defined function in the pipeline, and it’s an
important one.

{% highlight scala %}
/**
 * Transforms a slice of song ids into a collection of tuples `(s1,
 * s2)` signifying that `s2` appeared after `s1` in the slice, chronologically.
 *
 * @param slice of song ids representing a user's play history.
 * @return a list of song bigrams.
 */
def bigrams(slice: KijiSlice[String]): List[(String, String)] = {
  slice.orderChronologically().cells.sliding(2)
      .map { itr => itr.iterator }
      .map { itr => (itr.next().datum, itr.next().datum) }
      .toList
}
{% endhighlight %}

`bigrams` takes as an argument a slice of the table, and transforms it into a collection of tuples.
Each tuple is a pair of songs where the second song was played directly after the first, that is,
the function outputs a sequence of every two adjacent songs in the input.  Bigrams are common in
many types of statistical analysis, see
[the Wikipedia article on bigrams](http://en.wikipedia.org/wiki/Bigram) for more.  We use a flatMap
since the bigrams method produces a list of song pairs from each playlist.

{% highlight scala %}
    .flatMap('playlist -> ('firstSong, 'songId)) { bigrams }
{% endhighlight %}

#### Count the occurrences of every unique song pair

To perform a count, the `groupBy` method will be used to count the unique occurrences of two songs
played in sequence.  For each group of tuples with the same `firstSong` and `songId`, we put the
size of that group in the `count` field.

{% highlight scala %}
    .groupBy(('firstSong, 'songId)) { _.size('count) }
{% endhighlight %}

#### Pack the pair of ‘songId and ‘count into a SongCount record

Next, a `SongCount` record is constructed containing the song played and the count associated with
with it. KijiExpress's `packAvro` method is used to perform this operation. `packAvro` takes a
mapping from tuple fields to a field name. The resulting AvroRecord is bound to the field name. That
AvroRecord has every tuple entry that was packed into it as a field.  For example, here, after the
`packAvro` operation, the `songCount` field contains an AvroRecord with two fields: `songId` and
`count`.

{% highlight scala %}
    .packAvro(('songId, 'count) -> 'songCount)
{% endhighlight %}

#### Sort the nextSongs associated with each firstSong

To sort the `nextSongs` associated with each `firstSong`, we groupBy the `firstSongs` and sort those
groups.

{% highlight scala %}
    .groupBy('firstSong) { sortNextSongs }
{% endhighlight %}

You've seen `groupBy` operations before, with functions inline such as `_.size('count)` earlier
in this script. Here, sortNextSongs is a UDF we've defined:

{% highlight scala %}
/**
 * Transforms a group of tuples into a group containing a list of song count records,
 * sorted by count.
 *
 * @param nextSongs is the group of tuples containing song count records.
 * @return a group containing a list of song count records, sorted by count.
 */
def sortNextSongs(nextSongs: GroupBuilder): GroupBuilder = {
  nextSongs.sortBy('count).reverse.toList[SongCount]('songCount -> 'topSongs)
}
{% endhighlight %}

A Scalding `GroupBuilder` is basically a group of named tuples, which have been grouped by
a field, and that you can operate on inside a grouping operation.  Here, we've grouped by the
`firstSong` field, and we sort the group by the number of times they've been played. The result is
a list of songs in the order from most to least played, in the field `topSongs`.

`topSongs` contains the data we want: the next top songs, sorted by
popularity, that follow any particular song.  The last few lines are the machinery required to put
that into our songs table.

#### Do some additional processing on the tuples

Next, we convert the tuple in `topSongs` to an Avro record with one field `topNextSongs`.

{% highlight scala %}
    .packAvro('topSongs -> 'topNextSongs)
{% endhighlight %}

#### Write the topNextSongs field to the “info:top_next_songs” column in the Kiji table

Finally, we create entity IDs using the `firstSong` field and put it in the `entityId`, then write the
`topNextSongs` field to the "info:top_next_songs" column in our table.

{% highlight scala %}
    .map('firstSong -> 'entityId) { firstSong: String =>
        EntityId(firstSong) }
    .write(KijiOutput(args("songs-table"))('topNextSongs -> "info:top_next_songs"))
{% endhighlight %}

### Running TopNextSongs ###

* To run the TopNextSongs job:

<div class="userinput">
{% highlight bash %}
express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-0.10.0.jar \
    org.kiji.express.music.TopNextSongs --users-table ${KIJI}/users \
    --songs-table ${KIJI}/songs --hdfs
{% endhighlight %}
</div>

### Verify the Output ###

*  To see the output from the job:

<div class="userinput">
{% highlight bash %}
kiji scan ${KIJI}/songs --max-rows=2
{% endhighlight %}
</div>

You should see:

    Scanning kiji table: kiji://localhost:2181/kiji_express_music/songs/
    entity-id=['song-32'] [1365549351598] info:metadata
        {"song_name": "song name-32", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre1.0", "tempo": 120, "duration": 180}
    entity-id=['song-32'] [1365550614616] info:top_next_songs
        {"topSongs": [{"song_id": "song-31", "count": 8}, {"song_id": "song-30", "count": 8}, {"song_id": "song-33", "count": 6}, {"song_id": "song-32", "count": 6}, {"song_id": "song-38", "count": 4}, {"song_id": "song-37", "count": 4}, {"song_id": "song-39", "count": 2}, {"song_id": "song-8", "count": 2}, {"song_id": "song-6", "count": 1}, {"song_id": "song-34", "count": 1}, {"song_id": "song-29", "count": 1}, {"song_id": "song-24", "count": 1}, {"song_id": "song-23", "count": 1}, {"song_id": "song-0", "count": 1}]}
    entity-id=['song-49'] [1365549353027] info:metadata
        {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre4.0", "tempo": 150, "duration": 180}
    entity-id=['song-49'] [1365550613461] info:top_next_songs
        {"topSongs": [{"song_id": "song-38", "count": 1}, {"song_id": "song-8", "count": 1}, {"song_id": "song-31", "count": 1}, {"song_id": "song-10", "count": 1}, {"song_id": "song-6", "count": 1}]}

Notice that for each of these songs, there is now a info:top_next_songs column that contains
a record “topSongs”, the list of top songs played after each of these, in order of popularity.
We now have the data necessary for a song recommendation model based on the most popular next songs.

### Top Next Songs Job Content<a id="top-next-full-code"> </a>

Here's the entire TopNextSongs job:

<div id="accordion-container">
  <h2 class="accordion-header"> TopNextSongs.scala </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/kiji-express-music-0.10.0/src/main/scala/org/kiji/express/music/TopNextSongs.scala"> </script>
  </div>
</div>
