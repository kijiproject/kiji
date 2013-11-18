---
layout: post
title: Top Next Songs
categories: [tutorials, express-recommendation, devel]
tags: [express-music]
order: 6
description: Find the most popular song played after each song.
---

Now, for each song, we want to compute a list of the songs that users most frequently play
after that song.

The entire pipeline for this computation is a little more complex.  It includes the following steps:

- Read the column "info:track_plays" from rows in a Kiji table.
- Transform the history of songs played into [bigrams](http://en.wikipedia.org/wiki/Bigram) of
  songs.
- Count the occurrences of every unique song pair.
- Pack the pair of `'song_id` and `'count` fields into a `SongCount` record.
- Sort the `nextSongs` associated with each `first_song`.
- Do some final processing on the tuples.
- Write `top_next_songs` to the “info:top_next_songs” column in the Kiji table.

The following sections walk through the pipeline line-by-line and describe the custom functions as
they appear.  The entire file is available at the [end of the page](#top-next-full-code).

#### Read "info:track_plays" from a Kiji table

The first line reads all values in the “info:track_plays” column from the Kiji table “users-table”
into the field `‘playlist`.  Recall that the “info:track_plays” column contains, for each
user row, each track played by that user, at the timestamp it was played.

{% highlight scala %}
 KijiInput(args("users-table"),
      Map(QualifiedColumnInputSpec("info", "track_plays", all) -> 'playlist))
{% endhighlight %}

#### Transform songs histories into bigrams of songs

The next line maps `‘playlist` (all songs a user has played) to bigrams of songs that were played in
that order.  `bigrams` is our first user-defined function in the pipeline, and it’s an important
one:

{% highlight scala %}
/**
  * Transforms a slice of song ids into a collection of tuples `(s1, s2)` signifying that `s2`
  * appeared after `s1` in the slice, chronologically.
  *
  * @param slice of song ids representing a user's play history.
  * @return a list of song bigrams.
  */
def bigrams(slice: Seq[FlowCell[String]]): List[(String, String)] = {
  slice
      .sortBy { _.version }
      .sliding(2)
      .map { itr => itr.iterator }
      .map { itr => (itr.next().datum, itr.next().datum) }
      .toList
}
{% endhighlight %}

`bigrams` takes as an argument a slice of the table, and transforms it into a collection of tuples.
Each tuple is a pair of songs where the second song was played directly after the first, that is,
the function outputs a sequence of every two adjacent songs in the input.  Bigrams are common in
many types of statistical analysis, see
[the Wikipedia article on bigrams](http://en.wikipedia.org/wiki/Bigram) for more.  As in the
[previous section](express-play-count), we use a flatMap, since the `bigrams` method produces a list of
song pairs from each playlist.

{% highlight scala %}
    .flatMap('playlist -> ('first_song, 'song_id)) { bigrams }
{% endhighlight %}

#### Count the occurrences of every unique song pair

To count the occurrences of every unique song bigram, we use the `groupBy` and `size` functions.
Here we group by a pair of fields, `first_song` and `song_id`:

{% highlight scala %}
    .groupBy(('first_song, 'song_id)) { _.size('count) }
{% endhighlight %}

The tuples coming out of this function have three fields: `'first_song`, `'song_id`, and `'count`.

#### Pack the pair of ‘song_id and ‘count into a SongCount record

Next we convert every tuple into a `SongCount` [Avro](http://avro.apache.org/) record, which
contains a `song_id` and the corresponding `count.`  The `pack` creates these records, specifying the
fields (in this case, `'song_id` and `'count`) with which to construct the records, and the field
(`'song_count`) into which to place the records:

{% highlight scala %}
    .pack[SongCount](('song_id, 'count) -> 'song_count)
{% endhighlight %}

We define the `SongCount` Avro record using the following Avro
[AVDL](http://avro.apache.org/docs/current/idl.html):

    record SongCount {
      string song_id;
      long count;
    }

Learn more about [Kiji and Avro]({{site.userguide_mapreduce_devel}}/working-with-avro/).

#### Sort the nextSongs associated with each first_song

We will now determine, for each song our users have played, the most common song they played next.
To do so, we group by `first_song` and then sort by `nextSong`:

{% highlight scala %}
    .groupBy('first_song) { sortNextSongs }
{% endhighlight %}

You've seen `groupBy` operations before, with functions inline such as `_.size('count)` earlier
in this script. Here, `sortNextSongs` is another function we have defined:

{% highlight scala %}
/**
 * Transforms a group of tuples into a group containing a list of song count records,
 * sorted by count.
 *
 * @param nextSongs is the group of tuples containing song count records.
 * @return a group containing a list of song count records, sorted by count.
 */
def sortNextSongs(nextSongs: GroupBuilder): GroupBuilder = {
  nextSongs.sortBy('count).reverse.toList[SongCount]('songCount -> 'top_songs)
}
{% endhighlight %}

A Scalding `GroupBuilder` is basically a group of named tuples, which have been grouped by a field,
and upon which you can operate inside a grouping operation.  Here, we've grouped by the `first_song`
field, and we sort the group by the number of times the second song has been played. The result is a
list of songs in the order from most to least played, in the field `top_songs`.

`top_songs` contains the data we want: the next top songs, sorted by
popularity, that follow any particular song.  The last few lines are the machinery required to put
that into our songs table.

#### Do some additional processing on the tuples

Next, we convert the tuple in `top_songs` to an Avro record with one field `top_next_songs`.

{% highlight scala %}
    .map('top_songs -> 'top_songs) { ts: List[SongCount] => ts.asJava }
    .pack[TopSongs]('top_songs -> 'top_next_songs)
{% endhighlight %}

The call to
[asJava](http://www.scala-lang.org/api/current/index.html#scala.collection.JavaConverters$)
converts our Scala `List` into a Java `List` for Avro serialization (Avro requires a Java `List`.)
The Avro definition for `TopSongs` follows:

    record TopSongs {
      array<SongCount> top_songs;
    }


#### Write the top_next_songs field to the “info:top_next_songs” column in the Kiji table

Finally, we create entity IDs using the `first_song` field and put it in the `entityId`, then write the
`top_next_songs` field to the "info:top_next_songs" column in our table.

{% highlight scala %}
    .map('first_song -> 'entityId) { first_song: String => EntityId(first_song) }
    .write(KijiOutput(args("songs-table"),
        Map('top_next_songs -> QualifiedColumnOutputSpec(
            "info",
            "top_next_songs",
            schemaSpec = SchemaSpec.Specific(classOf[TopSongs])))))
{% endhighlight %}

### Running TopNextSongs ###

* To run the TopNextSongs job:

<div class="userinput">
{% highlight bash %}
express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-{{site.music_express_devel_version}}.jar \
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
        {"top_songs": [{"song_id": "song-31", "count": 8}, {"song_id": "song-30", "count": 8}, {"song_id": "song-33", "count": 6}, {"song_id": "song-32", "count": 6}, {"song_id": "song-38", "count": 4}, {"song_id": "song-37", "count": 4}, {"song_id": "song-39", "count": 2}, {"song_id": "song-8", "count": 2}, {"song_id": "song-6", "count": 1}, {"song_id": "song-34", "count": 1}, {"song_id": "song-29", "count": 1}, {"song_id": "song-24", "count": 1}, {"song_id": "song-23", "count": 1}, {"song_id": "song-0", "count": 1}]}
    entity-id=['song-49'] [1365549353027] info:metadata
        {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre4.0", "tempo": 150, "duration": 180}
    entity-id=['song-49'] [1365550613461] info:top_next_songs
        {"top_songs": [{"song_id": "song-38", "count": 1}, {"song_id": "song-8", "count": 1}, {"song_id": "song-31", "count": 1}, {"song_id": "song-10", "count": 1}, {"song_id": "song-6", "count": 1}]}

Notice that for each of these songs, there is now a info:top_next_songs column, which contains
a record “top_songs”, the list of top songs played after each of these, in order of popularity.
We now have the data necessary for a song recommendation model based on the most popular next songs.

### Top Next Songs Job Content<a id="top-next-full-code"> </a>

Here's the entire TopNextSongs job:

<div id="accordion-container">
  <h2 class="accordion-header"> TopNextSongs.scala </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/{{site.music_express_devel_branch}}/src/main/scala/org/kiji/express/music/TopNextSongs.scala"> </script>
  </div>
</div>
