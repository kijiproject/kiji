---
layout: post
title: Recommendations Producer
categories: [tutorials, express-recommendation, 0.11.0]
tags: [express-music]
order: 7
description: Generate recommendations per user.
---

<h3 style="margin-top:0px;padding-top:10px;">Personalizing Recommendations</h3>

In the previous tutorial step, for each song, we computed the songs most frequently played next.  We
now use this data to make actual recommendations. We simply look at the last song each user has
listened to and recommend the most popular song played after it.

Specifically, the `songs-table` has information about the top next songs in its column
"info:top_next_songs". We have the latest song a user has played in the `users-table` in column
"info:track_plays." We now have to join these to get our result.

The next sections walk through the pipeline line-by-line and describe the custom functions as they appear.
The entire file is available at the [end of the page](#recommend-full-code).

#### Get the most popular song played next

We first describe a helper function which retrieves the song ID of the most popular song given the
list of top next songs. `getMostPopularSong` takes as its input a Scala `Seq` of Kiji cells
containing AvroRecords from the "info:top_next_songs" column of the songs table.

The "info:top_next_songs" column contains a `TopSongs` Avro record.  You can look in the
ExpressMusic.avdl file for the definitions of these Avro records:

{% highlight scala %}
  /** A count of the number of times a song has been played. */
  record SongCount {
    string song_id;
    long count;
  }

  /** Container record for the top songs and their number of plays. */
  record TopSongs {
     array<SongCount> top_songs;
  }
{% endhighlight %}

In the Scala script for this step in the tutorial, we read out Avro data from our Kiji table as
specific Avro records.  We can access the fields within those records with getter functions,
possibly followed by casts to the appropriate primitive type.  For example, to get the value of
`count` within a `SongCount` Avro record, we would use the following:

{% highlight scala %}
val myCount: String = songCount.getCount.toLong
{% endhighlight %}

In the `getMostPopularSong` function below, we retrieve the most popular song from a `TopSongs`
record by getting the first song in the list of `top_songs` within the Avro record.  Recall that we
sorted this list in the previous step in the tutorial.  The items within the `top_songs` array are
instances of `SongCount`, which contains a `song_id` member, which is what we need to recommend the
next song to play:

{% highlight scala %}
  /**
   * This method retrieves the most popular song (at index 0) in the TopSongs record.
   *
   * @param songs from the TopSongs record.
   * @return the most popular song.
   */
  def getMostPopularSong(songs: Seq[FlowCell[TopSongs]]): String = {
    songs.head.datum.getTopSongs.get(0).getSongId.toString
  }
{% endhighlight %}

#### Apply the above function on a Scalding Pipe

Next, we construct a pipe does the following:

* Reads the column "info:top_next_songs" from the `songs` table as `'topNextSongs`.
* Uses the first component of a given row's `EntityId` as the `songId`.
* Applies the above described `getMostPopularSong` to all the songs.
* Outputs tuples of `('songId, nextSong)`

The code follows:

{% highlight scala %}
  val recommendedSong = KijiInput(args("songs-table"),
      Map(QualifiedColumnInputSpec("info", "top_next_songs", classOf[TopSongs])
          -> 'topNextSongs))
      .map('entityId -> 'songId) { eId: EntityId => eId(0) }
      .map('topNextSongs -> 'nextSong) { getMostPopularSong }
      .project('songId, 'nextSong)
{% endhighlight %}

#### Putting it all together

Finally we create a flow that does the following:

- Reads the column "info:track_plays" from the users table, which contains the listening history.
- Retrieves the song most recently played by a user into the field `'lastTrackPlayed`.
- Generates the recommendation by joining the two pipes, one of which contains the `'user` and
  `'songId` fields (for the last song played), and the other which contains the `'songId` and
  `'nextSong` fields for the most popular top songs.

{% highlight scala %}
  KijiInput(args("users-table"),
      Map(QualifiedColumnInputSpec("info", "track_plays") -> 'trackPlays))
      .map('trackPlays -> 'lastTrackPlayed) {
          slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString }
      .joinWithSmaller('lastTrackPlayed -> 'songId, recommendedSong)
      .write(KijiOutput(args("users-table"),
          Map('nextSong -> QualifiedColumnOutputSpec("info", "next_song_rec"))))
{% endhighlight %}

### Running the Example

* To run the SongRecommender job:

<div class="userinput">
{% highlight bash %}
express job ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-0.11.0.jar \
    org.kiji.express.music.SongRecommender --songs-table ${KIJI}/songs \
    --users-table ${KIJI}/users
{% endhighlight %}
</div>

### Verify Output

You can verify the output by scanning the `users-table`.

<div class="userinput">
{% highlight bash %}
kiji scan ${KIJI}/users --max-rows=2
{% endhighlight %}
</div>

You should see something like:

    Scanning kiji table: kiji://localhost:2181/kiji_express_music/users/
    entity-id=['user-41'] [1325762580000] info:track_plays
                                 song-41
    entity-id=['user-41'] [1379354039975] info:next_song_rec
                                 song-41

    entity-id=['user-3'] [1325751420000] info:track_plays
                                 song-0
    entity-id=['user-3'] [1379354034880] info:next_song_rec
                                 song-0

### Shut down the cluster

That's the end of the Express tutorial!

*  Now is a good time to shut down the BentoBox cluster:

<div class="userinput">
{% highlight bash %}
    bento stop
{% endhighlight %}
</div>

### Top Next Songs Job Content<a id="recommend-full-code"> </a>

Here's the entire SongRecommender job:

<div id="accordion-container">
  <h2 class="accordion-header"> SongPlayCounter.express </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/kiji-express-music-0.11.0/src/main/scala/org/kiji/express/music/SongRecommender.scala"> </script>
  </div>
</div>
