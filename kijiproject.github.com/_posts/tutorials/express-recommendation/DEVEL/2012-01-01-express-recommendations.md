---
layout: post
title: Recommendations Producer
categories: [tutorials, express-recommendation, DEVEL]
tags: [express-music]
order: 6
description: Generate recommendations per user.
---

<div id="accordion-container">
  <h2 class="accordion-header"> SongPlayCounter.express </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/{{site.music_express_DEVEL_branch}}/src/main/scala/org/kiji/express/music/SongRecommender.scala"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;">Personalizing Recommendations</h3>

In the last tutorial step, for each song, we computed the songs most frequently played next. It is now time
to use this data to make actual recommendations. We simply look at the last song each user has listened
to and recommend the most popular song played after it.

Specifically, the `songs-table` has information about the top next songs in its column
`info:top_next_songs`. We have the latest song a user has played in the `users-table` in column info:track_plays.
We now have to join these to get our result.

#### Get the most popular song played next

We first describe a helper function which retrieves the song id of the most popular song given the list of top
next songs. `getMostPopularSong` takes as its input a KijiSlice of TopSongs, which is basically an Avro class
with a list of the top songs. We get the latest version of the slice using `.getFirstValue`, get the underlying
list using `getTopSongs` and get the first (most popular) song. This object is of type `SongCount`. We retrieve
the song id by `getSongId` and convert the resulting CharSequence to a string.

{% highlight scala %}
  // This method retrieves the most popular song (at index 0) in the TopNextSongs record.
  def getMostPopularSong(songs: KijiSlice[TopSongs]): String = {
    songs.getFirstValue().getTopSongs().get(0).getSongId().toString
  }
{% endhighlight %}

#### Apply the above function on a Scalding Pipe

Next, we then construct a Scalding RichPipe does the following:

* Reads the column `info:top_next_songs` from the songs table as `'topNextSongs`.
* Uses the first component of the EntityId as the songId.
* Applies the above described `getMostPopularSong` to all the songs.
* Outputs tuples of (songId, nextSong)

{% highlight scala %}
  val recommendedSong = KijiInput(args("songs-table"))("info:top_next_songs" -> 'topNextSongs)
    .map('entityId -> 'songId) { eId: EntityId => eId(0) }
    .map('topNextSongs -> 'nextSong) { getMostPopularSong}
    .project('songId, 'nextSong)
{% endhighlight %}

#### Putting it all together

Finally we create a flow that does the following:

* Reads the column "info:track_plays" from the users table, which contains the listening history.
* Retrieves the song most recently played by a user into the field `'lastTrackPlayed`.
* Generate the recommendation by joining the two pipes, namely, one containing a (user, song id)
representing the latest song played, and another containing (song id, next song) representing the
most popular next song.

{% highlight scala %}
  KijiInput(args("users-table"))("info:track_plays" -> 'trackPlays)
      .map('trackPlays -> 'lastTrackPlayed) {
           slice: KijiSlice[String] => slice.getFirstValue()}
      .joinWithSmaller('lastTrackPlayed -> 'songId, recommendedSong)
      .write(KijiOutput(args("users-table"))('nextSong -> "info:next_song_rec"))
{% endhighlight %}

### Running the Example

<div class="userinput">
{% highlight bash %}
express job ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-0.2.0-SNAPSHOT.jar \
    org.kiji.express.music.SongRecommender --songs-table ${KIJI}/songs \
    --users-table ${KIJI}/users
{% endhighlight %}
</div>

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
    entity-id=['user-41'] [1367023207962] info:next_song_rec
                                     song-41

Alternately, you can also run these as scripts as follows:

<div class="userinput">
{% highlight bash %}
express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-0.2.0-SNAPSHOT.jar \
    org.kiji.express.music.SongRecommender --songs-table ${KIJI}/songs \
    --users-table ${KIJI}/users
{% endhighlight %}
</div>

