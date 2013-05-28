---
layout: post
title: Importing Data
categories: [tutorials, express-recommendation, DEVEL]
tags: [express-music]
order: 3
description: Importing data files into Kiji tables.
---


In this section of the tutorial, we will import metadata about songs into the Kiji table `songs`,
and import data about when users have listened to songs into the Kiji table `users`.

### Stock Importers
The user data can be imported using a stock Kiji bulk importer with the command:

<div class="userinput">
{% highlight bash %}
kiji bulk-import
-Dkiji.import.text.input.descriptor.path=express-tutorial/song-plays-import-descriptor.json
--importer=org.kiji.mapreduce.lib.bulkimport.JSONBulkImporter
--output="format=kiji table=${KIJI}/users nsplits=1"     --input="format=text
file=express-tutorial/song-plays.json"     --lib=${LIBS_DIR}
{% endhighlight %}
</div>


### Custom Importers in KijiExpress
If Kiji's stock bulk importers don't fit your use case, you can also write import jobs in
KijiExpress. This is what we've done to import the song metadata into the songs table.

The source code for this importer is at the bottom of this page, for interested readers. The syntax
will be explained more in-depth in the next section.

KijiExpress programs or scripts can be run using the `express` command. Here, we'll demonstrate how
to run the the song metadata importer as a precompiled job contained in a `jar` file:

<div class="userinput">
{% highlight bash %}
express job --libjars "${MUSIC_EXPRESS_HOME}/lib/*" \
    ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-{{site.music_express_DEVEL_version}}.jar \
    org.kiji.express.music.SongMetadataImporter \
    --input express-tutorial/song-metadata.json \
    --table-uri ${KIJI}/songs --hdfs
{% endhighlight %}
</div>


### Verify Output

After running the importer, you can verify that the Kiji table `songs` contains the imported data
using the `kiji scan` command.

<div class="userinput">
{% highlight bash %}
kiji scan ${KIJI}/songs --max-rows=5
{% endhighlight %}
</div>

You should see something like:

    Scanning kiji table: kiji://localhost:2181/kiji_express_music/songs/
    entity-id=['song-32'] [1365548283995] info:metadata
                                     {"song_name": "song name-32", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre1.0", "tempo": 120, "duration": 180}

    entity-id=['song-49'] [1365548285203] info:metadata
                                     {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre4.0", "tempo": 150, "duration": 180}

    entity-id=['song-36'] [1365548284255] info:metadata
                                     {"song_name": "song name-36", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre1.0", "tempo": 90, "duration": 0}

    entity-id=['song-10'] [1365548282517] info:metadata
                                     {"song_name": "song name-10", "artist_name": "artist-1", "album_name": "album-0", "genre": "genre5.0", "tempo": 160, "duration": 240}

    entity-id=['song-8'] [1365548282382] info:metadata
                                     {"song_name": "song name-8", "artist_name": "artist-1", "album_name": "album-1", "genre": "genre5.0", "tempo": 140, "duration": 180}

We can also use the `kiji scan` command to verify the users table import was successful.

<div class="userinput">
{% highlight bash %}
kiji scan ${KIJI}/users --max-rows=2 --max-versions=5
{% endhighlight %}
</div>

You should see something like:

    entity-id=['user-28'] [1325739120000] info:track_plays
                                     song-25
    entity-id=['user-28'] [1325739060000] info:track_plays
                                     song-23
    entity-id=['user-28'] [1325738940000] info:track_plays
                                     song-25
    entity-id=['user-28'] [1325738760000] info:track_plays
                                     song-28

    entity-id=['user-2'] [1325736420000] info:track_plays
                                     song-4
    entity-id=['user-2'] [1325736180000] info:track_plays
                                     song-3
    entity-id=['user-2'] [1325735940000] info:track_plays
                                     song-4
    entity-id=['user-2'] [1325735760000] info:track_plays
                                     song-28
    entity-id=['user-2'] [1325735520000] info:track_plays
                                     song-0

Now that you've imported your data, we are ready to start analyzing it!  The source code for the
song metadata importer is included below in case you are curious.  We will go over the syntax of
writing your own jobs in more detail in following sections.

### (Optional) Source Code for Scalding Importer

The data is formatted with a JSON record on each line. Each record corresponds to a song, and
provides the following metadata for the song:

* song id
* song name
* artist name
* album name
* genre
* tempo
* duration

The `info:metadata` column of the table contains an Avro record containing this relevant song
metadata.

The importer looks like this:

<div id="accordion-container">
  <h2 class="accordion-header"> SongMetadataImporter.scala </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-express-music/raw/{{site.music_express_DEVEL_branch}}/src/main/scala/org/kiji/express/music/SongMetadataImporter.scala"> </script>
  </div>
</div>
