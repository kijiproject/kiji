---
layout: post
title: Importing Data
categories: [tutorials, express-recommendation, DEVEL]
tags: [express-music]
order: 3
description: Importing data files into Kiji tables.
---

<h3 style="margin-top:0px;padding-top:10px;">Custom Importers</h3>
In this section of the tutorial, we will import metadata about songs into the Kiji table `songs`,
and import data about when users have listened to songs into the Kiji table `users`. The importer
for song metadata is in the file `SongMetadataImporter.scala`, while the importer for a user's song
plays history is in the file `SongPlaysImporter.scala`. Both of these files define importers that
can be run as precompiled jobs. They also exist in script form as `SongMetadataImporter.express` and
`SongPlaysImporter.express`.  Later, we'll demonstrate how to run the importers both as precompiled
jobs and as scripts (although it's only necessary to run the importers one of these ways).

In this section, we'll provide more details on the song metadata importer. The importer for a user's
song play history is similar. The importer for song metadata reads a file in HDFS that contains a
JSON record of song metadata on each line, and writes metadata (contained in an Avro record) to a
Kiji table.

Each JSON record contained in the input file corresponds to a song, and provides the following
metadata for the song:

* song id
* song name
* artist name
* album name
* genre
* tempo
* duration

Our importer will flatten each of these JSON records into Scalding tuple fields, and then pack those
tuple fields into an Avro record. To do this, we'll use a few user defined functions (UDF). A UDF in
KijiExpress is simply a Scala function; any Scala function can potentially be used as a UDF in a
KijiExpress flow. We'll first explain the UDFs used in our import flow before explaining the
flow itself.

The UDF `parseJson` takes as an argument a string containing the JSON for an individual record, and
returns a Scala tuple containing one element for each bit of metadata. The code for this function is
below.

{% highlight java %}
 def parseJson(json: String): (String, String, String, String, String, Long, Long) = {
    val metadata = JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
    (metadata.get("song_id").get.asInstanceOf[String],
        metadata.get("song_name").get.asInstanceOf[String],
        metadata.get("album_name").get.asInstanceOf[String],
        metadata.get("artist_name").get.asInstanceOf[String],
        metadata.get("genre").get.asInstanceOf[String],
        metadata.get("tempo").get.asInstanceOf[String].toLong,
        metadata.get("duration").get.asInstanceOf[String].toLong)
  }
{% endhighlight %}

As an example, suppose this function received the following JSON string as input:

{% highlight bash %}
{ "song_id" : "song-0", "song_name" : "song name-0", "artist_name" : "artist-1", "album_name" : "album-1", "genre" : "genre5.0", "tempo" : "100", "duration" : "240" }
{% endhighlight %}

It would then return the following Scala tuple:

{% highlight bash %}
(“song-0”, "song name-0", "artist-1", "album-1", "genre5.0", 100, 240)
{% endhighlight %}

Besides flattening JSON records into individual fields, we'll also have to create an entity id for
each song that identifies the row in the Kiji table where the metadata should be written. We will
use the `song_id` provided in the JSON metadata to form the entity id. The Scala function `entityId`
will act as a UDF that transforms a song id into an entity id for the Kiji table `songs`.

{% highlight java %}
  def entityId(songId: String): EntityId = {
    val uri = KijiURI.newBuilder(args("table-uri")).build()
    retainAnd(Kiji.Factory.open(uri)) { kiji =>
      retainAnd(kiji.openTable(uri.getTable)) { table =>
        table.getEntityId(songId)
      }
    }
  }
{% endhighlight %}

The layout of a Kiji table determines the format of the entity ids used in that table. As a result, we
need to build a Kiji URI for our table `KijiURI.newBuilder(args("table-uri")).build()`, get a handle
to a Kiji instance `Kiji.Factory.open(uri)`, open the table `kiji.openTable(uri.getTable)` and
finally use the open table to create an entity id using the song_id string
`table.getEntityId(songId)`.

Using these two UDFs, we can write our import flow.

{% highlight java %}
TextLine(args("input"))
      .map('line ->
          ('songId, 'songName, 'albumName, 'artistName, 'genre, 'tempo,'duration)) { parseJson }
      .map('songId -> 'entityId) { entityId }
      .pack[SongMetadata](('songName, 'albumName, 'artistName, 'genre, 'tempo, 'duration)
          -> 'metadata)
      .write(KijiOutput(args("table-uri"))('metadata -> "info:metadata"))
{% endhighlight %}

Let's step through it line by line.

#### Read JSON records from an input file in HDFS
{% highlight java %}
TextLine(args("input"))
{% endhighlight %}
This is done by using `TextLine`, a Scalding `Source` that views an input file as a collection
of tuples of the form `(offset, line)`. Here, `offset` is the offset within the file and `line`
is the contents of an individual line from the file. This stream of tuples is called a `pipe` in
Cascading terminology.

#### Flatten each JSON record into a tuple with fields corresponding to the song metadata extracted from the JSON record
{% highlight java %}
.map('line ->
    ('songId, 'songName, 'albumName, 'artistName, 'genre, 'tempo,'duration)) { parseJson }
{% endhighlight %}
We now chain this to a `map` operation. Map-like functions operate over individual rows in a pipe,
usually transforming them in some way. The new fields that are transformations of existing ones are
added to the original tuple. In our code, we take a line and convert it into a named tuple of
`('songId, 'songName, 'albumName, 'artistName, 'genre, 'tempo,'duration)` using the parseJson
UDF described above.

#### Transform the song id for each song into an entity id for the songs table
{% highlight java %}
.map('songId -> 'entityId) { entityId }
{% endhighlight %}
The next `map` transforms the `song_id` field from the tuple into an entity id, and adds this to the
tuple in a field named `entityId`. We supply the `entityId` UDF described above to accomplish
this transformation.

#### Pack song name, album name, artist name, genre, tempo, and duration for the song into an Avro record
{% highlight java %}
.pack[SongMetadata](('songName, 'albumName, 'artistName, 'genre, 'tempo, 'duration)
      -> 'metadata)
{% endhighlight %}
Scalding lets you
[pack](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference#wiki-pack) a set of
fields into a single object using reflection. In our code, SongMetadata is an Avro record consisting
of the fields described above. We now add a new field called `metadata` to each of the tuples whose
value is the Avro record created from the song metadata. This will be the object that gets written to the
Kiji table.

#### Write the Avro records to the column "info:metadata" in a row for the song in a Kiji table
{% highlight java %}
.write(KijiOutput(args("table-uri"))('metadata -> "info:metadata"))
{% endhighlight %}
Finally, we use `KijiOutput` to create a Scalding `Source` that can write to a Kiji table.
The argument `table-uri` contains the URI of the Kiji table to write to. We write the contents of
the tuple field `metadata` (our SongMetdata Avro record) to the Kiji table column `info:metadata`.
KijiExpress will automatically use the contents of the tuple field `entityId` as the entity id of
the row in the Kiji table to write to.

### Running the importers

Now we will run both the song metadata importer explained above and the song play history importer
to import data into the Kiji tables `songs` and `users`.

KijiExpress programs or scripts can be run using the `express` command. First, we'll demonstrate how
to run the importers as precompiled jobs contained in a `jar` file. To run the song metadata
importer, we use the following command.

<div class="userinput">
{% highlight bash %}
express job ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-DEVEL.jar \
    org.kiji.express.music.SongMetadataImporter \
    --input express-tutorial/song-metadata.json \
    --table-uri ${KIJI}/songs --hdfs
{% endhighlight %}
</div>

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

To run the song play history importer, we can use a similar command.

<div class="userinput">
{% highlight bash %}
express job ${MUSIC_EXPRESS_HOME}/lib/kiji-express-music-DEVEL.jar \
    org.kiji.express.music.SongPlaysImporter \
    --input express-tutorial/song-plays.json \
    --table-uri ${KIJI}/users --hdfs
{% endhighlight %}
</div>

We can again use the `kiji scan` command to verify the importer was successful.

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

Both importers also exist in script form, in the folder `${EXPRESS_MUSIC_HOME}/scripts`. As scripts,
the importers can be run as:

<div class="userinput">
{% highlight bash %}
express script ${EXPRESS_MUSIC_HOME}/scripts/SongMetadataImporter.express --hdfs
{% endhighlight %}
</div>

<div class="userinput">
{% highlight bash %}
express script ${EXPRESS_MUSIC_HOME}/scripts/SongPlaysImporter.express --hdfs
{% endhighlight %}
</div>

