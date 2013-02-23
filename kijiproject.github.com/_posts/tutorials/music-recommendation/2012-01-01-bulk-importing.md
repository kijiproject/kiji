---
layout: post
title : Bulk Importing
categories: [tutorials, music-recommendation, 1.0.0-rc4]
tags: [music]
order : 3
description: Bulk importing data into a Kiji table.
---

In cases where we have a significant amount of existing data that we'd like to load into a Kiji
table, it hardly makes sense to do it a row at a time. We will show you how to use MapReduce to import
large amounts of data into Kiji, efficiently.

### Custom Bulk Importers

One of the ways to bulk import your data is to extend `KijiBulkImporter` and use its produce() method
to insert rows into the Kiji table. In the example below, we use this method to populate the song
metadata.

Input files contain JSON data representing song metadata, with one song per line. Below is an
example of a JSON record in our input.

{% highlight js %}
{
    "song_id" : "0",
    "song_name" : "song0",
    "artist_name" : "artist1",
    "album_name" : "album1",
    "genre" : "awesome",
    "tempo" : "140",
    "duration" : "180"
}
{% endhighlight %}

The `SongMetadataBulkImporter` class extends `KijiBulkImporter`. It expects a
[text input format]({{site.userguide_mapreduce_rc4}}/command-line-tools/#input) where
input keys are the positions (in bytes) of each line in input file and input values are the lines
of text described above.

In the produce method of the class, we extract the json as follows:
{% highlight java %}
// Parse JSON:
final JSONObject json = (JSONObject) parser.parse(line.toString());

// Extract JSON fields:
final String songId = json.get("song_id").toString();
{% endhighlight %}

We use an Avro record called SongMetaData described below:
{% highlight java %}
record SongMetadata {
    string song_name;
    string artist_name;
    string album_name;
    string genre;
    long tempo;
    long duration;
    }
{% endhighlight %}

We then build the Avro metadata record from the parsed json.

{% highlight java %}
final SongMetadata song = SongMetadata.newBuilder()
      .setSongName(songName)
      .setAlbumName(albumName)
      .setArtistName(artistName)
      .setGenre(genre)
      .setTempo(tempo)
      .setDuration(duration)
      .build();
{% endhighlight %}

We create an [`EntityId`]({{site.api_schema_rc4}}/EntityId.html) object in order to use the song ID as the row key.
{% highlight java %}
final EntityId eid = context.getEntityId(songId);
{% endhighlight %}

Finally, we write this avro record to a cell in our Kiji table with the song ID as the row key.
{% highlight java %}
context.put(eid, "info", "metadata", song);
{% endhighlight %}

*As an aside, care must be taken while using explicit timestamps while writing to Kiji. You can read 
[this post](http://www.kiji.org/2013/02/13/common-pitfalls-of-timestamps-in-hbase/) on the Kiji blog
for more details about this.*

### Running the Example

<div class="userinput">
{% highlight bash %}
$ kiji bulk-import \
    --importer=org.kiji.examples.music.bulkimport.SongMetadataBulkImporter \
    --lib=${LIBS_DIR} \
    --output="format=kiji table=${KIJI}/songs nsplits=1" \
    --input="format=text file=kiji-mr-tutorial/song-metadata.json"
{% endhighlight %}
</div>

#### Verify
<div class="userinput">
{% highlight bash %}
$ kiji ls --kiji=${KIJI}/songs --max-rows=3
{% endhighlight %}
</div>

    entity-id='song-32' [1361561116668] info:metadata
                                 {"song_name": "song name-32", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre4.0", "tempo": 130, "duration": 120}

    entity-id='song-49' [1361561116737] info:metadata
                                 {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre7.0", "tempo": 80, "duration": 240}

    entity-id='song-36' [1361561116684] info:metadata
                                 {"song_name": "song name-36", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre4.0", "tempo": 170, "duration": 120}

### Bulk importing using table import descriptors

Another way to bulk import your data is by using import descriptors.
At the top-level, a table import descriptor contains:

 *   The table that is the destination of the import.
 *   The table column families.
 *   The source for the entity id.
 *   An optional timestamp to use instead of system timestamp.
 *   The format version of the import descriptor.

Each column family has:

 *   The name of the destination column.
 *   The name of the source field to import from.

In the example below, we use import descriptors to bulk import our history of song plays into the
user table. The import descriptor used for the user table is shown below:

{% highlight bash %}
{
  name : "users",
  families : [ {
    name : "info",
    columns : [ {
      name : "track_plays",
      source : "song_id"
    } ]
  } ],
  entityIdSource : "user_id",
  overrideTimestampSource : "play_time",
  version : "import-1.0"
}
{% endhighlight %}

We then use the pre-written `JSONBulkImporter` which expects a JSON file. Each line in this file
represents a separate JSON object to be imported into a row. The JSON object is described by an
import descriptor such as the one above. Target columns whose sources are not present in the JSON
object are skipped.

This descriptor defines a MapReduce job, where every row of the input file is parsed,
and inserted into the `users` Kiji table. The value of `user_id` will
be used as the row key in the Kiji table, the timestamp will be retrieved from the `play_time`
field. The value of `song_id` will be extracted and inserted into the `info:track_plays` column.

### Running the Example

Copy the descriptor file into HDFS.

<div class="userinput">
{% highlight bash %}
$ hadoop fs -copyFromLocal \
    $KIJI_HOME/examples/music/import/song-plays-import-descriptor.json \
    kiji-mr-tutorial/
{% endhighlight %}
</div>

Run the JSON bulk importer.

<div class="userinput">
{% highlight bash %}
$ kiji bulk-import \
    -Dkiji.import.text.input.descriptor.path=kiji-mr-tutorial/song-plays-import-descriptor.json \
    --importer=org.kiji.mapreduce.lib.bulkimport.JSONBulkImporter \
    --output="format=kiji table=${KIJI}/users nsplits=1" \
    --input="format=text file=kiji-mr-tutorial/song-plays.json" \
    --lib=${LIBS_DIR}
{% endhighlight %}
</div>

#### Verify
<div class="userinput">
{% highlight bash %}
$ kiji ls --kiji=${KIJI}/users --max-rows=3
{% endhighlight %}
</div>

    entity-id='user-41' [1325750820000] info:track_plays
                                 song-43

    entity-id='user-3' [1325756880000] info:track_plays
                                 song-1

    entity-id='user-13' [1325752080000] info:track_plays
                                 song-23

