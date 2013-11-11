CREATE TABLE users WITH DESCRIPTION 'A set of users'
ROW KEY FORMAT HASH PREFIXED(4)
WITH LOCALITY GROUP default
  WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH NONE,
  FAMILY info WITH DESCRIPTION 'Information about a user' (
    track_plays "string" WITH DESCRIPTION 'Tracks played by the user',
    next_song_rec "string" WITH DESCRIPTION 'Next song recommendation based on play history for a user'
  )
);

CREATE TABLE songs WITH DESCRIPTION 'Songs available for playing'
ROW KEY FORMAT HASH PREFIXED(4)
WITH LOCALITY GROUP default
  WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH NONE,
  FAMILY info WITH DESCRIPTION 'Information about a song' (
    metadata {"type" : "record",
              "name" : "SongMetadata",
              "namespace": "org.kiji.express.music.avro",
              "fields" :
                [ {"name" : "song_name", "type" : "string"},
                  {"name" : "album_name", "type" : "string"},
                  {"name" : "artist_name", "type" : "string"},
                  {"name" : "genre", "type" : "string"},
                  {"name" : "tempo", "type" : "long"},
                  {"name" : "duration", "type" : "long"}] } WITH DESCRIPTION 'Song metadata',
    top_next_songs WITH SCHEMA { "type" : "record", "namespace": "org.kiji.express.music.avro", "name" : "TopSongs", "fields" :
        [{"name" : "top_songs", "type" :
            {"type" : "array", "items" : { "type" : "record", "name" : "SongCount", "fields" :
                [{"name" : "song_id", "type" : "string"},
                 {"name" : "count", "type" : "long"}]
            } }
        } ]
    } WITH DESCRIPTION 'The most likely next songs to be played, and their counts.'
  )
);
