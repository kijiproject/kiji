CREATE TABLE movie_titles
  WITH DESCRIPTION 'movie IDs -> titles'
  ROW KEY FORMAT (movie_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    FAMILY info ( COLUMN title
      WITH SCHEMA "string" )
  );
