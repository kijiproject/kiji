CREATE TABLE item_item_similarities
  WITH DESCRIPTION 'Top-M list of similar items, using adjusted cosine similarity'
  ROW KEY FORMAT (movie_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    FAMILY most_similar ( COLUMN most_similar
      WITH SCHEMA CLASS org.kiji.modeling.examples.ItemItemCF.avro.AvroSortedSimilarItems )
  );

CREATE TABLE user_ratings WITH DESCRIPTION 'User ratings of movies (movies are rows)'
  ROW KEY FORMAT (user_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    MAP TYPE FAMILY ratings WITH SCHEMA "double" WITH DESCRIPTION 'MovieIds -> ratings'
  );

CREATE TABLE movie_titles
  WITH DESCRIPTION 'movie IDs -> titles'
  ROW KEY FORMAT (movie_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    FAMILY info ( COLUMN title
      WITH SCHEMA "string" )
  );
