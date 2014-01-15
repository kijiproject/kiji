CREATE TABLE user_ratings
  WITH DESCRIPTION 'User ratings of movies (movies are rows)'
  ROW KEY FORMAT (user_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    MAP TYPE FAMILY ratings WITH SCHEMA "double" WITH DESCRIPTION 'MovieIds -> ratings'
  );
