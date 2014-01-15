CREATE TABLE item_item_similarities
  WITH DESCRIPTION 'Top-M list of similar items, using adjusted cosine similarity'
  ROW KEY FORMAT (movie_id LONG)
  WITH LOCALITY GROUP default WITH DESCRIPTION 'Main locality group' (
    FAMILY most_similar ( COLUMN most_similar
      WITH SCHEMA CLASS org.kiji.modeling.examples.ItemItemCF.avro.AvroSortedSimilarItems )
  );
