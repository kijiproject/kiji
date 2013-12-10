CREATE TABLE product WITH DESCRIPTION 'Foodmart Product Metadata'
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Product Information' (
    product_class_id "int" WITH DESCRIPTION 'product class',
    brand_name "string" WITH DESCRIPTION 'products brand name',
    product_name "string" WITH DESCRIPTION 'products item name',
    sku "long" WITH DESCRIPTION 'stock keeping unit',
    srp "float" WITH DESCRIPTION 'suggested retail price'
  ),
  MAP TYPE FAMILY frequent_itemset_recos { "type" : "string" } WITH DESCRIPTION 'Recommendations using Frequent Itemsets',
  MAP TYPE FAMILY item_item_collab_recos { "type" : "string" } WITH DESCRIPTION 'Recommendations using Item-Item Collaborative Filtering'
);
