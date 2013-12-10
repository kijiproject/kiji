CREATE TABLE sales WITH DESCRIPTION 'Foodmart checkout details'
ROW KEY FORMAT (customer_id LONG, time_id INT)
WITH LOCALITY GROUP default WITH DESCRIPTION 'Order Information' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  MAP TYPE FAMILY purchased_items { "type" : "int" } WITH DESCRIPTION 'key:product_id, value:number of units'
);

