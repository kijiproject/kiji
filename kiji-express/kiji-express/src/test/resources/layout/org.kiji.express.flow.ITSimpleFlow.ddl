CREATE TABLE 'table'
ROW KEY FORMAT (row STRING)
WITH LOCALITY GROUP default (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH NONE,
  FAMILY info (
    name "string",
    email "string"
  )
);
