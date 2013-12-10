CREATE TABLE 'users' WITH DESCRIPTION 'example for maven archetype'
ROW KEY FORMAT HASH PREFIXED(2)
WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
  MAXVERSIONS = 10,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'information about the user' (
    userId "string" WITH DESCRIPTION 'id number of user',
    name "string" WITH DESCRIPTION 'name of user',
    nameCopy "string" WITH DESCRIPTION 'copy of user name'
  )
);
