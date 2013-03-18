CREATE TABLE words WITH DESCRIPTION 'Words in the 20Newsgroups dataset.'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Basic information' (
    word "string" WITH DESCRIPTION 'The word.',
    doubleword "string" WITH DESCRIPTION 'Twice the word.'
  )
);
