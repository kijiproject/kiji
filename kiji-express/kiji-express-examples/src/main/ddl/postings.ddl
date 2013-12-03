CREATE TABLE postings WITH DESCRIPTION 'Newsgroup posts in the 20Newsgroups dataset.'
ROW KEY FORMAT (group STRING, post_name STRING, HASH (THROUGH post_name, SIZE = 2))
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Basic information' (
    post "string" WITH DESCRIPTION 'The text of the post.',
    group "string" WITH DESCRIPTION 'The newsgroup that this posting belongs to.',
    postLength "int" WITH DESCRIPTION 'The number of words in this posting.',
    segment "int" WITH DESCRIPTION 'The training or test segment of this post. Training = 1, Test = 0'
  )
);
