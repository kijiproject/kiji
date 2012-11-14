CREATE TABLE phonebook WITH DESCRIPTION 'A collection of phone book entries'
ROW KEY FORMAT HASHED
WITH LOCALITY GROUP default
  WITH DESCRIPTION 'Main locality group' (
  MAXVERSIONS = 10,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH NONE,
  FAMILY info WITH DESCRIPTION 'basic information' (
    firstname "string" WITH DESCRIPTION 'First name',
    lastname "string" WITH DESCRIPTION 'Last name',
    email "string" WITH DESCRIPTION 'Email address',
    telephone "string" WITH DESCRIPTION 'Telephone number',
    address CLASS org.kiji.examples.phonebook.Address WITH DESCRIPTION 'Street address'
  ),
  FAMILY derived WITH DESCRIPTION 'Information derived from an individual\'s address.' (
    addr1 "string" WITH DESCRIPTION 'Address line one.',
    apt "string" WITH DESCRIPTION 'Address Apartment number.',
    addr2 "string" WITH DESCRIPTION 'Address line two.',
    city "string" WITH DESCRIPTION 'Address city.',
    state "string" WITH DESCRIPTION 'Address state.',
    zip "string" WITH DESCRIPTION 'Address zip code.'
  ),
  FAMILY stats WITH DESCRIPTION 'Statistics about a contact.' (
    talktime COUNTER WITH DESCRIPTION 'Time spent talking with this person'
  )
);
