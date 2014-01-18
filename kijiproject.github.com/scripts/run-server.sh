#!/bin/bash

# Temp file for the sed work
TMP_FILE=$(mktemp /tmp/config.XXXXXXXXXX)

# Get the currect directory no matter where we are or how we're called
SOURCE="${BASH_SOURCE[0]}"
# resolve $SOURCE until the file is no longer a symlink
while [ -h "$SOURCE" ]; do 
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  # if $SOURCE was a relative symlink, we need to resolve it relative to the 
  # path where the symlink file was located
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" 
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"


# run when user hits ctrl-c to bail out of jekyll
control_c()
{
  sed -e's/UA-REMOVED-DONT-COMMIT/UA-35866189-1/' $DIR/../_config.yml > $TMP_FILE
  mv $TMP_FILE $DIR/../_config.yml
  exit $?
}
 
# trap keyboard interrupt (control-c)
trap control_c SIGINT

# we're using the long route over sed in place because sed in place
# doesn't work the same on linux and macs.
sed -e's/UA-35866189-1/UA-REMOVED-DONT-COMMIT/' $DIR/../_config.yml > $TMP_FILE
mv $TMP_FILE $DIR/../_config.yml

jekyll serve --safe --watch
