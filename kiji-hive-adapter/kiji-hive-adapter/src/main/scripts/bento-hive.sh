#!/usr/bin/env bash
#
#   (c) Copyright 2013 WibiData, Inc.
#
#   See the NOTICE file distributed with this work for additional
#   information regarding copyright ownership.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Configuration parameters for this script
CLOUDERA_URL="http://archive.cloudera.com/cdh4/cdh/4/"
HIVE_VERSION="0.9.0-cdh4.1.2"
HIVE_DIRECTORY="hive-${HIVE_VERSION}"
HIVE_ARCHIVE="hive-${HIVE_VERSION}.tar.gz"
HIVE_URL="${CLOUDERA_URL}${HIVE_ARCHIVE}"
HBASE_JAR="hbase-0.92.1-cdh4.1.2-security.jar"
KIJI_HIVE_ADAPTER_VERSION="0.1.0-SNAPSHOT"

# Resolve a symlink to its absolute target, like how 'readlink -f' works on Linux.
function resolve_symlink() {
  TARGET_FILE=$1

  if [ -z "$TARGET_FILE" ]; then
    echo ""
    return 0
  fi

  cd $(dirname "$TARGET_FILE")
  TARGET_FILE=$(basename "$TARGET_FILE")

  # Iterate down a (possible) chain of symlinks
  count=0
  while [ -L "$TARGET_FILE" ]; do
      if [ "$count" -gt 1000 ]; then
        # Just stop here, we've hit 1,000 recursive symlinks. (cycle?)
        break
      fi

      TARGET_FILE=$(readlink "$TARGET_FILE")
      cd $(dirname "$TARGET_FILE")
      TARGET_FILE=$(basename "$TARGET_FILE")
      count=$(( $count + 1 ))
  done

  # Compute the canonicalized name by finding the physical path
  # for the directory we're in and appending the target file.
  PHYS_DIR=$(pwd -P)
  RESULT="$PHYS_DIR/$TARGET_FILE"
  echo "$RESULT"
}

# Generate a .hiverc file with the appropriate jars
function generate_hiverc() {
  HIVERC=$1
  echo "-- GENERATED FILE DO NOT EDIT" > ${HIVERC}
  echo "add jar ${KIJI_HIVE_LIB};" >> ${HIVERC}
  echo "add jar ${HBASE_LIB};" >> ${HIVERC}
}

# Generate default import table SQL
function generate_import_table_sql() {
  IMPORT_TABLE_URI=$1
  TABLE_NAME="${IMPORT_TABLE_URI##*/}"
  echo Generating import table SQL for ${TABLE_NAME}
  IMPORT_TABLE_SQL="${KIJI_HIVE_ADAPTER_HOME}${TABLE_NAME}-import.sql"
  if [ ! -z ${IMPORT_TABLE_URI} ]; then
    COLUMNS=`kiji ls ${IMPORT_TABLE_URI} | sed -e 's/.*\/\(.*\)\//\1/g'`
    echo "-- GENERATED FILE DO NOT EDIT" > ${IMPORT_TABLE_SQL}
    echo "CREATE EXTERNAL TABLE ${TABLE_NAME} (" >> ${IMPORT_TABLE_SQL}
    FIRST=true
    for x in ${COLUMNS}; do
      if [ "$FIRST" != "true" ]; then
        echo "," >> ${IMPORT_TABLE_SQL}
        FIRST=false
      fi
      QUALIFIER=${x##*:}
      echo -n "  ${QUALIFIER} STRUCT<ts: TIMESTAMP, value: STRING>" >> ${IMPORT_TABLE_SQL}
      FIRST=false
    done
    echo >> ${IMPORT_TABLE_SQL}
    echo ")" >> ${IMPORT_TABLE_SQL}
    echo "STORED BY 'org.kiji.hive.KijiTableStorageHandler'" >> ${IMPORT_TABLE_SQL}
    echo "WITH SERDEPROPERTIES (" >> ${IMPORT_TABLE_SQL}
    echo -n "  'kiji.columns' = '" >> ${IMPORT_TABLE_SQL}
    FIRST=true;
    for x in ${COLUMNS}; do
      if [ "${FIRST}" != "true" ]; then
        echo -n "," >> ${IMPORT_TABLE_SQL}
      fi
      echo -n "$x[0]" >> ${IMPORT_TABLE_SQL}
      FIRST=false
    done
    echo "'" >> ${IMPORT_TABLE_SQL}
    echo ")" >> ${IMPORT_TABLE_SQL}
    echo "TBLPROPERTIES (" >> ${IMPORT_TABLE_SQL}
    echo "  'kiji.table.uri' = '${IMPORT_TABLE_URI}'" >> ${IMPORT_TABLE_SQL}
    echo ");" >> ${IMPORT_TABLE_SQL}

    echo "Wrote sample import table sql to ${IMPORT_TABLE_SQL}"
  fi
}

function usage() {
  echo "Script that provides convenience methods for the Kiji Hive Adapter."
  echo
  echo "USAGE"
  echo
  echo "  ./bento-hive.sh <command> [OPTIONS]..."
  echo
  echo "TOOLS"
  echo "  shell                   Start a Hive shell with the Kiji Hive Adapter jars loaded"
  echo "  generate                Generates a sample CREATE EXTERNAL TABLE statement for"
  echo "                          a given Kiji table URI"
  echo "  import                  Generates statement(as with generate), runs the statement to"
  echo "                          import the table to the metastore, and runs a Hive shell"
}

# Relevant execution parameters
prgm="$0"
prgm=`resolve_symlink "$prgm"`
bin=`dirname "$prgm"`
bin=`cd "${bin}" && pwd`
command="$1"

KIJI_HIVE_ADAPTER_HOME="${bin}/../"
KIJI_HIVE_ADAPTER_LIB="${KIJI_HIVE_ADAPTER_HOME}lib/"

# Set metastore_db to the Kiji Hive Adapter root so that invocations from different directions
# use the same metastore
HIVE_OPTIONS="--hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$KIJI_HIVE_ADAPTER_HOME/metastore_db;create=true"

# First make sure we have everything we need in the environment.
if [ -z "${KIJI_HOME}" -o ! -d "${KIJI_HOME}" ]; then
    echo "Please set your KIJI_HOME environment variable."
    exit 1
fi
if [ -z "${HBASE_HOME}" -o ! -d "${HBASE_HOME}" ]; then
    echo "Please set your HBASE_HOME environment variable."
    exit 1
fi

# If HIVE_HOME isn't set and we find it in our lib directory, use that
if [ -z "${HIVE_HOME}" -a -d "${KIJI_HIVE_ADAPTER_LIB}${HIVE_DIRECTORY}" ]; then
    HIVE_HOME="${KIJI_HIVE_ADAPTER_LIB}${HIVE_DIRECTORY}"
fi

# If we can't find Hive, offer to download it into our lib directory
if [ -z "${HIVE_HOME}" -o ! -d "${HIVE_HOME}" ]; then
    echo HIVE_HOME doesn't exist or doesn't point to a valid location.
    read -p "Would you like to download Hive from Cloudera(y/n)?" INSTALL_HIVE

    if [[ $INSTALL_HIVE =~ ^[Yy]$ ]]; then
      wget ${HIVE_URL} -O ${KIJI_HIVE_ADAPTER_LIB}/${HIVE_ARCHIVE}
      tar -xzf ${KIJI_HIVE_ADAPTER_LIB}/${HIVE_ARCHIVE} -C ${KIJI_HIVE_ADAPTER_LIB}
      HIVE_HOME="${KIJI_HIVE_ADAPTER_LIB}/${HIVE_DIRECTORY}"
    else
      echo "No Hive installation present."
      exit 1
    fi
fi

HIVE_BINARY="${HIVE_HOME}/bin/hive"

KIJI_HIVE_LIB="${KIJI_HIVE_ADAPTER_LIB}/kiji-hive-adapter-${KIJI_HIVE_ADAPTER_VERSION}.jar"
export HADOOP_CLASSPATH="${KIJI_HIVE_LIB}"

HBASE_LIB="${HBASE_HOME}/${HBASE_JAR}"
HIVERC="${KIJI_HIVE_ADAPTER_HOME}.hiverc"

case $command in
    shell)
        generate_hiverc ${HIVERC}
        exec ${HIVE_BINARY} ${HIVE_OPTIONS} -i ${HIVERC}
        exit 0
        ;;
    generate)
        generate_hiverc ${HIVERC}
        IMPORT_TABLE_URI=$2
        generate_import_table_sql ${IMPORT_TABLE_URI}

        TABLE_NAME="${IMPORT_TABLE_URI##*/}"
        SQL_FILENAME="${KIJI_HIVE_ADAPTER_HOME}${TABLE_NAME}-import.sql"
        exit 0
        ;;
    import)
        generate_hiverc ${HIVERC}
        IMPORT_TABLE_URI=$2
        generate_import_table_sql ${IMPORT_TABLE_URI}

        TABLE_NAME="${IMPORT_TABLE_URI##*/}"
        SQL_FILENAME="${KIJI_HIVE_ADAPTER_HOME}${TABLE_NAME}-import.sql"
        ${HIVE_BINARY} ${HIVE_OPTIONS} -i ${HIVERC} -f ${SQL_FILENAME}
        if [ $? -ne 0 ]; then
          echo "Failed to import table '${TABLE_NAME}'."
          exit 1
        else
          echo "Successfully imported table '${TABLE_NAME}' into Hive! Starting a shell..."
          exec ${HIVE_BINARY} ${HIVE_OPTIONS} -i ${HIVERC}
          exit 0
        fi
        ;;
    *)
        usage
        exit 1
        ;;
esac


