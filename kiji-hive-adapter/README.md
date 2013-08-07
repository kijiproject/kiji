Introduction
============

The official Hive adapter for Kiji Schema 1.x.

The Kiji Hive Adapter can be used to for read-only access to
data stored in Kiji tables from Hive.

## Requirements
   * Hadoop 2.0.0-mr1-cdh4.3.0
   * HBase 0.94.6-cdh4.3.0
   * Hive 0.10.0-cdh4.3.0
   * KijiSchema 1.1.x

## Automatic Hive Shell
The included bin/bento-hive.sh script can be executed to automatically start a Hive shell
with the Kiji Hive adapter(and its dependencies preloaded).  Any jars that are referenced in
`HADOOP_CLASSPATH` will also be added within the Hive shell.  If necessary, this script will
automatically download Apache Hive from Cloudera.

This script can take an argument of a Kiji table URI to automatically create the table handler
from the specified table.  Currently this only supports String types, but the generated SQL
can be modified to change the relevant types.

## Manual Hive Installation

If you don't already have Apache Hive installed, you can download it from Cloudera:

    $ wget http://archive.cloudera.com/cdh4/cdh/4/hive-0.10.0-cdh4.3.0.tar.gz

Extract the archive

    $ tar -xzf hive-0.10.0-cdh4.3.0.tar.gz

Set up your environment by setting the `HIVE_HOME` environment variable and adding the hive
commands to your path:

    $ export HIVE_HOME=/path/to/hive-0.10.0-cdh4.3.0
    $ export PATH=$PATH:$HIVE_HOME/bin

## Manual Setup

To use the Kiji Hive Adapter, start hive with the JAR on your
classpath with the `HADOOP_CLASSPATH` environment variable.

    $ HADOOP_CLASSPATH=/path/to/kiji-hive-adapter-${project.version}.jar

At the hive shell, add required JARs to the distributed cache using
the 'add jar' command. Add the Kiji Hive Adapter JAR and the HBase
JAR from `$HBASE_HOME`.

    hive> add jar /path/to/kiji-hive-adapter-${project.version}.jar;
    hive> add jar /path/to/hbase-<version>.jar;

You will need to add these JARs each time you start a hive session.

### Creating Views of Kiji Tables

There are 4 things you will always specify when creating Hive table
view over an existing Kiji table.

   1. Use the `EXTERNAL` keyword to reference an existing Kiji table.
   2. Use `STORED BY 'org.kiji.hive.KijiTableStorageHandler'` so Hive
      knows how to speak Kiji.
   3. Use `WITH SERDEPROPERTIES` to specify the mapping of Kiji table
      columns into Hive columns using the 'kiji.columns' property.
   4. (Optional) Use the 'kiji.entity.id.shell.string' within the 
      `WITH SERDEPROPERTIES` section to specity the source of the EntityId 
      shell string to create a view that is writable.
   4. Use `TBLPROPERTIES` to specify the URI of the Kiji table.

This is best explained with an example.

### Sample Table

The Kiji Music example has an example table with some imported data that is imported as part
of the tutorial.  The table that we will be using is a users table that tracks each individual
user, their historical playlist, and their next song recommendations.  The SQL statement
necessary to setup this table to be used in Hive looks like:

    CREATE EXTERNAL TABLE users (
        user STRING,
        track_plays STRUCT<ts: TIMESTAMP, value: STRING>,
        next_song_rec STRUCT<ts: TIMESTAMP, value: STRING>
    )
    STORED BY 'org.kiji.hive.KijiTableStorageHandler'
    WITH SERDEPROPERTIES (
        'kiji.columns' = ':entity_id,info:track_plays[0],info:next_song_rec[0]',
        'kiji.entity.id.shell.string' = 'user'
    )
    TBLPROPERTIES (
        'kiji.table.uri' = 'kiji://.env/kiji_music/users'
    );

If you'd like to load this automatically you can run the script:

    bin/bento-hive.sh import kiji://.env/kiji_music/users

against a Bento cluster where this table has already been created and has the data populated.
This script only works with string object types, but this could be a baseline for a custom
`CREATE EXTERNAL TABLE` statement.


### Sample Queries

These SQL statements all rely on the above table being created in Hive.  You can start a Hive
shell with the necessary jars for Kiji tables via the command:
`bin/bento-hive.sh shell`

List all of the tables that have been created:

    SHOW TABLES;

List all of the fields in the table 'users':

    DESCRIBE users;

List all of the tracks that were played:

    SELECT track_plays.value FROM users;

Show only the first 10 users:

    SELECT track_plays.value FROM users LIMIT 10;

List all tracks that were played by play order:

    SELECT track_plays.ts,track_plays.value FROM users order by ts ASC;

List all of the times in which song-44 was played:

    SELECT track_plays.ts,track_plays.value FROM users WHERE track_plays.value = 'song-44';

List all songs that were played before the hour is less than 10 AM:

    SELECT track_plays.ts, track_plays.value FROM users WHERE HOUR(track_plays.ts) < 10;

Get a list of songs by popularity:

    SELECT track_plays.value,COUNT(1) AS COUNT FROM users group by track_plays.value ORDER BY COUNT DESC;

If you've created this users in a different instance as new_users, you can
use the write functionality to copy rows between these:

    INSERT INTO TABLE new_users SELECT * FROM USERS;

If you want more inspiration for queries take a look here:
https://cwiki.apache.org/Hive/tutorial.html#Tutorial-UsageandExamples

### Mapping Kiji Table Data into Hive

Each column declared in the Hive table needs to have a corresponding
entry in the 'kiji.columns' property of the `WITH SERDEPROPERTIES`
clause. The value of 'kiji.columns' is a comma-separated list of
Kiji row expressions. The number of Hive columns declared in the `CREATE
EXTERNAL TABLE` clause must match the number of Kiji row expressions in
your 'kiji.columns' value.

### Kiji Row Expressions

A Kiji row expression addresses a piece of data in a Kiji table
row. The form of a Kiji row expression is:

    +---------------------+---------------------------------------------+
    | Expression          | Hive Type                                   |
    +---------------------+---------------------------------------------+
    | family              | MAP<STRING, ARRAY<STRUCT<TIMESTAMP, cell>>> |
    | family[n]           | MAP<STRING, STRUCT<TIMESTAMP, cell>>        |
    | family:qualifier    | ARRAY<STRUCT<TIMESTAMP, cell>>              |
    | family:qualifier[n] | STRUCT<TIMESTAMP, cell>                     |
    +---------------------+---------------------------------------------+

It is important that the types of the Hive columns in the `CREATE`
statement match the type of data generated by the Kiji row
expressions according to the table above. For a family expression, the
column will contain a map from qualifier to the array of cells in
reverse chronological order. For column expressions, the column will
contain a struct with two fields: the timestamp and the cell value.

The symbol `n` specifies the index of the cell to retrieve, where the
0-th cell is the one with the greatest timestamp (most recent). For
example, `event:click[0]` represents the most recent cell from the
`click` column of the `event` family, and `event:click[1]` represents
the second most recent cell (the previous click). You may also use the
special index `-1` to specify the cell with the least timestamp
(oldest).

The type of the cell value depends on the Avro data type in the Kiji
table. For primitive types, the relationship is obvious (e.g. an Avro
"string" maps to a Hive STRING).

    +----------------------------+---------------------------+
    | Avro Type                  | Hive Type                 |
    +----------------------------+---------------------------+
    | "boolean"                  | BOOLEAN                   |
    | "int"                      | INT                       |
    | "long"                     | BIGINT, TIMESTAMP         |
    | "float"                    | FLOAT                     |
    | "double"                   | DOUBLE                    |
    | "string"                   | STRING                    |
    | "bytes"                    | BINARY                    |
    | "fixed"                    | BINARY, BYTE, SHORT       |
    | "null"                     | VOID                      |
    | record { T f1; U f2; ... } | STRUCT<f1: T, f2: U>      |
    | map<T>                     | MAP<STRING, T>            |
    | array<T>                   | ARRAY<T>                  |
    | union {T, U, ...}          | UNIONTYPE<T, Y, ...>      |
    | union {null, T}            | T                         |
    +----------------------------+---------------------------+

Note that we special case union types with null to just return the raw
Hive type, since Hive types are already nullable.

For any types that aren't explicitly supported within Hive(like enumerations),
the STRING type can be used in the Hive definition to use the type's toString()
representation.

There are two additional meta columns that are available to allow you to read
the entity ID for a row.

    +---------------------+---------------------------------------------+
    | Expression          | Hive Type                                   |
    +---------------------+---------------------------------------------+
    | :entity_id          | STRING                                      |
    | :entity_id[n]       | INT, BIGINT, or STRING                      |
    +---------------------+---------------------------------------------+

The first form requires the literal string `:entity_id` and will result in
a `STRING` formatted entity ID.  The second form allows you to specify
a component index and will result in the value of that component, which can be
one of `INT`, `BIGINT`, or `STRING`.

### Write access caveats

In order for the Kiji Hive Adapter to write back to a Kiji table, we need to
determine the row key to write back to.  Currently we use the notion of the
EntityId's shell string(found in the Kiji CLI) to form the row key.  There is 
a ticket(KIJIHIVE-30) to support building the EntityId from columns
representing each of the row key components.

For writing to a writable Hive view of a table, it is required that all columns
are present within the query.  This is a limitation of Hive, and if you only
only are writing a subset of columns, you will need to create another view
that only contains the relevant columns(and the EntityId).

Kiji Hive Adapter currently only supports write access with primitive types.
Complex Avro records would involve work done on KIJIHIVE-31.  In addition, to
determine the row key to write to(the Kiji EntityId), we require the use of the
EntityId's shell string(which is the view presented within the Kiji CLI.

Happy querying!
