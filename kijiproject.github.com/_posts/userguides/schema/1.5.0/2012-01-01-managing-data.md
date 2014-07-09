---
layout: post
title: Managing Data
categories: [userguides, schema, 1.5.0]
tags: [schema-ug]
version: 1.5.0
order: 3
description: How to manage data with KijiSchema.
---

Every table in Kiji has an associated layout. The layout of a table contains a
baseline schema definition which can be used to access the majority of the
information in the table without further special knowledge.  Each table's layout
describes the set of columns which can exist in any given row.  For each column,
a minimal "reader" schema is specified; this provides multiple team members
working with a large data set with a common data dictionary; it also enables
validation of cell data against a reference schema.  The layout also describes
some additional properties of the table; these are discussed in this section as
well.

Every data element in Kiji is stored in a Kiji cell, which is uniquely
identified by an entity ID, column name, and timestamp.  The column name is itself
two components: a family name and a qualifier.  As in HBase, these are written
`family:qualifier`.  Data written to a Kiji cell is serialized to a byte array
according to an Avro schema.  The writer schema used for the particular write
operation is stored alongside the cell data, so the input data can be
deserialized exactly by subsequent read requests. But this schema must be
compatible with the expected reader schema specified in the layout for the cell.

There are two ways to edit the layout of a Kiji table:

*  Using the KijiSchema DDL shell
*  Editing the JSON layout description directly.

Most users should use the KijiSchema DDL shell. It has a user-friendly language
for creating, modifying, and describing tables. To learn more about this
mechanism, skip ahead to the [DDL Shell reference](../schema-shell-ddl-ref).

The following section describes the attributes of a table at the low, JSON level.
This low-level layout was created by executing the following DDL in the
[KijiSchema DDL shell](../schema-shell-ddl-ref):

    USE JAR INFILE '/path/to/searchrecord.jar';

    CREATE TABLE users WITH DESCRIPTION 'A bunch of made-up users'
    ROW KEY FORMAT(uid LONG)
    WITH LOCALITY GROUP default WITH DESCRIPTION 'The default locality group' (
      FAMILY info WITH DESCRIPTION 'A bunch of fields' (
          id "string" WITH DESCRIPTION 'user id hash' ,
          name "string" WITH DESCRIPTION 'The person\'s name',
          email "string" WITH DESCRIPTION 'The person\'s email' ),
      MAP TYPE FAMILY search CLASS com.search.avro.Search
      WITH DESCRIPTION 'The recent search queries the user has made');

This requires that a compiled `com.search.avro.Search` Avro record class be present in
`searchrecord.jar`; the record IDL file may look something like:

    @namespace("com.search.avro")
    protocol searchprotocol {
      record Search {
        string query_term;
      }
    }

Viewing and editing the JSON layout description is a low-level task, typically
performed by system administrators, or for debugging purposes only. Its format
is described below.

A JSON layout descriptor is a specification for the locality groups,
columns, and data types that comprise a table, written as a JSON
document whose elements are described in the following subsections.  We will
refer to the following example layout file throughout this section:

{% highlight js %}
{
  "name" : "users",
  "description" : "A bunch of made-up users",
  "keys_format" : {
    "org.kiji.schema.avro.RowKeyFormat2" : {
      "encoding" : "FORMATTED",
      "salt" : {
        "hash_size" : 2
      },
      "components" : [ {
        "name" : "uid",
        "type" : "LONG"
      } ]
    }
  },
  "locality_groups" : [ {
    "id" : 1,
    "name" : "default",
    "description" : "The default locality group",
    "in_memory" : false,
    "max_versions" : 1,
    "ttl_seconds" : 2147483647,
    "bloom_type" : "NONE",
    "compression_type" : "NONE",
    "families" : [ {
      "id" : 1,
      "name" : "info",
      "description" : "A bunch of fields",
      "columns" : [ {
        "id" : 1,
        "name" : "id",
        "description" : "user id hash",
        "column_schema" : {
          "storage" : "UID",
          "type" : "AVRO",
          "avro_validation_policy" : "STRICT",
          "default_reader" : {
            "uid" : 0
          },
          "readers" : [ {
            "uid" : 0
          } ],
          "written" : [ {
            "uid" : 0
          } ],
          "writers" : [ {
            "uid" : 0
          } ]
        }
      }, {
        "id" : 2,
        "name" : "name",
        "description" : "The person's name",
        "column_schema" : {
          "storage" : "UID",
          "type" : "AVRO",
          "avro_validation_policy" : "STRICT",
          "default_reader" : {
            "uid" : 0
          },
          "readers" : [ {
            "uid" : 0
          } ],
          "written" : [ {
            "uid" : 0
          } ],
          "writers" : [ {
            "uid" : 0
          } ]
        }
      }, {
        "id" : 3,
        "name" : "email",
        "description" : "The person's email",
        "column_schema" : {
          "storage" : "UID",
          "type" : "AVRO",
          "avro_validation_policy" : "STRICT",
          "default_reader" : {
            "uid" : 0
          },
          "readers" : [ {
            "uid" : 0
          } ],
          "written" : [ {
            "uid" : 0
          } ],
          "writers" : [ {
            "uid" : 0
          } ]
        }
      } ]
    }, {
      "id" : 2,
      "name" : "search",
      "description" : "The recent search queries the user has made",
      "map_schema" : {
        "storage" : "UID",
        "type" : "AVRO",
        "avro_validation_policy" : "STRICT",
        "specific_reader_schema_class" : "com.search.avro.Search",
        "default_reader" : {
          "uid" : 8
        },
        "readers" : [ {
          "uid" : 8
        } ],
        "written" : [ {
          "uid" : 8
        } ],
        "writers" : [ {
          "uid" : 8
        } ]
      }
    } ]
  } ],
  "version" : "layout-1.3",
  "layout_id" : "0"
}
{% endhighlight %}

The schema of the table layout descriptor is available in the KijiSchema source tree at
[Layout.avdl](#ref.table_layout_desc).

### Overall structure of a table layout

At the top-level, a table contains:

*  the table name and description;
*  a description of the row keys encoding;
*  the table locality groups.

Each locality group has:

*  a primary name, unique within the table, a description and optionally some name aliases;
*  whether the data is to be stored in memory or on disk;
*  data retention lifetime;
*  maximum number of versions to keep;
*  type of compression;
*  the settings for a bloom filter, if any, to apply;
*  column families stored in this locality group.

Each column family has:

*  a primary name, unique within the table, a description and optionally some name aliases;
*  for map-type families, the Avro schema of the cell values;
*  for group-type families, the collection of columns in the group.

Each column in a group-type family has:

*  a primary name, unique within the family, a description and optionally some name aliases;
*  an Avro schema (or list of compatible Avro schemas) for use when reading and writing data.

Instance names must match the following regular expression: `[a-zA-z0-9_]+`
All other names must match the expression: `[a-zA-Z_][a-zA-Z0-9_]*`
Alises must match: `[a-zA-Z0-9_0]+`

### Row keys

Row keys consist of a tuple of named and typed fields. This may be a single field
like `userid`, or it could be a compound key like `(category_id, product_id)`.
Fields in row keys can have one of three types: `STRING`, `INT`, and `LONG`.

Compound row keys can be used to establish hierarchy (e.g., categories and products).
By default, when written to HBase, each row key is preceeded by a few bytes of salt,
calculated by hashing the first row key component and taking 1--2 bytes. You can
configure both the length of the salt as well as which components of the row key
are included in the hash. By default the first component alone is used, although
you can select any number of left-adjusted fields (e.g., in a 3-part key you could hash the first
two of three components, or all three components). Components that are not included
in the hash are stored adjacent to one another. So for example, if you hash
only the `category_id` field in a `(category_id, product_id)` table, all products
in the same category will be stored adjacent to one another in the table, sorted
by `product_id`.

#### Using `null` in row keys

The first component of a key cannot be null. Subsequent fields can be declared
nullable, or non-nullable. If one field in the key is null, all fields to its right
must be null as well. So `("a", "b", "c")` is a legal row key, as is `("a", null, null)`.
The row key `("a", null, "c")` would be illegal.

The <a href="../schema-shell-ddl-ref/#create_table_syntax"><tt>CREATE TABLE</tt>
syntax</a> section of the DDL shell reference has more detail on the possible row
key formats and how to specify them.

### Group-type and map-type families

In KijiSchema, there are two kinds of column families:

*  Group-type families define a fixed set of named columns.  In the example
   layout above, there is a single group-type family named `info`, containing
   the columns `info:id`, `info:name`, and `info:email`, each with their own
   Avro schema.  Each row may contain any subset of these three columns, but may
   not contain any additional columns.

*  Map-type families define a family where the cell column qualifiers are not
   explicitly defined.  In the example layout above, the map-type family named
   `searches` may be used to store every search performed by a user, each with
   their own column qualifier; cell columns would have names of the form
   `searches:<search-terms>`; the contents of each cell might be a compound Avro
   record containing the list of products returned by the query specified in the
   cell's qualifier, as well as a boolean for each indicating whether the user
   actually clicked that search result.  A map-type column specifies a single Avro
   schema for all its column.  Note that the column qualifiers used in a map-type
   family must be valid UTF-8 strings (rather than arbitrary byte arrays).

### Kiji cell schema

Within a table, Kiji cells are stored using binary-encoded Avro. In addition to
the encoded payload, the writer schema must also be stored in the cell. It would
be inefficient to store the entire Avro representation of the schema in each
cell, so Kiji offers three alternatives using the `storage` field:

*  `HASH` - Store the 128-bit MD5 hash of the writer schema in each encoded
   cell. The full schema is stored in the KijiSchemaTable keyed by the hash.
*  `UID` - Generate and store a unique identifier for each writer schema. This
   numerical identifier is stored with a compact variable-length encoding. The
   full schema is stored in the KijiSchemaTable keyed by the identifier.
*  `FINAL` - Don't store the writer schema in each cell. Instead, the writer
   schema is assumed to be the same as what is declared in the table
   layout. This also means that you may never change the schema for the column.

There are four ways to declare the reader schema of Kiji cells, specified using
the `type` field. The first three of these are available in all versions of KijiSchema.
Introduced in the `layout-1.3` table layout format, the fourth schema declaration type
(`AVRO`) is new in KijiSchema 1.3 and provides a composite description
of currently-used reader and writer schemas. Compatibility with these schemas is
respected when evolving the table's layout.

*  `INLINE` - field `value` contains the JSON representation of an Avro schema.
   In the earlier example, all three columns contain a single Avro `"string"`
   field, but a column could also contain an array, record or other complex Avro
   data type. This is deprecated in KijiSchema 1.3 and `layout-1.3`; new columns
   should use the `AVRO` type.
*  `CLASS` - field `value` contains the fully-qualified name of a Java class
   mapped by an Avro data type, like an implementation of `SpecificRecord`.  The
   user must ensure that the class is available on the classpath of any Kiji
   tools accessing the cell.
   This is deprecated in KijiSchema 1.3 and `layout-1.3`; new columns
   should use the `AVRO` type.
*  `COUNTER` - cells are encoded as long integers and support atomic increment
   or decrement operations.
*  `AVRO` - The `value` field is not used; instead, the `default_reader`, `readers`,
   `writers`, and `written` fields track the default reader schema's uid, and
   lists of the uids of acceptable reader and writer schemas, and all previously-used
   writer schemas. Multiple schemas may be valid for use when reading or writing
   data in this column. See the next section on <a href="#schema_evolution">schema evolution</a>
   in table layouts for more information.

The schema of cells in a group-type family is specified by the `column_schema`
field (see columns `info:id`, `info:name`, `info:email` in the example layout).
The schema of cells in a map-type family is specified by the `map_schema` field
(see map-type family `searches` in the example layout).


<a id="schema_evolution"> </a>
### Schema evolution

Starting in version 1.3.0, a powerful concept in KijiSchema is its ability to help you
safely perform _schema evolution_ in a table layout. This means that you can change the
Avro schema associated with a given column, without physically rewriting the underlying
data. This is especially useful for columns containing record-type data. This section
applies to any tables with layout version `layout-1.3` (which is the default used by
the DDL shell when creating tables in Kiji instances installed with KijiSchema 1.3.0).

As different processes interact with data in a table, their preferred Avro reader and
writer schemas can all be associated with a given column. The column will allow any
compatible reader and writer schemas to be "attached". Compatibility with existing
attached schemas is a requirement to attach any subsequent schemas. In this way, multiple
processes can ensure that they can always read and write data in a manner that can be
re-read by everyone else.

We will motivate this section with the following example:

Let's start by creating a very simple table. Using the DDL shell, you could create a table
with a few columns like:

    schema> CREATE TABLE users ROW KEY FORMAT (userid STRING)
         -> PROPERTIES (VALIDATION = STRICT)
         -> WITH LOCALITY GROUP default (
         ->   FAMILY info (
         ->     name "string",
         ->     email "string"
         -> ));

This command introduces a newer property in the table definition, `VALIDATION = STRICT`.
This enables the strict schema evolution validation capabilities described in this
section. This is only available in Kiji instances created by KijiSchema 1.3.0 or higher.
Kiji instances previously created use the non-validating layout upgrade semantics for all
tables. With `STRICT` validation enabled, schema evolution is enforced to be consistent.
Prior versions of KijiSchema allowed incompatible schema migrations by applying a
different table layout. The semantics of your existing tables (or tables in existing Kiji
instances) from versions <= 1.3.0 are unchanged.

Now imagine we wanted to add an `info:location` column that had the following schema:

    record LocationPoint {
      float lat;
      float lon;
      string data;
    }

This `LocationPoint` record can hold checkin data with a user's latitude and longitude.
A Big Data Application built on Kiji may have several decoupled processes that manipulate
the data:

* A deployed mobile application may report a user's coordinates.
* A KijiMR or Express job may analyze data using LocationPoints, for example, to cluster users.

In this example, one component (the mobile app) writes `LocationPoints`. Another component
reads them.

To add this column to the table, you would use the following DDL in the KijiSchema DDL
shell:

    USE JAR INFILE '/path/to/jar/with/locationpointclass.jar';
    ALTER TABLE users ADD COLUMN info:location WITH SCHEMA CLASS LocationPoint;

The `LocationPoint` class was described above in the Avro IDL; we assume you're using
Avro's "SpecificRecord" interface, that you compile your record types into classes,
and use those classes in your programs.

The first line (`USE JAR...`) will add the jar containing `LocationPoint.class` to
the classpath for running certain commands that follow it.

The `ALTER TABLE... ADD COLUMN` command will add a column named `info:location`. It will
look up the `LocationPoint` class to get its Avro schema, using the jar you added to the
classpath first.

Each column tracks the schemas used for a variety of purposes. In the initial configuration,
they'll all be set to the same value:

* A list of active writer schemas: the current LocationPoint schema is the only acceptable writer.
* A list of active reader schemas: the current LocationPoint is the only acceptable reader.
* A default reader schema: if the user doesn't know which of (possible multiple) reader schemas
  they prefer, a default one is set.
* A default reader schema class: if the column has a SpecificRecord-based schema, the class name
  associated with this is also recorded. Applications that have that class on their classpath
  can use it to read the column if they don't have a preference of which reader schema to use.
* A list of written schemas: the complete history of writer schemas in the active list is recorded
  here. Future schema evolutions require that you preserve compatibility with any existing,
  written data.

Using any schema except the (lat, lon) based schema described earlier to read or write
data will cause an error. Users will always be safe using the default reader schema associated
with the column; it guarantees that you will see some compatible view of the underyling
column data.

Note that the `info:name` and `info:email` columns track all this same information; their
valid reader and valid writer schema lists are both initialized to `"string"` in the
`CREATE TABLE` statement.

Suppose in version 2.0 of your mobile app, you gain the ability to track a user's altitude.
You may want to upgrade your checkin data to work in 3-d:

    record LocationPoint {
      float lat;
      float lon;
      float altitude = 0.0; // assume sea-level unless otherwise specified.
      string data;
    }

You now want to deploy the v2.0 of the mobile app, but you must keep in mind that existing
v1.0 mobile app clients exist, which will write the former format. Furthermore, existing
MapReduce processes will be using the old schema. You might not be able to upgrade these
simultaneously (or may not want to upgrade them).

Before you can use the new schema, you must verify that it's compatible with the old one.
KijiSchema enforces the following constraints:

- All active reader schemas must be able to read data written with any active or formerly-active
  writer schema.
- All active writer schemas must be compatible with one another.

After recompiling the jar containing the `LocationPoint` class, you can re-register the
class (with its new internal schema) in the DDL shell:

    USE JAR INFILE '/path/to/jar/with/locationpointclass.jar';
    ALTER TABLE users ADD READER SCHEMA CLASS LocationPoint FOR COLUMN info:location;
    ALTER TABLE users ADD WRITER SCHEMA CLASS LocationPoint FOR COLUMN info:location;

This will re-read the schema from the associated class, and add it as both a valid reader
and valid writer schema. The evolution of adding an optional field (i.e., one with a
default value) is an example of a compatible evolution. Now, applications can read or
write in both the (lat, lon) or (lat, lon, altitude) formats. An altitude-aware reader
accessing an older record will have a value of `0.0` applied to the `altitude` field of
legacy records.

KijiSchema would prevent you from making an incompatible change. For example, you couldn't
add the following as a writer schema:

    record LocationPoint {  // ignore location, just focus on height.
      float altitude = 0.0;
      string data;
    }

KijiSchema would recognize that a reader expecting latitude and longitude fields would not
be able to read these records correctly. This could, however, be used as a reader schema.

Suppose now that we can confirm that no more v1.0 mobile clients exist: all writers have
migrated to (lat, lon, altitude) and we expect no more (lat, lon) records to be written.
You can enforce this migration at the database level by revoking write access for this
older schema. Running the following command will display the writer schemas associated
with the column:

    DESCRIBE users COLUMN info:location SHOW WRITER SCHEMAS;

This will list the available schemas and their numeric ids within KijiSchema. The command
`ALTER TABLE users DROP WRITER SCHEMA ID <n> FOR COLUMN info:location;` would then
flag the associated schema as no longer legal. While the existing records won't be
modified, you cannot write new (lat, lon) records in there yourself. This ensures that
newer applications can't accidentally use an older version of the schema that we want to
discourage.

Notes:

* You can use generic schemas as well as SpecificRecord classes in this manner.
* This can only be used in Kiji instances installed with KijiSchema 1.3.0 or higher. Older
  Kiji instances cannot use schema validation.
* The semantics of existing tables are unchanged; you can upgrade from KijiSchema 1.2.0 to
  1.3.0 without worrying about changing the prior loose semantics, if you depended on
  them. Of course, this means that KijiSchema 1.3.0 still can't warn you about
  incompatibilities in your layout upgrades for these legacy tables -- take care!
* For more information, see the <a
  href="../schema-shell-ddl-ref/#managing_col_schemas">managing column schemas</a> section
  of the KijiSchema DDL shell reference.

### Locality groups

All families within a locality group are stored together in HBase.  It is
usually a good idea to put families that are often read and written together
into the same locality group.

Locality groups control the physical properties of the underlying storage:

*  `in_memory` - when this boolean flag is set, Kiji configures HBase to keep as
   much of this locality group in memory as possible.
*  `ttl_seconds` - cells' time to live : cells older than this number of seconds
   will be automatically discarded.
*  `max_versions` - maximum number of timestamped versions of cells to retain :
   as new versions of a cell are written, older versions are deleted to not
   exceed this limit.
*  `compression` - one of `NONE`, `GZ`, `LZO` or `SNAPPY`.
*  `bloom_type` - one of `NONE`, `ROW`, or `ROWCOL` to specify the granularity of
   a bloom filter, if any, to maintain when writing and searching for data in HFiles.

### Names

Locality groups, families, and columns are identified by their primary names.

### Updating layouts

Table layouts may be updated by specifying a table layout update descriptor. A table
layout update descriptor entirely specifies the new layout, and sets the `reference_layout`
field to specify the original layout to update. Locality groups, families, and columns can be
updated by redefining the locality group, family, or column with the same name.
To rename locality groups, families, or columns, set the `renamed_from` field
in the new definition to the original name.  Table names may not be changed.

For example, to rename the `default` locality group into `new_name`, one may
update the table layout with a locality group descriptor as follows:

{% highlight js %}
locality_groups: [ {
  name: "new_name",
  renamed_from: "default",
  ...
} ]
{% endhighlight %}

### Layout record descriptor
<a name="ref.table_layout_desc" id="ref.table_layout_desc"> </a>

For reference, the Avro descriptor for table layout records is defined in
[`src/main/avro/Layout.avdl`](https://github.com/kijiproject/kiji-schema/blob/kiji-schema-root-1.5.0/kiji-schema/src/main/avro/Layout.avdl "Layout.avdl")
within the kiji-schema git project as follows:


<div id="accordion-container">
  <h2 class="accordion-header"> Layout.avdl </h2>
  <div class="accordion-content">
    <script
    src="http://gist-it.appspot.com/github/kijiproject/kiji-schema/raw/kiji-schema-root-1.5.0/kiji-schema/src/main/avro/Layout.avdl"> </script>
  </div>
</div>

<!--
*  TODO: This section requires additional work.
**  TODO: Advice on how to design your schema (maybe talk about "crazy columns" here).
**  TODO: Somewhere in here talk about schema evolution.
**  TODO: Remove the references to Avro serialization with a more generic Hadoop version
-->
