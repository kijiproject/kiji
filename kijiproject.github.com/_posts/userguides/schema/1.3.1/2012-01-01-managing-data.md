---
layout: post
title: Managing Data
categories: [userguides, schema, 1.3.1]
tags: [schema-ug]
version: 1.3.1
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
* Using the KijiSchema DDL shell
* Editing the JSON layout description directly.

Most users should use the KijiSchema DDL shell. It has a user-friendly language
for creating, modifying, and describing tables. To learn more about this
mechanism, skip ahead to the [DDL Shell reference](../schema-shell-ddl-ref).

Viewing and editing the JSON layout description is a low-level task, typically
performed by system administrators, or for debugging purposes only. Its format
is described below.

A JSON layout descriptor is a specification for the locality groups,
columns, and data types that comprise a table, written as a JSON
document whose elements are described in the following subsections.  We will
refer to the following example layout file throughout this section:

{% highlight js %}
{
  name: "users",
  description: "A bunch of made-up users",
  version: "layout-1.1",
  keys_format: {
    encoding : "FORMATTED",
    salt : {
      hash_size : 2
    },
    components : [ {
      name : "uid",
      type : "LONG"
    }
   ]
  },
  locality_groups: [ {
    name: "default",
    description: "The default locality group",
    in_memory: false,
    max_versions: 1,
    ttl_seconds: 2147483647,
    compression_type: "NONE",
    families: [ {
      name: "info",
      description: "A bunch of fields",
      columns: [ {
        name: "id",
        description: "user id hash",
        column_schema: {type: "INLINE", value: '"string"'}
      }, {
        name: "name",
        description: "The person's name",
        column_schema: {type: "INLINE", value: '"string"'}
      }, {
        name: "email",
        description: "The person's email",
        column_schema: {type: "INLINE", value: '"string"'}
      } ]
    }, {
      name: "searches",
      description: "The recent search queries the user has made",
      map_schema: {type: "CLASS", value: "com.search.avro.Search"}
    } ]
  } ]
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
*  column families stored in this locality group.

Each column family has:

*  a primary name, unique within the table, a description and optionally some name aliases;
*  for map-type families, the Avro schema of the cell values;
*  for group-type families, the collection of columns in the group.

Each column in a group-type family has:

*  a primary name, unique within the family, a description and optionally some name aliases;
*  an Avro schema.

Instance names must match the following regular expression: `[a-zA-z0-9_]+`
All other names must match the expression: `[a-zA-Z_][a-zA-Z0-9_]*`
Alises must match: `[a-zA-Z0-9_0]+`

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

There are three ways to declare the reader schema of Kiji cells, specified using
the `type` field:

*  `INLINE` - field `value` contains the JSON representation of an Avro schema.
   In the earlier example, all three columns contain a single Avro `"string"`
   field, but a column could also contain an array, record or other complex Avro
   data type.
*  `CLASS` - field `value` contains the fully-qualified name of a Java class
   mapped by an Avro data type, like an implementation of `SpecificRecord`.  The
   user must ensure that the class is available on the classpath of any Kiji
   tools accessing the cell.
*  `COUNTER` - cells are encoded as long integers and support atomic increment
   or decrement operations.

The schema of cells in a group-type family is specified by the `column_schema`
field (see columns `info:id`, `info:name`, `info:email` in the example layout).
The schema of cells in a map-type family is specified by the `map_schema` field
(see map-type family `searches` in the example layout).

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
[`src/main/avro/Layout.avdl`](https://github.com/kijiproject/kiji-schema/blob/kiji-schema-root-1.3.1/kiji-schema/src/main/avro/Layout.avdl "Layout.avdl")
within the kiji-schema git project as follows:


<div id="accordion-container">
  <h2 class="accordion-header"> Layout.avdl </h2>
  <div class="accordion-content">
    <script
    src="http://gist-it.appspot.com/github/kijiproject/kiji-schema/raw/kiji-schema-root-1.3.1/kiji-schema/src/main/avro/Layout.avdl"> </script>
  </div>
</div>

<!--
*  TODO: This section requires additional work.
**  TODO: Advice on how to design your schema (maybe talk about "crazy columns" here).
**  TODO: Somewhere in here talk about schema evolution.
**  TODO: Remove the references to Avro serialization with a more generic Hadoop version
-->
