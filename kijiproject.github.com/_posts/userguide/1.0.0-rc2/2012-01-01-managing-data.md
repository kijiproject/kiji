---
layout: post
title: Managing Data
categories: [userguide, schema, 1.0.0-rc2]
tags: [schema-ug]
version: 1.0.0-rc2
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

A JSON layout descriptor is a specification for the locality groups,
columns, and data types that comprise a table, written as a JSON
document whose elements are described in the following subsections.  We will
refer to the following example layout file throughout this section:

{% highlight js %}
{
  name: "users",
  description: "A bunch of made-up users",
  version: "kiji-1.0",
  keys_format: {encoding: "HASH"},
  locality_groups: [ {
    name: "default",
    description: "The default locality group",
    in_memory: false,
    max_versions: 1,
    ttl_seconds: 7776000,
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

All names must start with a letter and may only use letters, numbers, and underscores.

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

Within a table, Kiji cells are encoded according to their declared schema.
There are three types of Kiji cell schemas, specified using the `type` field:

*  `INLINE` - field `value` contains an the JSON representation of an Avro
   schema.  In the earlier example, all three columns contain a single Avro
   `"string"` field, but a column could also contain an array, record or other
   complex Avro data type.
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
*  `ttl_seconds` - cells' time to live : cells older that this number of seconds
   may be automatically discarded.
*  `max_versions` - maximum number of timestamped versions of cells to retain :
   as new versions of a cell are written, older versions are deleted to not
   exceed this limit.
*  `compression` - one of `NONE`, `GZ`, `LZO` or `SNAPPY`.

### Names

Locality groups, families, and columns are identified by their primary names.
The primary name may be changed with the help of the `renamed_from` field.
For example, to rename the `default` locality group into `new_name`, one may
update the table layout with a locality group descriptor as follows:

{% highlight js %}
locality_groups: [ {
  name: "new_name",
  renamed_from: "default",
  ...
} ]
{% endhighlight %}

Table names may not be changed.


### Layout record descriptor <a id="ref.table_layout_desc"> </a>

For reference, the Avro descriptor for table layout records is defined in
`src/main/avro/Layout.avdl` within the kiji-schema git project as follows:

{% highlight java %}
/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Kiji table layout.

@namespace("org.kiji.schema.avro")
protocol KijiTableLayoutRecords {

  /** Type of compression for the data within a locality group. */
  enum CompressionType {
    NONE,
    GZ,
    LZO,
    SNAPPY
  }

  /** Type of schema for column data. */
  enum SchemaType {
    /** Column contains data encoded as specified inline. */
    INLINE,

    /** Column contains Avro records with the specified class name. */
    CLASS,

    /** Column contains counters. */
    COUNTER
  }

  /** How schemas get encoded in cells. */
  enum SchemaStorage {
    /** Data is prepended with the schema hash. */
    HASH,

    /** Data is prepended with the schema unique ID. */
    UID,

    /** Schema is immutable and not stored in the cell. */
    FINAL
  }

  /** Schema of a Kiji cell. */
  record CellSchema {
    /** Schema encoding in cells. Unused if type is COUNTER. */
    SchemaStorage storage = "HASH";

    /** Type of schema. */
    SchemaType type;

    /**
     * Schema value, whose interpretation depends on the schema type:
     *  - inline : immediate Avro schema description, eg. "string";
     *  - class : Avro schema class name, eg. "org.kiji.avro.Node";
     *  - counter : unused, must the empty.
     */
    union { null, string } value = null;
  }

  /** Column descriptor. */
  record ColumnDesc {
    /** Column ID. Not visible to the user. 0 means unset. */
    int @internal("true") id = 0;

    /** Column primary name (A-Z, a-z, 0-9, -, _). */
    string name;

    /** Column name aliases. */
    array<string> aliases = [];

    /** When false, the column is not visible or usable. */
    boolean enabled = true;

    /** User description of the column. */
    string description = "";

    /** Schema for the cell values. */
    CellSchema column_schema;

    // Fields below are used to apply a diff against a reference family layout:

    /** When true, applying this layout deletes the column. */
    boolean @diff("true") delete = false;

    /** Reference primary name of the column, when renaming the column. */
    union { null, string } @diff("true") renamed_from = null;
  }

  /** Descriptor for a group of columns. */
  record FamilyDesc {
    /** Family ID. Not visible to the user. 0 means unset. */
    int @internal("true") id = 0;

    /** Column family primary name (A-Z, a-z, 0-9, -, _). */
    string name;

    /** Column family name aliases. */
    array<string> aliases = [];

    /** When false, the family and its columns are not visible/usable. */
    boolean enabled = true;

    /** User description of the column family. */
    string description = "";

    /** Cell schema for map-type families. Null for group-type families. */
    union { null, CellSchema } map_schema = null;

    /** Columns, for group-type families only. Empty for map-type families. */
    array<ColumnDesc> columns = [];

    // Fields below are used to apply a diff against a reference family layout:

    /** When true, applying this layout deletes the family. */
    boolean @diff("true") delete = false;

    /** Reference primary name of the family, when renaming the family. */
    union { null, string } @diff("true") renamed_from = null;
  }

  /** A group of Kiji column families stored together in a table. */
  record LocalityGroupDesc {
    /** Locality group ID. Not visible to the user. 0 means unset. */
    int @internal("true") id = 0;

    /** Locality group primary name (A-Z, a-z, 0-9, -, _). */
    string name;

    /** Locality group name aliases. */
    array<string> aliases = [];

    /** When false, the locality group and its families are not visible. */
    boolean enabled = true;

    /** User description of the locality group. */
    string description = "";

    /** Reduce latency by forcing all data to be kept in memory. */
    boolean in_memory;

    /** Maximum number of the most recent cell versions to retain. */
    int max_versions;

    /** Length of time in seconds to retain cells. */
    int ttl_seconds;

    /** Data compression type. */
    CompressionType compression_type;

    /** Column family descriptors. */
    array<FamilyDesc> families = [];

    // Fields below are used against a reference locality group layout:

    /** When true, applying this layout deletes the locality group. */
    boolean @diff("true") delete = false;

    /** Reference primary name of the locality group, when renaming. */
    union { null, string } @diff("true") renamed_from = null;
  }


  /** Hashing methods. */
  enum HashType {
    /** MD5 hashing (16 bytes). */
    MD5
  }

  /** Row keys encoding. */
  enum RowKeyEncoding {
    /** Row keys are managed by the user. */
    RAW,

    /** Row keys are hashed. */
    HASH,

    /** Row keys are prefixed by a hash. */
    HASH_PREFIX
  }

  record RowKeyFormat {
    /** Encoding of the row key. */
    RowKeyEncoding encoding;

    /** Type of hashing used, if any. */
    union { null, HashType } hash_type = null;

    /**
     * Size of the hash, in bytes.
     *  - unused when encoding is RAW.
     *  - smaller than the hash size used for HASH or HASH_PREFIX.
     */
    int hash_size = 0;
  }

  /** Layout of a Kiji table. */
  record TableLayoutDesc {
    /** Name of the table (A-Z, a-z, 0-9, -, _). */
    string name;

    /** User description of the table. */
    string description = "";

    /** Whether row key hashing should be managed by Kiji. */
    RowKeyFormat keys_format;

    /** Locality groups in the table. */
    array<LocalityGroupDesc> locality_groups = [];

    /** Data layout version (eg. "kiji-1.0"). */
    string version;

    /** ID of the layout. */
    union { null, string } layout_id = null;

    /** Reference to the base layout this descriptor builds on. */
    union { null, string } reference_layout = null;
  }

  // ---------------------------------------------------------------------------
  // Backup records

  /** An MD5 hash. */
  fixed MD5Hash(16);

  /** An entry from the SchemaTable inside a metadata backup file. */
  record SchemaTableEntry {
    /** Schema ID: positive integers only. */
    long id;

    /** 128 bits (16 bytes) hash of the schema JSON representation. */
    MD5Hash hash;

    /** JSON representation of the schema. */
    string avro_schema;
  }

  /** Entry to backup a table layout update. */
  record TableLayoutBackupEntry {
    /** Update timestamp, in ms. */
    long timestamp;

    /**
     * Table layout update, as specified by the submitter/user.
     * Except for the first one, the update builds on the previous layout.
     */
    union { TableLayoutDesc, null } update = null;

    /** Effective table layout, after applying the update. */
    TableLayoutDesc layout;
  }

  /** Table backup, ie. everything needed to restore a table. */
  record TableBackup {
    /** Table name. */
    string name;

    /** Sequence of layouts, in order. */
    array<TableLayoutBackupEntry> layouts = [];
  }

  /** Record that encapsulates all Kiji metadata, for backup purposes. */
  record MetadataBackup {
    /** Layout version (eg. "kiji-1.0"). */
    string layout_version;

    /** Schema entries. */
    array<SchemaTableEntry> schema_table = [];

    /** Map from table names to table backup records. */
    map<TableBackup> meta_table = {};
  }

}
{% endhighlight %}

<!--
*  TODO: This section requires additional work.
**  TODO: Advice on how to design your schema (maybe talk about "crazy columns" here).
**  TODO: Somewhere in here talk about schema evolution.
**  TODO: Remove the references to Avro serialization with a more generic Hadoop version
-->
