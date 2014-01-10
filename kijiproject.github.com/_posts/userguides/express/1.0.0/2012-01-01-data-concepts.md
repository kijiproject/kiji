---
layout: post
title: KijiExpress Data Concepts
categories: [userguides, express, 1.0.0]
tags : [express-ug]
version: 1.0.0
order : 3
description: KijiExpress Data Concepts.
---

* [Kiji Tables](#kiji_table_layout_and_cells)
* [Tuples and Pipelines](#tuples_and_pipelines)
* [Avro Data](#avro_and_representing_complex_data)

## Kiji Table Layout and Cells

KijiExpress relies on KijiSchema’s data model, which in itself is an extension of HBase’s
columnar storage model.

As you would expect, Kiji tables contain many rows; each row contains many columns. Every
row is identified by an entity ID.

Kiji table columns are logically organized into column families, which allow you to associate
related data together. Columns are identified by their column family and name, which means
that you can repeat column names in a table, just so the duplicate names are not in the
same family. Columns are physically organized by locality groups, which help store related
families physically close to one another in memory or on disc.

![Table Layout][column_family_locality]

[column_family_locality]: ../../../../assets/images/column_family_locality.png

In addition, a given cell identified by a row and column can hold many different
timestamped values, representing the changing values of the cell over time.

![Table Layout with Versions][column_family_locality_with_versions]

[column_family_locality_with_versions]: ../../../../assets/images/column_family_locality_with_versions.png

A particular cell is identified by four coordinates: entity ID, column family, column
qualifier, and timestamp.

>What Kiji calls a “locality group”, HBase calls a “family”. The Kiji column “family”
>allows you to choose the logical grouping and namespace of columns separately from the
>physical configuration of how the data is stored. In Kiji, multiple families can belong
>to one locality group.

Here’s an example of a DDL that specifies the structure of a Kiji table:

    CREATE TABLE users WITH DESCRIPTION 'A set of users'
    ROW KEY FORMAT (username STRING, userId STRING),
    WITH LOCALITY GROUP default
      WITH DESCRIPTION 'Main locality group' (
      MAXVERSIONS = INFINITY,
      TTL = FOREVER,
      INMEMORY = false,
      COMPRESSED WITH NONE,
      FAMILY info WITH DESCRIPTION 'Information about a user' (
        track_plays "string" WITH DESCRIPTION 'Tracks played by the user',
        next_song_rec "string" WITH DESCRIPTION 'Next song recommendation based on play history for a user'
      )
    );

As in HBase, rows in Kiji tables can have an arbitrary number of columns. Individual rows
may have hundreds or thousands (or more) columns. Different rows may not necessarily have
the same set of columns.

For more information about columns, column families, locality groups, and table layouts,
see [Managing Data]({{site.userguide_schema_1_3_4}}/managing-data) in the KijiSchema User Guide. For
details on how to turn your layout design into KijiSchema DDL Shell statements, see [DDL Shell
Reference]({{site.userguide_schema_1_3_4}}/schema-shell-ddl-ref).

## Tuples and Pipelines

KijiExpress views a data set as a collection of named tuples. A named tuple can be thought
of as an ordered list where each element has a name. When using KijiExpress with data
stored in a Kiji table, a row from the Kiji table corresponds to a single tuple, where
columns from the Kiji table correspond to named elements in the tuple.

Data processing occurs in pipelines, where the input and output from each pipeline is a
stream of named tuples represented by a data object. Each operation you described in your
KijiExpress program, or job, defines the input, output, and processing for a pipe.

Data enters a pipe from a source. Sources can be such places as Kiji tables, text files,
Sequence files ?what’s a sequence file?. At the end of the pipe, tuples are written
to a sink. Again, sinks can be Kiji tables, text files, or SequenceFiles.

## Avro and Representing Complex Data

Each cell in a Kiji table has a schema associated with it. Schemas in KijiSchema are versioned.

Avro serializes data from a byte stream into a record that has the structure that you define.
Systems use Avro when exchanging data because it provides a way to identify the structure of
the data that all clients of the data can understand.

Avro data can be one of the Avro primitive types, which include strings, numbers, booleans, and
arrays.  You can also define your own complex avro records that are composed of fields of the
primitive types.  For example, you could design an Avro record to include customer address
information, with fields for the customer name, street address, state, and zip code.

Data written to a Kiji cell is serialized to a byte array according to an Avro schema.  The writer
schema used for the particular write operation is stored alongside the cell data, so the input data
can be deserialized exactly with the same schema by subsequent read requests. This schema must be
compatible with the expected reader schema specified in the layout for the cell.  An application
reading from a Kiji table can also specify a reader schema to use, which must be compatible with the
writer schema of the data.  For more on schema compatbility and evolution, see [this blog
post](http://www.kiji.org/2013/10/15/introduction-to-schema-evolution-a-tale-of-two-dbs/).
