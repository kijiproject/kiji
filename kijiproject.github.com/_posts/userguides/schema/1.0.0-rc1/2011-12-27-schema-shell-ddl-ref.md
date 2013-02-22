---
layout: post
title: DDL Shell Reference
id: userguide/ddl-ref
categories: [userguides, schema, 1.0.0-rc1]
component: schema
version: 1.0.0-rc1
order : 6
tags: [schema-ug]
description: An introduction to the DDL that manipulates table layouts.
---

The KijiSchema DDL Shell implements a _data definition language_ (DDL) that
allows you to create, modify, inspect, and delete kiji table layouts. A
_table layout_ is the KijiSchema data dictionary element that defines a set
of column families, qualifiers, etc. and associates these columns with
Avro schemas.

With this shell, you can quickly create tables to use in your KijiSchema
applications. You can add, drop, or modify columns quickly too. These
modifications can be performed without any downtime associated with the
table; the only exception is that modifying a locality group causes
the table to be taken offline while applying changes. But locality group
changes should be rare in running applications.

Finally, this shell allows you to quickly understand the layout of an
existing table. The `DESCRIBE` command will pretty-print the layout of a
table, eliminating the guesswork as to where your coworker has stashed an
important piece of data.


## Running

* Export `$KIJI_HOME` to point to your KijiSchema installation.
* Run `bin/kiji-schema-shell`

This command takes a few options (e.g., to load a script out of a file).
See `bin/kiji-schema-shell --help` for all the available options.

### An Example

The following commands could be entered at the DDL shell prompt to create
a table, add another column family, describe its layout, and then remove the
table.

    CREATE TABLE foo WITH DESCRIPTION 'some data'
    ROW KEY FORMAT HASHED
    WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
      MAXVERSIONS = INFINITY,
      TTL = FOREVER,
      INMEMORY = false,
      COMPRESSED WITH GZIP,
      FAMILY info WITH DESCRIPTION 'basic information' (
        name "string" WITH DESCRIPTION 'The user\'s name',
        email "string",
        age "int"),
      MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
    );

    ALTER TABLE foo ADD MAP TYPE FAMILY strings { "type" : "string" }
      WITH DESCRIPTION 'ad-hoc string data' TO LOCALITY GROUP default;

    SHOW TABLES;
    DESCRIBE EXTENDED foo;
    DROP TABLE foo;
    SHOW TABLES;


## The Layout Definition Language

This section defines the layout definition language.

### General Language Notes and Style

All statements in the language must end with a semi-colon character.
Statements may wrap across multiple lines.

Keywords are case insensitive. The following are equivalent:

    schema> DESCRIBE foo;
    schema> describe foo;

Names for tables and columns are case sensitive. The following are _different_:

    schema> DESCRIBE FOO;
    schema> DESCRIBE foo;

Lines beginning with '#' are treated as comments and ignored.

    schema> # this is a comment line and will be ignored.
    schema> # the next line will not be ignored.
    schema> show tables;


Table and column names can be provided either as identifiers or
`'in single quotes'`. The following are equivalent:

    schema> DESCRIBE foo;
    schema> DESCRIBE 'foo';

If you have a table whose name is a keyword (e.g., "SHOW" or "tables"),
you can wrap them `'in single quotes'` to protect them. e.g.:

    schema> CREATE TABLE 'WITH' WITH DESCRIPTION 'A table with a silly name';

By contrast, strings used to specify table or column descriptions, or other
string "values" must always be single quoted.

Quoted strings may contain `'internally \'escaped quotes\', as well as escaped slashes:\\'`.

The escape sequences `'\t'` and `'\n'` work as you'd hope, representing a tab
character and a newline character respectively. Everything else escapes to itself.

`INFINITY` and `FOREVER` are both synonyms for Int.MaxValue wherever an integer
may be specified.

Avro schemas are specified with the Avro JSON schema syntax. JSON uses "double quotes"
to enclose strings; this is respected in this shell as well. JSON can be entered directly
into the command. For example:

    ALTER TABLE foo ADD MAP TYPE FAMILY strings { "type" : "string" }
      WITH DESCRIPTION 'ad-hoc string data' TO LOCALITY GROUP default;


For more information, see
[The Avro 1.7 specification](http://avro.apache.org/docs/1.7.2/spec.html).

## Layout Definition Language Statements

### Working with Kiji Instances

A kiji instance is a collection of kiji tables. You can list the available
instances:

    schema> SHOW INSTANCES;

The currently selected instance will have an asterisk next to it.

You can also select an instance:

    schema> USE foo;

The default instance is named `default`; in addition to `USE default;`, 
you can also select that with the more readable (and more verbose) syntax:

    schema> USE DEFAULT INSTANCE;


### Listing and Describing Tables

You can list the available tables with:

    schema> SHOW TABLES;

You can pretty-print the layout of a table with:

    schema> DESCRIBE foo;

This will not print locality group information or hidden columns. To include
this information, use:

    schema> DESCRIBE EXTENDED foo;


### Creating Tables

This shell will allow you to create tables and define their locality groups,
column families, and column qualifiers. Each map-type family or qualifier in a
group-type family has a schema associated with it.

#### Schemas

There are three possible kinds of schemas that can be associated with a
column:

* JSON schemas are declared inline. e.g. `"string"` or `{ "type" : "int" }`
* Avro SpecificRecord class names are declared as `CLASS com.example.FooRecord`
* A special type `COUNTER` can define an HBase counter column that can be
  atomically incremented.

#### Names

Table, locality group, family, and column names must have the following
format:

    [a-zA-Z_][a-zA-Z0-9_]*

The following are legal names:

    foo
    meep123
    wom_bat
    _aard_vark
    users2

The following are not allowed:

    42
    2users
    some-dashed-name
    this$uses!special(characters_)

#### `CREATE TABLE` Syntax

    CREATE TABLE t [WITH DESCRIPTION 'd']
    [ROW KEY FORMAT { HASHED | RAW | HASH PREFIXED(int) }]
    WITH locality_group [ , locality_group ...]*;

This creates a table, sets its description, defines how row keys are formatted,
and defines a list of locality groups.

Row keys may be literal string values (`ROW KEY FORMAT RAW`), or they may be
the hash of the string value (`ROW KEY FORMAT HASHED`). They may also be a
composite of the two (`ROW KEY FORMAT HASH PREFIXED(n)`), where `n` is an
integer between 1 and 16 specifying the number of hash bytes to retain as
the prefix size (e.g: `ROW KEY FORMAT HASH PREFIXED(2)` will use raw keys
with two bytes of the key hash as a prefix.) If no ROW KEY FORMAT clause is
given, it is assumed to be `HASHED`.

Each locality group is defined as follows:

    LOCALITY GROUP foo [WITH DESCRIPTION 'd'] [ ( property, property... ) ]

This defines a locality group named `foo` with one or more properties set. The
valid properties are:

    MAXVERSIONS = int
    INMEMORY = bool
    TTL = int
    COMPRESSED WITH (GZIP | LZO | SNAPPY | NONE)

Remember that you can use `INFINITY` and `FOREVER` when specifying integer
values for `MAXVERSIONS` and `TTL` to make your statements more readable.

You can also specify map- and group-type column families in the properties
list:

    [GROUP TYPE] FAMILY f [WITH DESCRIPTION 'd'] [( column, column... )]
    MAP TYPE FAMILY f [WITH SCHEMA] schema [WITH DESCRIPTION 'd']


Within a group-type family, individual column qualifiers are specified with:

    [COLUMN] foo [WITH SCHEMA] schema [WITH DESCRIPTION 'd']


#### `CREATE TABLE Example`

The following is an example that puts together all of the different syntax
elements described in the previous section:

    schema> CREATE TABLE foo WITH DESCRIPTION 'some data'
         -> ROW KEY FORMAT HASHED
         -> WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
         ->     MAXVERSIONS = INFINITY,
         ->     TTL = FOREVER,
         ->     INMEMORY = false,
         ->     COMPRESSED WITH GZIP,
         ->     FAMILY info WITH DESCRIPTION 'basic information' (
         ->         name "string" WITH DESCRIPTION 'The user\'s name',
         ->         email "string",
         ->         addr CLASS com.example.AddressRecord,
         ->         age "int"),
         ->     MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
         -> );

### Deleting Tables

You can delete a table with the command:

    schema> DROP TABLE foo;

Warning: THIS WILL DELETE ALL THE DATA IN THE TABLE.

### Changing Table Properties

All table properties that can be set (descriptions, columns & schemas,
locality group properties, etc) when a table is created, can also be changed
after the fact.

Keywords or clauses in \[square brackets\] are optional in the examples below:


    ALTER TABLE t ADD COLUMN info:foo [WITH SCHEMA] schema [WITH DESCRIPTION 'd'];
    ALTER TABLE t RENAME COLUMN info:foo [AS] info:bar;
    ALTER TABLE t DROP COLUMN info:foo;

    ALTER TABLE t ADD [GROUP TYPE] FAMILY f [WITH DESCRIPTION 'd'] TO [LOCALITY GROUP] lg;

    ALTER TABLE t ADD MAP TYPE FAMILY f [WITH SCHEMA] schema
        [WITH DESCRIPTION 'd'] TO [LOCALITY GROUP] lg;

    ALTER TABLE t DROP FAMILY f;
    ALTER TABLE t RENAME FAMILY f [AS] f2;

    ALTER TABLE t CREATE LOCALITY GROUP lg [WITH DESCRIPTION 'd']
        [( property, property... )]

    ... Where 'property' is one of:
          MAXVERSIONS = int
        | INMEMORY = bool
        | TTL = int
        | COMPRESSED WITH { GZIP | LZO | SNAPPY | NONE }
    ... or a FAMILY definition (see the earlier section on the CREATE TABLE syntax)

    ALTER TABLE t RENAME LOCALITY GROUP lg [AS] lg2;
    ALTER TABLE t DROP LOCALITY GROUP lg;

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR FAMILY f;
    ALTER TABLE t SET SCHEMA = schema FOR [MAP TYPE] FAMILY f;

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR COLUMN info:foo;
    ALTER TABLE t SET SCHEMA = schema FOR COLUMN info:foo;

    ALTER TABLE t SET DESCRIPTION = 'desc';

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR LOCALITY GROUP lg;
    ALTER TABLE t SET property FOR LOCALITY GROUP lg;
    ... where 'property' is one of MAXVERSIONS, INMEMORY, TTL, or COMPRESSED WITH.


### Working With Scripts

You can write scripts that contain DDL statements to define tables or whole
instances. If you put a sequence of DDL statements in a file, you can run
this file with:

    schema> LOAD FROM FILE '/path/to/foo.ddl';

You can print the DDL that would create a whole instance or a specific table
to the screen:

    schema> DUMP DDL;
    schema> DUMP DDL FOR TABLE foo;

Or you can save the output to a file:

    schema> DUMP DDL TO FILE '/path/to/foo.ddl';
    schema> DUMP DDL FOR TABLE foo TO FILE '/path/to/foo.ddl';

