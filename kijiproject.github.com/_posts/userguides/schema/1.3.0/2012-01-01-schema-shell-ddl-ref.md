---
layout: post
title: DDL Shell Reference
id: userguide/ddl-ref
categories: [userguides, schema, 1.3.0]
component: schema
version: 1.3.0
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
Run `bin/kiji-schema-shell --help` for all the available options.

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

You can create an instance by running:

    schema> CREATE INSTANCE foo;

Creating an instance will automatically `USE` the new instance.

You can also drop an instance by running:

    schema> DROP INSTANCE foo;

This will drop all tables in the instance! You cannot drop the current instance.
You must `USE` another instance first (or maybe create one with `CREATE INSTANCE`
to move to).

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
    [ROW KEY FORMAT { HASHED | RAW | HASH PREFIXED(int) | formatted_key_spec }]
    [PROPERTIES ( table_property, ... )]
    WITH locality_group [ , locality_group ...]*;

This creates a table, sets its description, defines how row keys are formatted,
and defines a list of locality groups.

##### Row key syntax

Row keys are generally a tuple of one or more string or numeric values, which are usually
prefixed by a few bytes of salt for load balancing purposes. The salt is calculated
by hashing one or more elements of the tuple and using a few bytes of the hash.

A `formatted_key_spec` looks something like this:

    ROW KEY FORMAT (component_name STRING, component_name2 INT, ..., HASH (hash_props))

The row key must have at least one component. Each component in the key tuple must have a name
that matches `[A-Za-z_][A-Za-z0-9_]*`. Components may have one of the following types:

* `STRING` - A UTF-8-encoded string that does not contain the codepoint `\u0000` ('nil').
* `INT` - A 32-bit signed integer.
* `LONG` - A 64-bit signed integer.
* Additionally, components may be marked `NOT NULL` (e.g., `foo STRING NOT NULL`). The
  first element is implicitly `NOT NULL`. Subsequent elements may be marked as such,
  but `NOT NULL` elements may not follow nullable ones.

And `hash_props` is a collection of the following properties:

* `THROUGH component_name` - Specifies that the hash prefix is calculated through all
  fields up to and including `component_name`. By default, the hash prefix will be
  calculated based on the first field.
* `SIZE = n` - Specifies the number of bytes to use in the hash prefix, between 0 and 16 inclusive.
  If not given, this defaults to 2 bytes.
* `SUPPRESS FIELDS` - If specified, then the "plain text" of your key is discarded; only the
  hash of the key is used. If `SIZE` is not specified but `SUPPRESS FIELDS` is, the entire 16
  byte hash will be retained. Do not use this if you need to read the keys out of the table;
  only use this if recording the hashes alone is sufficient.
* Each of the above 3 properties may be specified at most once.

There are also a few "canned" row key formats available:

* `ROW KEY FORMAT HASH PREFIXED(n)` - Use a single string component in the key, and
  include a hash-based salt prefix. `n` specifies the number of bytes of salt. This should
  be at least 2 bytes. More than 4 is probably overkill.
* `ROW KEY FORMAT HASHED` - Use a single string component in the key, but rather than
  include the string component in the key -- just use the 16 byte hash of the string.
  If you need to read back the value in the key, prefer `HASH PREFIXED` over `HASHED`.
* `ROW KEY FORMAT RAW` - Use a literal raw byte array for the key. Appropriate for advanced
  manual control over row keys; use at your own caution.
* If no `ROW KEY FORMAT` clause is given, it is assumed to be `HASHED`.

Example row keys:

    # A simple row key consisting of a single, hash-prefixed component:
    CREATE TABLE ex0 ROW KEY FORMAT (username STRING) WITH LOCALITY GROUP...

    # A composite row key:
    CREATE TABLE ex1 ROW KEY FORMAT (lastname STRING, firstname STRING) WITH LOCALITY GROUP...

    # Key components are implicitly strings; this is equivalent:
    CREATE TABLE ex1a ROW KEY FORMAT (lastname, firstname) WITH LOCALITY GROUP...

    # The hash prefix is calculated based on (state, zip), not just 'state'.
    # Note that 'state' is implicitly NOT NULL since it's the first element:
    CREATE TABLE ex2 ROW KEY FORMAT (state STRING, zip INT NOT NULL, HASH(THROUGH zip)) ...

    # Equivalent to "ROW KEY FORMAT HASHED":
    CREATE TABLE ex3 ROW KEY FORMAT (key STRING, HASH(SUPPRESS FIELDS)) ...

    # Include a bigger hash (4 bytes) than the default (2 bytes):
    CREATE TABLE ex4 ROW KEY FORMAT (category_id LONG, product_id LONG, HASH(SIZE=4)) ...

    # Setting many options:
    CREATE TABLE ex5 ROW KEY FORMAT (a STRING, b LONG NOT NULL, c INT, HASH(THROUGH b, SIZE=8)) ...

    # Row key component names can also be 'single quoted', like other identifiers:
    CREATE TABLE ex6 ROW KEY FORMAT ('a' STRING) ...

As a final word of caution, please note that after a table is created, its row key format
cannot be altered.

##### Table properties

Some optional properties of tables can be specified:

    MAX FILE SIZE = <n>

Sets the maximum size in bytes for a file. `<n>` may be a long integer or `NULL`, in
which case HBase's default size will be used.

    MEMSTORE FLUSH SIZE = <n>

Sets the size of the HBase memstore for the table in bytes, before it forces a flush.
`<n>` may be a long integer or `NULL`, in which case HBase's default value will be used.

    NUMREGIONS = <n>

Specifies the initial region count when creating the table. The table will be split into `<n>`
evenly-spaced regions. This cannot be used with a `RAW` row key format. This property is
not preserved by a `DUMP DDL` statement.

    VALIDATION = { NONE | LEGACY | DEVELOPER | STRICT }

Specifies the preferred schema validation mode to apply to columns in this table. This
affects what reader and writer schemas are legal to attach to a given column.

* `NONE` - No validation; any combination of reader and writer schemas may be declared. At
  write time, there is no enforcement that the actual writer schema is in the approved
  list of writer schemas.
* `LEGACY` - Performs validation according to the semantics of tables using `layout-1.2`
  and below (as created by KijiSchema 1.2.0 and below). Primitive types are validated
  against the approved writer schema at write time; incompatible changes to the table
  layout are permitted.
* `DEVELOPER` - Performs strong validation of reader and writer schema compatibility when
  changing the layout of a table. Compatible writer schemas may be added on-demand at
  write time by a new process. This is the default.
* `STRICT` - Performs strong validation of reader and writer schema compatibility when
  changing the layout of a table; you may not add any schemas that are incompatible with
  currently-active reader and writer schemas (nor any previously-used "recorded" writer
  schemas). At write time, the writer schema must match one of the schemas explicitly
  added through the Kiji shell.

Schema validation is discussed in greater detail further down in this document in the
section "Managing Column Schemas."

##### Locality group definition syntax

Each locality group is defined as follows:

    LOCALITY GROUP foo [WITH DESCRIPTION 'd'] [ ( property, property... ) ]

This defines a locality group named `foo` with one or more properties set. The
valid properties are:

    MAXVERSIONS = int
    INMEMORY = bool
    TTL = int
    COMPRESSED WITH (GZIP | LZO | SNAPPY | NONE)
    BLOOM FILTER = (NONE | ROW | ROWCOL)
    BLOCK SIZE = int

Remember that you can use `INFINITY` and `FOREVER` when specifying integer
values for `MAXVERSIONS` and `TTL` to make your statements more readable.

You can also specify `BLOCK SIZE = NULL` in which case HBase's default value
will be used.

You can also specify map- and group-type column families in the properties
list:

    [GROUP TYPE] FAMILY f [WITH DESCRIPTION 'd'] [( column, column... )]
    MAP TYPE FAMILY f [WITH SCHEMA] schema [WITH DESCRIPTION 'd']


Within a group-type family, individual column qualifiers are specified with:

    [COLUMN] foo [WITH SCHEMA] schema [WITH DESCRIPTION 'd']


#### `CREATE TABLE` Example

The following is an example that puts together all of the different syntax
elements described in the previous section:

    schema> CREATE TABLE users WITH DESCRIPTION 'some data'
         -> ROW KEY FORMAT (username STRING, HASH(SIZE=3))
         -> PROPERTIES (MAX FILE SIZE = 10000000000)
         -> WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
         ->     MAXVERSIONS = INFINITY,
         ->     TTL = FOREVER,
         ->     INMEMORY = false,
         ->     COMPRESSED WITH GZIP,
         ->     BLOOM FILTER = ROW,
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

Row keys are the exception to this: their format is final.

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
        | BLOOM FILTER = { NONE | ROW | ROWCOL }
        | BLOCK SIZE = int
    ... or a FAMILY definition (see the earlier section on the CREATE TABLE syntax)

    ALTER TABLE t RENAME LOCALITY GROUP lg [AS] lg2;
    ALTER TABLE t DROP LOCALITY GROUP lg;

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR FAMILY f;
    ALTER TABLE t SET SCHEMA = schema FOR [MAP TYPE] FAMILY f; (deprecated)

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR COLUMN info:foo;
    ALTER TABLE t SET SCHEMA = schema FOR COLUMN info:foo; (deprecated)

    ALTER TABLE t SET DESCRIPTION = 'desc';
    ALTER TABLE t SET table_property = value;
    ... Where 'table_property' is one of MAX FILE SIZE or MEMSTORE FLUSH SIZE.

    ALTER TABLE t SET DESCRIPTION = 'desc' FOR LOCALITY GROUP lg;
    ALTER TABLE t SET property FOR LOCALITY GROUP lg;
    ... where 'property' is one of MAXVERSIONS, INMEMORY, TTL, etc. as above.


### Managing Column Schemas

Starting in KijiSchema 1.3.0, newly-created Kiji instances support table layouts with
_validated schemas_ (using layout version `layout-1.3`).  This permits schema evolution in
a flexible fashion, while respecting the requirements of existing components in a
distributed Big Data Application.

When you create a column, you typically apply a schema to it. e.g.:

    ALTER TABLE t ADD COLUMN info:foo WITH SCHEMA "int";

This sets the expected reader and writer schema for this column to be `"int"`. In practice,
each column actually has several schemas associated with it:

* A set of one or more acceptable reader schemas for applications to use.
* A set of one or more acceptable writer schemas for applications to use.
* A set of one or more _recorded_ schemas -- writer schemas which have been used before.
* A _default reader schema_ (optional).
* The name of a SpecificRecord class to use as a reader schema (optional).

Schema validation ensures that new schemas added to the reader and writer lists are
compatible with all other current readers and writers. In addition, they must be compatible
with writer schemas which were actually used in the past to record data to the table. This
allows applications to ensure they can always read and write all data in the table--but you
do not need to update all applications simultaneously to manage a schema change.

In the case of `info:foo` above, it would be legal to add `"long"` as a reader schema,
since all `"int"` data stored by Avro can be read as `"long"`. It would not be legal
to add `"long"` as a writer schema, since readers who expect to read this data as `"int"`
could not convert any `long` value back to an `int`. To add `"long"` as a writer schema
would require that you first unregister `"int"` as a valid reader schema, and add `"long"`
as the new valid reader schema. This change would require that you first shut down any
processes that expect to write data using the `"int"` reader schema.

(Note: this enforcement logic affects tables with layout version `layout-1.3`, and
`VALIDATION` set to `DEVELOPER` or `STRICT`. Tables using `VALIDATION = NONE` or
`VALIDATION = LEGACY` may add any arbitrary set of reader and writer schemas, at their own
peril. `layout-1.3` can only be used in Kiji instances with data version `system-2.0`,
i.e. instances created by KijiSchema 1.3.0 or later. Tables in existing Kiji instances will not use
the commands in this section; they should continue to use `ALTER TABLE.. SET SCHEMA =
schema..`.)

In addition to the lists of reader and writer schemas available, KijiSchema also allows
you to provide applications with a _default reader schema_ for a column; if they don't
have a preferred schema, they may rely on this suggestion from the table layout. It is
recommended that you do not change this schema in an incompatible way, so that all
applications continue to function. This compatibility is not enforced by the system on
your behalf.

Finally, you may provide a _default specific reader class_ for a column. Applications that
prefer to use the Avro SpecificRecord API may use this class (assuming its on the
application's classpath) to deserialize records from the table, in the same manner as the
_default reader schema_ discussed above. Because the SpecificRecord class name listed
in the table layout is not guaranteed to be available to every application at runtime,
and because it may change while an application is live, we encourage you to explicitly
specify the SpecificRecord class name you plan to use in your application, so you can
be sure it's available on your application's classpath.

#### Supported Validation Modes

KijiSchema supports multiple levels of schema validation. When creating a table, you can
set the validation mode to apply to its columns:

    CREATE TABLE t
        ROW KEY FORMAT...
        PROPERTIES (
          VALIDATION = { NONE | LEGACY | DEVELOPER | STRICT }
        )
        WITH LOCALITY GROUP...

The `VALIDATION` property of the table specifies the preferred schema validation mode to
apply to columns in this table. This affects what reader and writer schemas are legal to
attach to a given column.

* `NONE` - No validation; any combination of reader and writer schemas may be declared. At
  write time, there is no enforcement that the actual writer schema is in the approved
  list of writer schemas.
* `LEGACY` - Performs validation according to the semantics of tables using `layout-1.2`
  and below (as created by KijiSchema 1.2.0 and below). Primitive types are validated
  against the approved writer schema at write time; incompatible changes to the table
  layout are permitted.
* `DEVELOPER` - Performs strong validation of reader and writer schema compatibility when
  changing the layout of a table. Compatible writer schemas may be added on-demand at
  write time by a new process. This is the default.
* `STRICT` - Performs strong validation of reader and writer schema compatibility when
  changing the layout of a table; you may not add any schemas that are incompatible with
  currently-active reader and writer schemas (nor any previously-used "recorded" writer
  schemas). At write time, the writer schema must match one of the schemas explicitly
  added through the Kiji shell.

##### Changing the Validation Semantics for a Table

Changing the `VALIDATION` property for a table will only affect new columns going forward.
Existing columns will use their current validation semantics:

    ALTER TABLE t SET VALIDATION = STRICT;
    ALTER TABLE t ADD COLUMN info:foo WITH SCHEMA "int"; // Strict validation semantics
    ALTER TABLE t SET VALIDATION = NONE;
    ALTER TABLE t ADD COLUMN info:bar WITH SCHEMA "long"; // No validation for this col.

#### Listing Column Schemas

The examples in the following few sections use the following single-column table:

    schema> CREATE TABLE t WITH LOCALITY GROUP default (FAMILY info (foo "int"));
    OK.

By default this uses the `VALIDATION = DEVELOPER` table property.

When you run a `DESCRIBE <tablename>` command to describe a table, it will print the
default reader schema for each column, as well as tell you how many other schemas
are present:

    schema> DESCRIBE t;
    Table: t ()
    Row key:
      key: STRING NOT NULL

    Column family: info
      Description:

      Column info:foo ()
        Default reader schema: "int"
        1 reader schema(s) available.
        1 writer schema(s) available.

You may list the active reader and writer schemas, as well as previously-recorded writer
schemas, using the commands:

    DESCRIBE <table> COLUMN info:foo SHOW [n] READER SCHEMAS;
    DESCRIBE <table> COLUMN info:foo SHOW [n] WRITER SCHEMAS;
    DESCRIBE <table> COLUMN info:foo SHOW [n] RECORDED SCHEMAS;

The parameter `n` is optional; you may use this to control the number of schemas returned.
By default it will print the most recent 5 schemas added to the column. Each schema will
be printed on a line by itself, prefixed by its unique identifier within KijiSchema:

    schema> DESCRIBE t COLUMN info:foo SHOW READER SCHEMAS;
    Table: t
    Column: info:foo
      Description:

      Reader schemas:
    (*) [2]: "int"

The asterisk next to the schema denotes that `"int"` is the default reader schema.
When manipulating lists of schemas, you may refer to schemas by their literal text
(`"int"`), by a SpecificRecord class name (`CLASS com.example.MyRecord`), or by
their id (`ID 2`). Schema ID numbers are not stable from instance to instance and
should only be used when modifying data within a single Kiji instance.

#### Adding compatible schemas

You may add compatible reader and writer schemas to a column or map type family
without changing any existing applications. A "compatible" reader schema is one
that can read data written by all currently-active writer schemas, and any writer
schemas previously used to record data to the table. A "compatible" writer schema
is one which can be read by all currently-active reader schemas.

In the example `info:foo` column above, we could add `"long"` as a reader schema:

    schema> ALTER TABLE t ADD READER SCHEMA "long" FOR COLUMN info:foo;
    ...
    OK

    schema> DESCRIBE t COLUMN info:foo SHOW READER SCHEMAS;
    Table: t
    Column: info:foo
      Description:

      Reader schemas:
    [3]: "long"

    (*) [2]: "int"


Note that `"int"` remains the default reader schema for applications that are not
specifically coded to use the new reader schema. Because some applications may expect
to read data with schema `"int"`, the following will not work:

    schema> ALTER TABLE t ADD WRITER SCHEMA "long" FOR COLUMN info:foo;
    In column: 'info:foo' Reader schema: "int" is incompatible with writer schema: "long".

The `ALTER TABLE.. ADD SCHEMA` command has the following syntax:

    ALTER TABLE <tablename> ADD [ [DEFAULT] READER | WRITER ] SCHEMA <schema>
    FOR COLUMN familyName:qualifier;

You may add a schema to the readers list (`ADD READER SCHEMA`) or the writers list (`ADD
WRITER SCHEMA`). If you do not specify these options, the schema will be added to both the
approved reader and writer lists. You may also explicitly specify a schema as the default
reader schema by running `..ADD DEFAULT READER SCHEMA..`.

The schema may be specified as follows:

    SCHEMA <json>
    SCHEMA CLASS com.example.MySpecificRecord
    SCHEMA ID <id>

To refer to a schema by by its SpecificRecord class, you must make it available to the
Kiji shell. All jar files in the `$KIJI_CLASSPATH` environment variable will be present.
You can also load jars at runtime by using the command:

    USE JAR INFILE '/path/to/my-local-jar-file.jar';

#### Removing Deprecated Schemas

Suppose our example application no longer reads data to `info:foo` as type `"int"`; we
know that all readers are using the `"long"` reader schema. In this case, we may remove
the unnecessary schema:

    schema> ALTER TABLE t DROP READER SCHEMA "int" FOR COLUMN info:foo;
    Warning: Removing default reader schema
    ...
    OK.

Since `"int"` was the default reader schema, there is now no default reader schema
present. We can verify this by running a `DESCRIBE` command:

    schema> DESCRIBE t COLUMN info:foo SHOW READER SCHEMAS;
    Table: t
    Column: info:foo
      Description:

      Reader schemas:
    [3]: "long"

Note the lack of a `(*)` next to the `"long"` reader schema. You can specify that this is
the default reader schema by running:

    schema> ALTER TABLE t ADD DEFAULT READER SCHEMA ID 3 FOR COLUMN info:foo;
    ...
    OK
    schema> DESCRIBE t COLUMN info:foo SHOW READER SCHEMAS;
    Table: t
    Column: info:foo
      Description:

      Reader schemas:
    (*) [3]: "long"

This example used `ID 3` to refer to the schema as above; you could also have used `...ADD
DEFAULT READER SCHEMA "long"...` to accomplish the same.

The `ALTER TABLE.. DROP SCHEMA` command has the following syntax:

    ALTER TABLE <tablename> DROP [ [DEFAULT] READER | WRITER ] SCHEMA <schema>
    FOR COLUMN familyName:qualifier;

#### Schema Management Commands for Map-Type Families

Schemas for map-type column families are managed in the same way as schemas for columns
within a group-type family. The following syntax is supported for map-type family schema
management:

    ALTER TABLE <tablename> ADD [ [DEFAULT] READER | WRITER ] SCHEMA <schema>
    FOR [MAP TYPE] FAMILY familyName;

    ALTER TABLE <tablename> DROP [ [DEFAULT] READER | WRITER ] SCHEMA <schema>
    FOR [MAP TYPE] FAMILY familyName;

#### Deprecated Schema Management Statements

The following commands are deprecated:

    ALTER TABLE t SET SCHEMA = schema FOR COLUMN info:foo;
    ALTER TABLE t SET SCHEMA = schema FOR [MAP TYPE] FAMILY f;

In tables that support layout validation, running these commands will display a warning
message, and they now operate like `ALTER TABLE t ADD SCHEMA...`.

Tables in Kiji instances created by KijiSchema 1.2 or previous should continue to use
these commands, as schema validation is only enforced in Kiji instances installed by
KijiSchema 1.3 or above.


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

