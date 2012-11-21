---
layout: post
title: Command-Line Tool Reference
category: userguide
component: schema
version: 1.0.0-rc1
tags : [schema-ug]
order : 7
description: A reference of commands available for the schema command line tool.
---

Running kiji with no arguments will list all the available tools:

{% highlight bash %}
$ kiji COMMAND [FLAG]...
{% endhighlight %}

## Commands

*  `help`                  - Display this help message.
*  `install`               - [Install KijiSchema onto a running HBase instance](#ref.install)
*  `uninstall`             - [Uninstall KijiSchema from a running HBase instance](#ref.uninstall)
*  `version`               - Print the version of KijiSchema.
*  `classpath`             - Print the classpath used to build or run KijiSchema
*  `jar`                   - [Run a main class from the specified jar](#ref.jar)
        Use: `kiji jar <jarFile> <mainClass> [args...]`
*  `ls`                    - [List, describe, or scan Kiji tables](#ref.ls)
*  `create-table`          - [Create a Kiji table](#ref.create-table)
*  `delete-table`          - [Delete a Kiji table](#ref.delete-table)
*  `flush-table`           - [Flush table write-ahead logs](#ref.flush-table)
*  `layout`                - [Manage table layouts](#ref.layout)
*  `increment`             - [Increment a counter cell](#ref.increment)
*  `put`                   - [Set the value of a single cell](#ref.put)
*  `synthesize-user-data`  - [Synthesize user data into a table](#ref.synthdata)

## Targeting a KijiSchema instance: Kiji URI<a id="ref.kiji_uri"> </a>

Most commands accept an optional parameter `--kiji=<kiji-uri>` that specifies a
KijiSchema instance to interact with.
The format for this parameter is a URI generally formatted as:
`kiji://zookeeper_host:2181/kiji_instance_name`, and includes:

*  The address of the HBase instance KijiSchema has been installed on.
   This is the address of the ZooKeeper quorum used by the HBase instance.
   The default is `.env`, which tells KijiSchema to use the HBase instance
   identified by the HBase configuration files in `$HBASE_HOME`.
*  And the name of the KijiSchema instance.
   The default KijiSchema instance name is `default`.

The default value for this parameter is `--kiji=kiji://.env/default`, which
references the KijiSchema instance named "default" and installed on the HBase
instance identified in the HBase configuration files in `$HBASE_HOME`.

The URI for KijiSchema instance named "the_instance" and installed on a mini
HBase cluster running locally on a laptop is
`--kiji=kiji://localhost:2181/the_instance`.

## Installation: `install`<a id="ref.install"> </a>

The `kiji install` command will create the initial metadata tables
`kiji.<instance-name>.meta`, `kiji.<instance-name>.status`,
`kiji.<instance-name>.schema_id` and `kiji.<instance-name>.schema_hash`
required by the KijiSchema system.
This should be run once during initial setup of KijiSchema.

A different HBase instance or KijiSchema instance name may be specified using the
[`--kiji` parameter](#ref.kiji_uri).

## Removal: `uninstall`<a id="ref.uninstall"> </a>

The `kiji uninstall` command removes an installed KijiSchema instance, and deletes
all the user tables it contains.
The KijiSchema instance to remove is specified through the
[`--kiji` parameter](#ref.kiji_uri).

This command accepts an optional parameter:
*  `--confirm`
   - Must be set to perform the instance removal without interactive confirmation.

## Listing Information: `ls`<a id="ref.ls"> </a>

The `kiji ls` command is the basic tool used to explore a KijiSchema repository.
It can drill down into KijiSchema-based data sets at many levels.

When run with `--instances`, this command lists the Kiji instances installed on the
HBase instance specified with `--kiji=<kiji-uri>` (by default, `kiji://.env`):

{% highlight bash %}
$ kiji ls --instances
default
{% endhighlight %}

When run with no arguments, this command lists the tables in the Kiji instance
specified with `--kiji=<kiji-uri>` (by default, `kiji://.env/default`):

{% highlight bash %}
$ kiji ls
users
products
{% endhighlight %}

The `ls` command can also be used to list the contents of a table.
`kiji ls --table=<table-name>` displays the contents of a Kiji table.
Each record appears as a set of lines, set apart by blank lines.
Each cell appears on two lines:
the first line contains the row key (a hashed representation of a primary key),
a timestamp (expressed in milliseconds since the UNIX epoch),
and the cell name (`family:qualifier`).
The second line contains the string representation of the cell data itself.
For example, running `kiji ls --table=users` on a table generated with the `synthdata`
command (see [Generating Sample Data](#ref.synthdata)) displays rows like:

{% highlight bash %}
$ kiji ls --table=users
\xA6t\xCEIm\xB7A\x88\x7F\xD1\xA9n\xB0\xEC\x16\xDB [1305851507300] info:name
                                 Olga Jefferson
\xA6t\xCEIm\xB7A\x88\x7F\xD1\xA9n\xB0\xEC\x16\xDB [1305851507301] info:email
                                 Olga.Jefferson@hotmail.com

\xf8\x8f\xe2\xb2,E\x8b\xea\xd5\x08\xf8\x8a\xee`\x91y [1305851507425] info:name
                                 Sidney Tijuana
\xf8\x8f\xe2\xb2,E\x8b\xea\xd5\x08\xf8\x8a\xee`\x91y [1305851507427] info:email
                                 Sidney.Tijuana@hotmail.com
…
{% endhighlight %}

Providing just the `--table` argument is not particularly useful;
you will typically want to restrict the set of data printed to the terminal.
The following options will do just that:

*  `--columns=family:qualifier,family:qualifier...`
   - Display a subset of the available columns. `--columns=*` will include all columns.

*  `--start-row=row-key` and `--limit-row=row-key`
   - Restrict the row range to print.

*  `--max-rows=<int>`
   - Display at most this many rows of data.

*  `--max-versions=<int>`
   - Display at most this many versions of each cell.

*  `--min-timestamp=<long>` and `--max-timestamp=<long>`
   - Display only cells whose timestamps fall within the given range.
     Timestamps are expressed in milliseconds since the Epoch.

*  `--entity-hash=<string>`
   - Display only cells from a single row; ignores `--start-row` and `--limit-row`.
     The string argument is the hexadecimal representation of the pre-hashed row id.

*  `--entity-id=<string>`
   - Display only cells from a single row; ignores `--start-row` and `--limit-row`.
     The string is the human-readable value representing the row id, encoded as UTF8.

## Creating Tables: `create-table`<a id="ref.create-table"> </a>

The `kiji create-table` command creates a new
Kiji table. This is stored in an underlying HBase table with the name
`kiji.<instance-name>.table.<table-name>`.

This command has two mandatory arguments:

*  `--table=<table-name>`
   - Name of the table to create.
     It is an error for this table to already exist.
*  `--layout=<path/to/layout.json>`
   - Path to a file a JSON file containing the table layout specification,
     as described in [Managing Data]({{site.userguide_url}}managing-data#layouts).

The following arguments are optional:

*  `--kiji=<kiji-uri>`
   - Address of the Kiji instance to interact with.

*  `--num-regions=<int>`
   - The number of initial regions to create in the table.
     This may only be specified if the table uses row key hashing.
     It may not be used in conjunction with `--split-key-file`.

*  `--split-key-file=<filename>`
   - Path to a file containing the row keys to use as initial boundaries between regions.
     This may only be specified if the table uses row key hashing.
     It may not be used in conjunction with `--num-regions`.

<!--
TODO: Document the format of the files specifying split keys.
-->

## Deleting tables: `delete-table`<a id="ref.delete-table"> </a>

The `delete-table` command will delete a KijiSchema
table definition, and drop all rows which were in the table.
This command has two mandatory arguments:

*  `--table=<table-name>`
   - Specifies the name of the table to delete.
*  `--confirm`
   - Must be set to perform the table deletion without interactive confirmation.

It also accepts an optional `--kiji=<kiji-uri>` argument to
specify the address of the installed Kiji instance from which to delete
the table.

Flushing tables: `flush-table`<a id="ref.flush-table"> </a>
--------------------------------------------------------

The `flush-table` command will instruct HBase to
flush the contents of a table to HDFS. When HBase receives new data, it
is recorded in a write-ahead log (WAL). But this WAL is not merged with
existing table files until the table is flushed or compacted. This
happens more frequently if more data is written to a table. But you can
force data to be written to table files with this command. If a table is
not frequently updated, flushing the data with this command may improve
recovery time in the event that HBase experiences a failure.

You must use one or both of the following arguments to specify what
to flush:

*  `--table=<table-name>`
   - Specifies the table name to flush.
*  `--meta`
   - If set, flushes KijiSchema metadata tables.

It also accepts an optional `--kiji=<kiji-uri>` argument to
specify the address of the installed Kiji instance in which to find
the table to flush.

You should only flush tables during a period of relative inactivity.
Flushing while a large number of operations are ongoing may adversely
affect performance. The flush operation is also asynchronous; the
command may return before the actual flush operation is complete.

Managing layouts: `layout`<a id="ref.layout"> </a>
------------------------------------------------

The `kiji layout` command displays or modifies
the layout associated with a table.

When run with `--table=<table-name>`, this displays the current layout
associated with a given table:

{% highlight bash %}
$ kiji layout --table=users
{
  name: "users",
  description: "The user table",
  keys_format : {encoding : "RAW"},
  locality_groups : [
    …
  ],
  layout_id : "3",
}
{% endhighlight %}

When run with the `--do=set --layout=/path/to/layout.json`
argument, this sets the layout for a table to match the specified
layout JSON file.

The `--dry-run` argument specifies that `--do=set`
should not actually update the layout; it simply prints out
a message stating whether or not the update would succeed (i.e., whether or not
the layout is valid) and what locality groups would be updated by the new layout.

When run with the `--do=history --max-versions=<int>`, this command dumps the most
recent versions of the table layout.

Running an Application Jar with KijiSchema: `jar`<a name="jar"> </a>
--------------------------------------------------------------------

If your application requires KijiSchema and its dependencies, you
can use the `kiji jar` command to launch your program's
main method with KijiSchema present on the classpath.

This command requires two unlabeled arguments: the jar filename, and the
main class to run:

{% highlight bash %}
$ kiji jar myapp.jar com.pkg.MyApp [args...]
{% endhighlight %}

Incrementing counters: `increment`<a name="ref.increment"> </a>
---------------------------------------------------------------

The `kiji increment` command may be used to increment
(or decrement) a KijiSchema counter.

The following arguments are required:

*  `--table=<table-name>`      - Target table
*  `--column=family:qualifier` - Target column
*  `--entity-hash=entity`      - Target row id (the actual pre-hashed entity string)
*  `--entity-id=entity`        - Target row id (an unhashed, human-readable string)
*  `--value=amount`            - The value to increment by.

Exactly one of `--entity-id` or `--entity-hash` must be used.

Setting Individual Cells: `put`<a name="put"> </a>
--------------------------------------------------

To aid in the insertion of small data sets, debugging, and testing, the
`kiji put` command may be used to insert individual values in a Kiji table.

The following arguments are required:

*  `--table=<table-name>`      - Target table
*  `--column=family:qualifier` - Target column
*  `--entity-hash=entity`      - Target row id (the actual pre-hashed entity string)
*  `--entity-id=entity`        - Target row id (an unhashed, human-readable string)
*  `--value=<JSON value>`      - The value to insert. The value is specified as a
   JSON string according to [the Avro JSON encoding specification](http://avro.apache.org/docs/current/spec.html#json_encoding)

Exactly one of `--entity-id` or `--entity-hash` must be used.

The following arguments are optional:

*  `--schema=Avro schema`      - By default, KijiSchema will use the reader schema
   attached to a column in its layout to decode the JSON and encode the binary
   data for insertion in the table. This argument allows you to use an alternate
   writer schema.
*  `--timestamp=long`          - Specifies a timestamp (in milliseconds since the Epoch) other than "now".
*  `--kiji=<kiji-uri>`         - Address of the Kiji instance to interact with.

Generating Sample Data: `synthesize-user-data`<a name="ref.synthdata"> </a>
---------------------------------------------------------------------------

In the interest of enabling quick experimentation with KijiSchema,
the `kiji synthesize-user-data` tool will
generate a number of semi-random rows for you.

The tool creates a set of rows which contain columns `info:id`, `info:name`, and
`info:email`; these are pseudo-randomly generated first and last names, with
plausible email addresses with gmail, hotmail, etc. accounts based on the
generated names. These columns can be used with mappers and reducers.

To use this tool, first create a table with the layout in
`${KIJI_HOME}/examples/synthdata-layout.xml`.
Then invoke `bin/kiji synthesize-user-data --table=<table-name>`.
This will generate 100 rows of data.
You can create a different number of records by specifying
`--num-users=<int>`.

You can specify a different list of names with the `--name-dict=filename` argument.

It also accepts an optional `--kiji=<kiji-uri>` argument to specify the address of the Kiji instance.
