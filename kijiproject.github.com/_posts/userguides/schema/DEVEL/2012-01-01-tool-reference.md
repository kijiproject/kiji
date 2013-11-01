---
layout: post
title: Command-Line Tool Reference
categories: [userguides, schema, devel]
component: schema
version: devel
tags : [schema-ug]
order : 8
description: A reference of commands available for the schema command line tool.
---

Running `kiji help` will list all the available tools. The usage format for the tools is:

{% highlight bash %}
$ kiji <tool> [FLAG]...
{% endhighlight %}

### Instance administration
*  `install`                 - [Install a kiji instance onto a running HBase cluster.](#ref.install)
*  `uninstall`               - [Remove a kiji instance from a running HBase cluster.](#ref.uninstall)
*  `metadata`                - [Backup or restore kiji metadata.](#ref.metadata)

### Table administration
*  `create-table`            - [Create a kiji table in a kiji instance.](#ref.create-table)
*  `delete`                  - [Delete kiji tables, rows, and cells.](#ref.delete)
*  `layout`                  - [View or modify kiji table layouts.](#ref.layout)
*  `flush-table`             - [Flush kiji user and meta table write-ahead logs.](#ref.flush-table)

### Data inspection/modification
*  `ls`                      - [List kiji instances, tables and columns.](#ref.ls)
*  `get`                     - [Get individual records of a kiji table.](#ref.get)
*  `scan`                    - [Scan the records of a kiji table.](#ref.scan)
*  `increment`               - [Increment a counter column in a kiji table.](#ref.increment)
*  `put`                     - [Write a cell to a column in a kiji table.](#ref.put)

### Miscellaneous
*  `help`                    - Describe available Kiji tools.
*  `version`                 - Print the kiji distribution and data versions in use.
*  `classpath`               - Print the classpath used to run kiji tools.
*  `synthesize-user-data`    - [Synthesize user data into a kiji table.](#ref.synthdata)
*  `jar`                     - [Run a class from a user-supplied jar file.](#ref.jar)

### Targeting a Kiji instance or a Kiji table with Kiji URIs<a id="ref.kiji_uri"> </a>

Most KijiSchema command-line tools require a parameter specifying an HBase cluster, a Kiji instance or a Kiji table.
These elements can be specified in a unified way using Kiji URIs.

Kiji URIs are formatted hierarchically:
{% highlight bash %}
    kiji://<HBase cluster>/<Kiji instance>/<Kiji table>/<columns>
{% endhighlight %}

*  `HBase cluster`: the address of a ZooKeeper quorum used by the HBase
   instance KijiSchema has been installed on. The default is `.env`, which
   tells KijiSchema to use the HBase instance identified by the HBase
   configuration files available on the classpath.
*  `Kiji instance`: the name of the Kiji instance within the HBase cluster.
*  `Kiji table`: the name of the Kiji table within the Kiji instance.
*  `columns`: a set of columns or column families within the Kiji table, separated by comas.

The HBase cluster address is a required component of all Kiji URIs.
Additional components may be required depending on the context. For instance:

*   `kiji://localhost:2181` references the HBase cluster whose ZooKeeper quorum is composed
    of the server listening on `localhost:2181`.
*   `kiji://localhost:2181/kiji_instance_1` designates the Kiji instance named `kiji_instance_1`
    and living in the HBase cluster `kiji://localhost:2181`.
*   `kiji://localhost:2181/kiji_instance_1/the_table` designates the Kiji table named `the_table`
    and living in the Kiji instance `kiji://localhost:2181/kiji_instance_1`.
*   `kiji://localhost:2181/kiji_instance_1/the_table/family1,family:column2` references the
    column family `family1` and the column `family:column2` within the Kiji table
    `kiji://localhost:2181/kiji_instance_1/the_table`.

If a URI is left unspecified, then the URI is set to the default `kiji://.env/default`, which
references the Kiji instance named `default`, installed on the HBase
cluster identified by the HBase configuration available on the classpath.

As of version `1.0.0-rc5`, KijiSchema also support relative URIs.
All such URIs are relative to `kiji://.env/`. For instance:

*   `/kiji_instance` is equivalent to `kiji://.env/kiji_instance`.
*   `/kiji_instance/table` is equivalent to `kiji://.env/kiji_instance/table`.

### Scripting using the command-line interface<a id="ref.interactive"> </a>

All Kiji command-line tools accept a `--interactive` flag that controls whether user
interactions are allowed. By default, this flag is set to true, which enables user
interactions such as confirmations for dangerous operations.

When scripting Kiji commands, you may disable user interactions with `--interactive=false`.



## Installation: `install`<a id="ref.install"> </a>

The `kiji install` command creates the initial metadata tables
`kiji.<instance-name>.meta`, `kiji.<instance-name>.status`,
`kiji.<instance-name>.schema_id` and `kiji.<instance-name>.schema_hash`
required by the KijiSchema system.
This should be run once during initial setup of a KijiSchema instance.

The HBase cluster and the Kiji instance name may be specified with a [Kiji URI](#ref.kiji_uri):

{% highlight bash %}
kiji install --kiji=kiji://hbase_cluster/kiji_instance
{% endhighlight %}

## Removal: `uninstall`<a id="ref.uninstall"> </a>

The `kiji uninstall` command removes an installed KijiSchema instance, and deletes
all the user tables it contains.
The HBase cluster and the name of the Kiji instance to remove is specified with a [Kiji URI](#ref.kiji_uri):

{% highlight bash %}
kiji uninstall --kiji=kiji://hbase_cluster/kiji_instance
{% endhighlight %}

## Metadata backups: `metadata`<a name="ref.metadata"> </a>


The `kiji metadata` command allows you to backup and restore metadata information in KijiSchema.
This metadata contains table layout information as well as the schema definitions.

### Creating a backup

You can backup the metadata for a specific Kiji instance with:

{% highlight bash %}
kiji metadata --kiji=kiji://hbase_cluster/kiji_instance --backup=mybackup
{% endhighlight %}

### Restoring from a backup

Similarily, you can restore the metadata for a specific Kiji instance with:

{% highlight bash %}
kiji metadata --kiji=kiji://hbase_cluster/kiji_instance --restore=mybackup
{% endhighlight %}

After asking for confirmation:

    Are you sure you want to restore metadata from backup?
    This will delete your current metatable.
    Please answer yes or no.

Restoration begins:

    Restoring Metadata from backup.
    Restore complete.

If restoration of only a subset of the table and schema information is desired, the following flags should be used:
* `--tables` - restores all tables from the metadata backup into the specified Kiji instance.
* `--schemas` - restores all schema table entries from the metadata backup into the specified Kiji instance.



## Creating Tables: `create-table`<a id="ref.create-table"> </a>

The `kiji create-table` command creates a new
Kiji table. This is stored in an underlying HBase table with the name
`kiji.<instance-name>.table.<table-name>`.

This command has two mandatory arguments:

*  `--table=<table-uri>`
   - Kiji URI of the table to create.
     It is an error for this table to already exist.
*  `--layout=<path/to/layout.json>`
   - Path to a file a JSON file containing the table layout specification,
     as described in [Managing Data]({{site.userguide_schema_devel}}/managing-data#layouts).

The following arguments are optional:

*  `--num-regions=<int>`
   - The number of initial regions to create in the table.
     This may only be specified if the table uses row key hashing.
     It may not be used in conjunction with `--split-key-file`.

*  `--split-key-file=<filename>`
   - Path to a file containing the row keys to use as initial boundaries between regions.
     This may only be specified if the table uses row key hashing.
     It may not be used in conjunction with `--num-regions`.

The `--split-key-file` argument must be the path to a file of row keys to use
as boundaries between regions. Split key files are formatted as one HBase
encoded row per line:

* each HBase encoded row is formatted in ASCII form;
* non printable characters are escaped as `\x??`
* backslash must be escaped as `\\`
*  The first and last HBase rows are omitted: a region split file with one
   entry E designates 2 regions: `[''..E)` and `[E..)`.

## Deleting tables, rows, and cells: `delete`<a id="ref.delete"> </a>

The `kiji delete` command will delete a KijiSchema
table, row, or cell, and drop all values which were in them.
This command has one mandatory argument:

*  `--target=<kiji-uri>` -
   URI of the target to delete or to delete from.
   The target may be an entire Kiji instance, a Kiji table or a set of columns within a Kiji table.

And several optional arguments:

*  `--entity-id=<entity-id>` -
   Specifies the entity ID of a single row to delete or to delete from.
   Requires the target Kiji URI to designate at least a Kiji table.

   The default is to not target a specific row, ie. to delete the entire Kiji table specified with `--target=...`.

*  `--timestamp=<timestamp-spec>` -
   Timestamp specification:
   *   `'<timestamp>'` to delete cells with exactly this timestamp, expressed in milliseconds since the Epoch;
   *   `'latest'` to delete the most recent cell only;
   *   `'upto:<timestamp>'` to delete all cells with a timestamp older than the specified timestamp expressed in milliseconds since the Epoch;
   *   `'all'` to delete all cells, irrespective of their timestamp.

   The default is `--timestamp=all`.

## Managing layouts: `layout`<a id="ref.layout"> </a>

The `kiji layout` command displays or modifies the layout associated with a table.

This command is intended for use by system administrators, or for debugging purposes.
Most users should view or modify their table layouts by using the KijiSchema DDL shell.

This command requires two parameters:

*  `--table=<table-uri>` - URI of the Kiji table to examine the layout of.
*  `--do=<action>` - Action to perform on the layout: `dump` (the default), `set` or `history`.

You may dump the current layout of a table with:
{% highlight bash %}
$ kiji layout --table=kiji://.env/default/users
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

You may update the layout of a table with:
{% highlight bash %}
$ kiji layout --table=kiji://.env/default/users --do=set --layout=/path/to/layout.json
{% endhighlight %}

The file `/path/to/layout.json` is a JSON descriptor of the updated table layout.

Optionally, you may use the `--dry-run` argument to prints out messages stating whether or not the update would succeed
(i.e., whether or not the layout is valid) and what locality groups would be updated by the new layout.

Finally, you may dump the layout history of a table with:
{% highlight bash %}
$ kiji layout \
    --table=kiji://.env/default/users \
    --do=history \
    --max-version=5 \
    --write-to=/path/to/table-layout-history/layout
{% endhighlight %}

This dumps the 5 latest revisions of the table layout in 5 JSON files `/path/to/table-layout-history/layout-<timestamp>.json`.

## Flushing tables: `flush-table`<a id="ref.flush-table"> </a>

The `kiji flush-table` command will instruct HBase to
flush the contents of a table to HDFS. When HBase receives new data, it
is recorded in a write-ahead log (WAL). But this WAL is not merged with
existing table files until the table is flushed or compacted. This
happens more frequently if more data is written to a table. But you can
force data to be written to table files with this command. If a table is
not frequently updated, flushing the data with this command may improve
recovery time in the event that HBase experiences a failure.

You must use one or both of the following arguments to specify what
to flush:

*  `--table=<table-uri>`
   - URI of the Kiji table to flush.
*  `--meta`
   - If set, flushes KijiSchema metadata tables.

You should only flush tables during a period of relative inactivity.
Flushing while a large number of operations are ongoing may adversely
affect performance. The flush operation is also asynchronous; the
command may return before the actual flush operation is complete.


## Listing Information: `ls`<a id="ref.ls"> </a>

The `kiji ls` command is a basic tool used to explore a KijiSchema repository.
It can list instances, tables, or even columns in the specified Kiji URI argument.
Note that this tool takes Kiji URIs as unflagged arguments.
If no URI argument is specified, then the tool assumes the default URI: `kiji://.env`.

You may list the Kiji instances existing in an HBase cluster by specifying the URI of an HBase cluster:
{% highlight bash %}
$ kiji ls kiji://localhost:2181
kiji://localhost:2181/kiji_instance1/
kiji://localhost:2181/kiji_instance2/
{% endhighlight %}

You may list the Kiji tables within a Kiji instance by specifying the URI of a Kiji instance:
{% highlight bash %}
$ kiji ls kiji://localhost:2181/kiji_instance1
kiji://localhost:2181/kiji_instance1/table1
kiji://localhost:2181/kiji_instance1/table2
kiji://localhost:2181/kiji_instance1/table3
{% endhighlight %}

You may list the columns of a table by specifying the URI of a Kiji table:
{% highlight bash %}
$ kiji ls kiji://localhost:2181/kiji_instance1/table1
kiji://localhost:2181/kiji_instance1/table1/info:name
kiji://localhost:2181/kiji_instance1/table1/info:email
…
{% endhighlight %}

Finally, you may even iteratively list multiple URIs by providing them as multiple arguments:
{% highlight bash %}
$ kiji ls kiji://localhost:2181 kiji://localhost:2181/kiji_instance1 kiji://localhost:2181/kiji_instance1/table1
kiji://localhost:2181/kiji_instance1/
kiji://localhost:2181/kiji_instance2/
kiji://localhost:2181/kiji_instance1/table1
kiji://localhost:2181/kiji_instance1/table2
kiji://localhost:2181/kiji_instance1/table3
kiji://localhost:2181/kiji_instance1/table1/info:name
kiji://localhost:2181/kiji_instance1/table1/info:email
…
{% endhighlight %}


## Getting a row: `get`<a id="ref.get"> </a>

The `kiji get` command prints the record specified by the flag `--entity-id` in the Kiji URI argument (with or without columns specified).
Each cell from the record appears on two lines:
the first line contains the record's entity ID,
the cell timestamp expressed in milliseconds since the UNIX epoch,
and the cell column name (`family:qualifier`) specified by the Kiji URI argument;
The second line contains the string representation of the cell data itself.

{% highlight bash %}
$ kiji get kiji://localhost:2181/kiji_instance1/table1 --entity-id="['Olga Jefferson']"
Looking up entity: ['Olga Jefferson'] from kiji table: : kiji://localhost:2181/kiji_instance1/table1/info:name,info:email
entity-id=['Olga Jefferson'] [1305851507300] info:name
                                 Olga Jefferson
entity-id=['Olga Jefferson'] [1305851507301] info:email
                                 Olga.Jefferson@hotmail.com
{% endhighlight %}

The URI is specified similar to `kiji ls`, but the flag `--entity-id` is as follows.

*  `--entity-id=<string>` -
   Specifies the entity ID of a single row to look up:

   *   Either as a Kiji row key, with `--entity-id=kiji=...`:

       Old deprecated Kiji row keys are specified as naked UTF-8 strings;

       New Kiji row keys are specified in JSON, as in: `--entity-id=kiji="['component1', 2, 'component3']"`.

   *   or as HBase encoded row keys specified as bytes:
       *    by default as UTF-8 strings, or prefixed as in `'utf8:encoded\x0astring'`;
       *    in hexadecimal as in `'hbase:hex:deadfeed'`;
       *    as a URL with `'url:this%20URL%00'`.

You will typically want to further constrain the data printed to the terminal with the following options.

*  `--max-versions=<int>` -
   Restrict the number of versions of each cell to display.

   The default is 1, ie. displays the latest version of each cell.

*  `--timestamp=<long>..<long>` -
   Excludes cell versions whose timestamp is outside the specified time range min..max.
   Timestamps are expressed in milliseconds since the Epoch.
   If the lower bound is unspecified, it defaults to 0 and if the upper bound is unspecified, it defaults to `Long.MAX_VALUE`.

   The default is 0.., i.e. from Epoch to `Long.MAX_VALUE`.


## Scanning a table: `scan`<a id="ref.scan"> </a>

The `kiji scan` command, unlike `kiji get`, scans _multiple_ records in the table specified by Kiji URI argument (with or without columns specified).
Each record appears as a set of cells separated from other records by blank lines.
The cells appear similar to how they do with `kiji get`.

{% highlight bash %}
$ kiji scan kiji://localhost:2181/kiji_instance1/table1/info:name,info:email
Scanning kiji table: kiji://localhost:2181/kiji_instance1/table1/
entity-id=['Olga Jefferson'] [1305851507300] info:name
                                 Olga Jefferson
entity-id=['Olga Jefferson'] [1305851507301] info:email
                                 Olga.Jefferson@hotmail.com

entity-id=['Sidney Tijuana'] [1305851507425] info:name
                                 Sidney Tijuana
entity-id=['Sidney Tijuana'] [1305851507427] info:email
                                 Sidney.Tijuana@hotmail.com
…
{% endhighlight %}


The scanned records may be further constrained by using the following options:

*  `--start-row=row-key` and `--limit-row=row-key` -
   Restrict the range of rows to scan through.
   The start row is included in the scan while the limit row is excluded.
   Start and limit rows are expressed in the same way as `--entity-id` for [`kiji get`](#ref.get).
   For example as HBase encoded rows: `--start-row='hex:0088deadbeef'` or `--limit-row='utf8:the row key in UTF8'`.

   The default is to scan through all the rows in the table.

*  `--max-rows=<int>` -
   Limits the total number of rows to display the content of.

   The default is 0 and sets no limit.

The following additional options also apply to `kiji scan`.

*  `--max-versions=<int>` -
   Restrict the number of versions of each cell to display.

   The default is 1, ie. displays the latest version of each cell.

*  `--timestamp=<long>..<long>` -
   Excludes cell versions whose timestamp is outside the specified time range min..max.
   Timestamps are expressed in milliseconds since the Epoch.
   If the lower bound is unspecified, it defaults to 0 and if the upper bound is unspecified, it defaults to `Long.MAX_VALUE`.

   The default is 0.., i.e. from Epoch to `Long.MAX_VALUE`.


## Incrementing counters: `increment`<a name="ref.increment"> </a>


The `kiji increment` command may be used to increment
(or decrement) a KijiSchema counter.

The following arguments are required:

*  `--cell=<column-uri>`       - [Kiji URI](#ref.kiji_uri) specifying a single column to increment.
*  `--entity-id=<entity>`      - Entity ID of the target row.
*  `--value=amount`            - The value to increment by.

See [`kiji ls`](#ref.ls) for how to specify entity IDs.

Setting Individual Cells: `put`<a name="ref.put"> </a>


To aid in the insertion of small data sets, debugging, and testing, the
`kiji put` command may be used to insert individual values in a Kiji table.

The following arguments are required:

*  `--target=<column-uri>`     - [Kiji URI](#ref.kiji_uri) specifying a single column to write.
*  `--entity-id=<entity>`      - Target row id (an unhashed, human-readable string).
*  `--value=<JSON value>`      - The value to insert. The value is specified as a
   JSON string according to [the Avro JSON encoding specification](http://avro.apache.org/docs/current/spec.html#json_encoding).
*  `--schema=Avro schema`      - If there is no schema attached to the column 
   or if you want to use an alternate writer schema, specify this option. While it is not 
   strictly required, you should specify this option as there are few cases where there 
   is a sensible default. The option
   value is a JSON representation of an Avro Schema. To ensure this value can contain
   escaped quoted strings, wrap the entire JSON in single quotes: `'"string"'` (that's
   single quotes around double quotes around the word string).

See [`kiji ls`](#ref.ls) for how to specify entity IDs.

The following argument is optional:

*  `--timestamp=long`          - Specifies a timestamp (in milliseconds since the Epoch) other than "now".



## Running an Application Jar with KijiSchema: `jar`<a name="ref.jar"> </a>


If your application requires KijiSchema and its dependencies, you
can use the `kiji jar` command to launch your program's
main method with KijiSchema present on the classpath.

This command requires two unlabeled arguments: the jar filename, and the
main class to run:

{% highlight bash %}
$ kiji jar myapp.jar com.pkg.MyApp [args...]
{% endhighlight %}


## Generating Sample Data: `synthesize-user-data`<a name="ref.synthdata"> </a>


In the interest of enabling quick experimentation with KijiSchema,
the `kiji synthesize-user-data` tool will
generate a number of semi-random rows for you.

The tool creates a set of rows which contain columns `info:id`, `info:name`, and
`info:email`; these are pseudo-randomly generated first and last names, with
plausible email addresses with gmail, hotmail, etc. accounts based on the
generated names. These columns can be used with mappers and reducers.

To use this tool, first create a table with the layout in
`${KIJI_HOME}/examples/synthdata-layout.xml`.
Then invoke `bin/kiji synthesize-user-data --table=<table-uri>`.
This will generate 100 rows of data.
You can create a different number of records by specifying
`--num-users=<int>`.

You can specify a different list of names with the `--name-dict=filename` argument.
