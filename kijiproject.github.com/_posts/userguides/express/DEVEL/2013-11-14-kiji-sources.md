---
layout: post
title: KijiExpress Sources
categories: [userguides, express, devel]
tags : [express-ug]
version: devel
order : 6
description: KijiExpress Sources.
---

# KijiExpress Sources

Scalding provides the `Source` abstraction for reading and writing data as a part of jobs. In
addition to the Scalding `Source`s for reading and writing data to HDFS files, Express provides the
`KijiSource` for reading and writing data to Kiji tables, and `HFileKijiSource`s for writing data to
HFiles.  The following sections will explore how to use `KijiSource` and `HFileKijiSource` as part
of flows.

## KijiSource

The `KijiSource` class is used by Express to read and write to on-line Kiji tables as part of
Express flows.  Users should use the `KijiInput` and `KijiOutput` classes for specifying how
`KijiSource` should read from and write to Kiji tables, respectively.

### KijiInput

`KijiInput` specifies how data should be read from a KijiTable and converted into tuples in a flow.
Each row from the Kiji table will populate a single tuple, but the fields of the tuple and whether
the fields hold a column or an entire column family is configurable.  Additionally, `KijiInput`
allows specifying a specific timerange of cells to be read from the table.

The tuples that result from using a `KijiInput` contain a field per input column in the map.  Each
field will contain a [`Seq`](http://www.scala-lang.org/api/current/index.html#scala.collection.Seq)
of `Cell` objects.  A Scala `Seq` is very similar to List, and it supports many higher-order Scala
operations such as `map`, `filter`, and `fold`.  `Cell` objects are simple data containers which
hold the `family`, `qualifier`, `version` (or timestamp), and `datum`.  The `datum` field holds the
value of the cell, and can be accessed with `myCell.datum`.

The basic KijiInput syntax looks like this.  More details on configuration options follow.

#### Specifying a Timerange (Optional)

You can specify an optional time range on a **KijiInput**.  (Note that the time range is applied to
the entire input request and not a single column. You can further control how many versions of an
individual column are returned by using maxversions described further ahead.).  This time range that
limits the data returned from all the columns you request to that time range.  The default time
range is `All`, which imposes no limits on the data from the columns of the table.

Here are the provided constructors for a TimeRange:

    timeRange = Before(1000L)
    timeRange = After(1000L)
    timeRange = Between(1000L, 2000L)
    timeRange = All

The time range is an optional parameter to KijiInput, so your construction will look something like
what follows:

    KijiInput(tableUri = “kiji://localhost:2181/default/users”,
        timeRange =Before(1000L),
        Map(
            QualifiedColumnInputSpec(“userinfo:email”, maxVersions = 2) -> ‘email,
            ColumnFamilyInputSpec(“purchases”, maxVersions = all) -> ‘purchases))

Note that this means the `’email` field will contain only the data in the `userinfo:email` column
that has timestamps before 1000L.  Since we only requested a maximum of 2 versions of that column,
this means we get the most recent 2 versions of the user’s email before time 1000, even if there are
more after time 1000.  This also means the `’purchases` field contains all the purchases that fall
within the specified `timeRange` for this `KijiInput`, not all purchases that are in the users
table.

#### Specifying a Column to Field Mapping (Required)

`KijiInput` takes a map of tuple field name to `ColumnInputSpec` as an argument to the constructor
function. This map defines the Kiji columns that will be read into specific fields of the flow.

The simplest KijiInput syntax looks like this:

    KijiInput(tableUri = "kiji://localhost:2181/default/users",
        "info:name" -> 'name, "purchases" -> 'purchases)

This constructs a KijiInput that reads from the specified table, the specified columns "info:name"
and "purchases" and puts the contents of those columns into the fields `'name` and `'purchases`
respectively.

However, to specify further options on the columns that you request data from, you must use the
following syntax:

    KijiInput(tableUri = "kiji://localhost:2181/default/users",
        Map(QualifiedColumnInputSpec("info", "name") -> 'name,
            ColumnFamilyInputSpec("purchases") -> 'purchases))

`ColumnInputSpec` is a trait, or interface, with two implementations: `QualifiedColumnInputSpec` and
`ColumnFamilyInputSpec`. `QualifiedColumnInputSpec` is used to specify a single Kiji column should
be read into a single field in each tuple.  The `ColumnFamilyInputSpec` is used to specify an entire
column family should be read into a single field in each tuple, it is particularly useful for
reading map-type column families.

In both cases, additional options on how to read the column exist, such as number of versions,
filters, paging, and schema.

##### maxVersions

    import org.kiji.express.flow
    maxVersions = 2
    maxVersions = all

`maxVersions` is an Integer specifying the number of versions of this column to be in the tuple.  By
  default, this is 1, so you will only get the latest version within the time range of the entire
KijiInput request.

##### schemaSpec

This specifies the Avro schema to read the data in this column with.  Here are the
options:

* `SchemaSpec.Specific(classOf[MySpecificRecordClass])`: used when you have a specific class
  that has been compiled by Avro.  `MySpecificRecordClass` must extend `
`org.apache.avro.SpecificRecord`
* `SchemaSpec.Generic(myGenericSchema)`: If you don’t have the specific class you want to use to
  read, you can construct a generic schema and use it as the reader schema.
* `SchemaSpec.Writer`: used when you want to read with the same schema that the data was written
  with.  This is the default if you don’t specify any `SchemaSpec` for reading.
* `SchemaSpec.DefaultReader`: specifies that the default reader for this column, stored in the
  table layout, should be used for reading this data.  If you use this option, first make sure
the column in your Kiji table has a default reader specified.

##### filters

By default, no filter is applied, but you can specify your own.  Only data that pass
these filters will be requested and populated into the tuple.  Two column filters are currently
provided: `ColumnRangeFilterSpec` and `RegexQualifierFilterSpec`.  Both of these filter the data
returned from a ColumnFamilyInputSpec by qualifier in some way.  These filters can be composed with
`AndFilterSpec` and `OrFilterSpec`.  Filters are implemented via HBase filters, not on the client
side, so they can cut down on the amount of data transferred over your network.

* `ColumnRangeFilterSpec(minimum = “c”, maximum = “m”, minimumIncluded = true, maximumIncluded =
  false)`:  Specifies a range of qualifiers for the cells that should be returned.  In this
example, it returns all data from all columns with qualifiers “c” and later, up to but not including
“m”.  All of the parameters are optional, so you can write `ColumnRangeFilterSpec(minimum = “m”,
minimumIncluded = true)` to specify columns with qualifiers “m” and later.
* `RegexQualifierFilterSpec(“http://.*”)`: Specifies a regex for the qualifier names that you
  want data from.  In this example, only data from columns whose qualifier names start with
“http://” are returned.
* `AndFilterSpec(List(mRegexFilter, mQualifierFilter))`: The `AndFilterSpec` composes a list of
  `FilterSpec`s, returning only data from columns that satisfy all the filters in the
`AndFilterSpec`.
* `OrFilterSpec(List(mRegexFilter, mQualifierFilter))`: Is analagous to `AndFilterSpec`, but
  returns only data from columns that satisfy at least one of the filters.  `OrFilterSpec` and
`AndFilterSpec` can themselves be composed.

###### paging

    paging = PagingSpec.Cells(10)

Specifies the number of cells per page.  By default, `paging = PagingSpec.Off`, which disables
paging.  With paging disabled, all cells from the specified column will be loaded into memory at
once.  Sometimes, this will be too much.  In these cases you can specify `paging =
PagingSpec.Cells(10)` to load only 10 cells at a time.  See “A Note on Paging” below for more
information about usage and pitfalls of paging.

#### A Note on Paging

If paging is enabled on a `ColumnInputSpec`, then the resulting fields will contain a `Seq` with
lazy evaluation semantics, that is, any transformations on the collection will only retrieve the
requested `Cell`s from HBase when they are needed.  This has several important performance
implications:

  * Cells will only be retrieved from HBase as they are needed, so if a job does not use the cells,
    no further paging will be triggered.
  * Full evaluation of the `Seq` can be triggered by performing an operation over entire `Seq`, such
    as `map` or `filter`.
  * Holding a reference to the `Seq` and triggering a full evaluation **will** result in the full
    sequence of `Cell`s being in heap at once.  To avoid this, do not hold a reference to a `Seq`
which will be fully evaluated.  For example, the following shows the correct way to filter a `Seq`
of `Cell`s which may be bigger than the heap size down to a more manageable collection without
running out of memory.

  * In particular, sorting a `Seq` will entirely materialize the `Cell`s in memory, thus if the
    `ColumnInputSpec` specifies reading more values from the Kiji column or column family than will
fit in heap, an `OutOfMemoryException` will occur.

#### The Entity Id Field

The `’entityId` field of each tuple is automatically populated by the entity ID of the corresponding
row in the Kiji table.  You can treat it as any other field.

