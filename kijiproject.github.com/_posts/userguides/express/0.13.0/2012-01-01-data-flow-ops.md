---
layout: post
title: Data Flow Operations in KijiExpress
categories: [userguides, express, 0.13.0]
tags : [express-ug]
version: 0.13.0
order : 7
description: Data Flow Operations in KijiExpress.
---

## Data Types in a Kiji Pipe

Once a user has used KijiInput to read in a Kiji table, the resulting pipe will contain a tuple for
every row from the table.

### The Entity Id Field

The `’entityId` field of each tuple is automatically populated by the entity ID of the corresponding
row in the Kiji table.  You can treat it as any other field. The type of the row key is
[EntityId]({{site.api_express_0_13_0}}/flow/EntityId.html).

### Named fields

Each tuple will also contain the field names specified in the ColumnInputSpec map. For example,
consider the input below:

{% highlight scala %}
    KijiInput(tableUri = "kiji://localhost:2181/default/users",
        Map(QualifiedColumnInputSpec("info", "name") -> 'name,
            ColumnFamilyInputSpec("purchases") -> 'purchases))
{% endhighlight %}

Irrespective of whether the column is from a Group Type or Map Type family, and whether it contains
a single value or a time series, the resulting tuple field will be a
[Seq](http://www.scala-lang.org/api/2.9.2/index.html#scala.collection.Seq) of
[FlowCell]({{site.api_express_0_13_0}}/flow/org.kiji.express.flow.FlowCell$.html)s.

A `FlowCell` is a container for data from a Kiji table cell. It contains some datum tagged with a
column family, column qualifier, and version.

You can access the data stored within a flow cell as shown below:

{% highlight scala %}
   // Extracts the data stored within cell.
   val myData: T = cell.datum

   // Extracts the family, qualifier, and version of the cell.
   val myFamily: String = cell.family
   val myQualifier: String = cell.qualifier
   val myVersion: Long = cell.version
{% endhighlight %}

The type of the datum depends on the schema of the Kiji cell. In the example above, if purchases was
a String, the resulting field `'purchases` would contain a Seq[FlowCell[String]]. If it was an generic
Avro Record, this would be Seq[FlowCell[GenericRecord]].

If the value for a particular column is missing for some row, an empty Seq is returned.

By default, this sequence is sorted by version, as is the default in HBase.

Calling `sorted` sorts this based on the datum in the `FlowCell` by default. If the datum has a
complex type such as an Avro Record, you will need to provide an `Ordering` for it, however, it
should just work for primitive types.

To sort by any other dimension, you may call `sorted` and provide one of the following orderings:

* versionOrder
* qualifierOrder

{% highlight scala %}
pipe
    .map(‘purchases -> (‘sortedPurchases, ‘sortedByQualifier)) {
      purchases: Seq[FlowCell[String]] => (purchases.sorted, purchases.sorted(qualifierOrder))
    }
{% endhighlight %}

## KijiExpress extensions

Express pipelines extend Scalding pipelines by adding functionality useful for authoring jobs which
interact with Kiji tables. The majority of operations on Express pipelines are identical to
Scalding.  Documentation for Scalding pipelines can be found in the [Scalding Field
API](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference).  The following sections
will explore the extensions that Express introduces in order to make working with Kiji tables
easier.

### First-class Avro Support

Scalding provides
[`pack`](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference#wiki-pack) and
[`unpack`](https://github.com/twitter/scalding/wiki/Fields-based-API-Reference#wiki-unpack) (and
corresponding `packTo` and `unpackTo`) for converting Scalding fields into and out of Java bean-like
objects.  `pack` and `unpack` have been improved in Express to allow packing and unpacking Avro
compiled specific records.

For example, suppose you have a compiled Avro record class, `SongCount` which contains two fields:
`song_id` of type `String`, and `count` of type `long`.  Additionally, you have a pipeline
containing tuples with fields `'song_id` and `'count` containing values of the appropriate type.
`pack` can be used to create an instance of `SongCount` for each tuple.

{% highlight scala %}

  val songCounts = pipe.pack[SongCount](('song_id, 'count) -> 'record)

{% endhighlight%}

Similarly, `unpack` may be used to extract fields from an Avro record into a tuple.

{% highlight scala %}

  val songCountFields = songCounts.unpack[SongCount]('record -> ('song_id, 'count))

{% endhighlight%}

Note, as with Scalding's `pack` and `unpack`, tuple field names must match the Avro record field names.

### Generic Avro Records

Express also provides built-in support for packing and unpacking generic Avro records.  `unpack` and
`unpackTo` work seamlessly with generic records by specifiying
`[GenericRecord](http://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecord.html)`
as the class to be unpacked.

{% highlight scala %}

  val unpackedFields = pipe.unpack[GenericRecord]('record -> ('some_field, 'other_field))

{% endhighlight%}

Avro requires that a [schema specification](http://avro.apache.org/docs/current/spec.html) be
provided when creating a generic record.  Express provides the `packGenericRecord` and
`packGenericRecordTo` operations for easily creating generic records as part of an Express flow.
These operations are analogous to `pack` and `packTo`, except they take an Avro
`[Schema](http://avro.apache.org/docs/current/api/java/org/apache/avro/Schema.html)` object as an
argument in place of a type parameter.

{% highlight scala %}

  val songCountSchema: Schema = ...
  val genericSongCounts = pipe.packGenericRecord(('song_id, 'count) -> 'record)(songCountSchema)

{% endhighlight%}
