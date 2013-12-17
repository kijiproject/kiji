---
layout: post
title: What is KijiMR?
categories: [userguides, mapreduce, 1.2.3]
tags : [mapreduce-ug]
version: 1.2.3
order : 1
description: Overview.
---

KijiMR allows KijiSchema users to employ MapReduce-based techniques to develop many kinds of
applications, including those using machine learning and other complex analytics.

KijiMR is organized around three core MapReduce job types: _Bulk Importers_, _Producers_ and
_Gatherers_.

 * _Bulk Importers_ make it easy to efficiently load data into Kiji tables from a variety of
   formats, such as JSON or CSV files stored in HDFS.
 * _Producers_ are entity-centric operations that use an entity's existing data to generate new
   information and store it back in the entity's row. One typical use-case for producers is to
   generate new recommendations for a user based on the user's history.
 * _Gatherers_ provide flexible MapReduce computations that scan over Kiji table rows and output
   key-value pairs. By using different outputs and reducers, gatherers can export data in a variety
   of formats (such as text or Avro) or into other Kiji tables.

Finally, KijiMR allows any of these jobs to combine the data they operate on with external
_KeyValueStores_. This allows the user to join data sets stored in HDFS and Kiji.

Unlike KijiSchema, where the classes most relevant to application developers were usually concrete,
these core job types exist in KijiMR as abstract classes (such as
[`KijiProducer`]({{site.api_mr_1_2_3}}/produce/KijiProducer.html)). It is typically up to the
application developer to subclass the appropriate class in their application and implement their
application's analysis logic in a few methods (such as
[`KijiProducer`]({{site.api_mr_1_2_3}}/produce/KijiProducer.html)'s `produce()` and
`getDataRequest()`). They can then point the job at the appropriate Kiji table using either the
`kiji` command line tools or programmatically using one of the framework's JobBuilders (such as
[`KijiProduceJobBuilder`]({{site.api_mr_1_2_3}}/produce/KijiProduceJobBuilder.html)) that make
launching these jobs easy.  Kiji can also record metadata about jobs run using KijiMR to provide
a historical view.

The KijiMR Library provides a growing repository of implemented solutions to many common use-cases.
These solutions can be used directly or as example code. Both KijiMR and the KijiMR Library are
included with distributions of Kiji BentoBox.

In the sections of this guide that follow, the core job types will be explained in greater detail.
Motiviation, example code snippets, and (where appropriate) a description of reference
implementations in the KijiMR Library will be given for each. This guide also contains an in-depth
description of how to use  _KeyValueStores_ to expose side-data stored in HDFS and Kiji through a
consistent interface to your MapReduce jobs. Finally, this guide contains a description of the
command line tools included with KijiMR and facilities that make it easier to test KijiMR
application code.

## Using KijiMR in Your Project

You will need to include KijiMR as a dependency in your project. If you're
using Maven, this can be included as follows:

    <dependency>
      <groupId>org.kiji.mapreduce</groupId>
      <artifactId>kiji-mapreduce</artifactId>
      <version>1.2.3</version>
      <scope>provided</scope>
    </dependency>

If you want to use bulk importers or other classes in the KijiMR Library, you will
need to include:

    <dependency>
      <groupId>org.kiji.mapreduce.lib</groupId>
      <artifactId>kiji-mapreduce-lib</artifactId>
      <version>1.1.3</version>
      <scope>provided</scope>
    </dependency>

Different versions of Hadoop are incompatible with one another at a binary level. The version
of KijiMR specified above can be used with Hadoop 2-based systems (e.g., CDH 4). You can use
KijiMR with a Hadoop 1-based system by declaring a dependency on a different build of KijiMR
without changing your source code. Just use the following dependency instead:

    <dependency>
      <groupId>org.kiji.mapreduce</groupId>
      <artifactId>kiji-mapreduce</artifactId>
      <version>1.2.3</version>
      <classifier>hadoop1</classifier>
      <scope>provided</scope>
    </dependency>

You can also explicitly specify `<classifier>hadoop2</classifier>` if you'd like, although
this is the default. If you use the `bin/kiji` command to launch your Kiji application,
it will automatically detect which version of Hadoop is present and use the appropriate
build of KijiMR at runtime without any changes to your code.

* You will also need a dependency on KijiSchema. See [the KijiSchema
  documentation]({{site.userguide_schema_1_3_4}}/kiji-schema-overview/) for this information.
* You'll probably need to configure your Maven `settings.xml` to locate these dependencies.
  See [Getting started with Maven](http://www.kiji.org/get-started-with-maven)
  for more details.
