---
layout: post
title: What is KijiMR?
categories: [userguides, mapreduce, 1.0.0-rc4]
tags : [mapreduce-ug]
version: 1.0.0-rc4
order : 1
description: Overview.
---

Kiji MapReduce is the second major component in the Kiji ecosystem for Apache Hadoop and HBase after KijiSchema.

Kiji MapReduce includes APIs to build MapReduce jobs that read and write data stored in Kiji tables, bringing MapReduce-based analytic techniques to a broad base of KijiSchema users, to support applications such as machine learning and analysis.

KijiMR is organized around three core MapReduce job types: _Bulk Importers_, _Producers_ and _Gatherers_. These may read from external data sources through _KeyValueStores_.

 * _Bulk Importers_ make it easy to efficiently load data into Kiji tables from a variety of formats, such as JSON or CSV files in HDFS clusters.
 * _Producers_ are entity-centric operations that use a entity's existing data to generate new information and store it back in the entity's row. One typical use-case for producers is to generate new recommendations for a user based on the user's history.
 * _Gatherers_ provide flexible MapReduce computations that scan over Kiji table rows and output key-value pairs. By using different outputs and reducers, Gatherers can export data to external data stores such as files in HDFS clusters in a variety of formats (such as text or avro) or even into HFiles to efficiently load data into other Kiji Tables.

Finally, Kiji MapReduce allows any of these jobs to combine their data with external _KeyValueStores_ as a way to join data sets store in HDFS, in Kiji, etc.

Unlike Kiji Schema, where the classes most relevant to application developers were usually concrete, these core job types exist in Kiji MapReduce as abstract classes (such as `KijiProducer`). It is typically up to the application developer to subclass the appropriate class in their application and implement their application's analysis logic in a few methods (such as `KijiProducer`'s `produce()` and `getDataRequest()`). They can then point the job at the appropriate Kiji table using either the `kiji` command-line tools or programmatically using one of the framework's JobBuilders (such as `KijiProduceJobBuilder`) that make launching these jobs easy.  Jobs within Kiji can record relevant job related metadata to provide a historical view.

To provide a starting point and some useful precanned solutions, the complementary Kiji MapReduce Library provides a growing repository of sample implementations that can be used directly or as example code. Both Kiji MapReduce and the Kiji MapReduce Library are included in the Kiji Bento Box distribution.

In the sections of this guide that follow, the core job types will be explained in greater detail, including motivation, example code snippets, and where appropriate, a description of the Kiji MapReduce Library's reference implementations. This guide also contains an in-depth description of how to use Kiji MapReduce's _KeyValueStores_ to expose data stored in different places and formats (such as HDFS or KijiTables) through a consistent interface to your MapReduce jobs. For power users, we've included instructions on how to use the `KijiMapReduceJobBuilder` class and `kiji mapreduce` command to build and launch arbitrary MapReduce jobs using Kiji mappers and reducers to perform flexible data transformation.

Finally this guide contains a description of the command-line tools included with Kiji MapReduce and facilities that make it easier to test Kiji MapReduce application code.