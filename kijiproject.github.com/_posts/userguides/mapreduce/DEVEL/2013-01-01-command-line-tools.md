---
layout: post
title: Command Line Tools
categories: [userguides, mapreduce, DEVEL]
tags : [mapreduce-ug]
version: DEVEL
order : 7
description: Command Line Tools.
---

The KijiMR framework provides command line tools to submit and monitor MapReduce jobs.

## Overview of available tools

KijiMR provides the following command line tools:
*   `kiji bulk-import`: runs a bulk-importer job that imports data from an external data source into a Kiji table.
*   `kiji produce`: runs a producer job.
*   `kiji gather`: runs a gatherer job that processes the rows from a Kiji table and writes files, optionally using a reducer.
*   `kiji bulk-load`: bulk-loads the HFile output of a job into a KijiTable.
*   `kiji mapreduce`: runs an arbitrary Map/Reduce job using Kiji mappers and reducers.
*   `kiji job-history`: retrieve information about jobs previously run through kiji if enabled.

## Using common-flags

Kiji commands bulk-import, produce and gather all recognize the following flags:

*   `--kvstores=/path/to/store-specifications.xml`: specifies the path of XML file describing the key/value stores used by the job.

*   `--lib=/path/to/jars-directory/`: specifies a directory of jar files that contain user code.


*   `--start-row=` and `--limit-row=`:
    Restrict the range of rows to scan through.
    The start row is included in the scan while the limit row is excluded.
    Start and limit rows are expressed in the same way as `--entity-id` for [`kiji get`]({{site.userguide_schema_DEVEL}}/tool-reference/#ref.get).
    For example as HBase encoded rows: `--start-row='hex:0088deadbeef'` or `--limit-row='utf8:the row key in UTF8'`.


Bulk importers must specify the name of the class providing the bulk-import logic:

*   `--importer=java.package.BulkImporterClassName`: specifies the [`KijiBulkImporter`]({{site.api_mr_DEVEL}}/bulkimport/KijiBulkImporter.html) class to use.

Producers must specify the name of the class providing the producers logic:

*   `--producer=java.package.ProducerClassName`: specifies the [`KijiProducer`]({{site.api_mr_DEVEL}}/produce/KijiProducer.html) class to use.

Gatherer must specify the name of the class providing the gathering logic, and optionally a reducing logic:

*   `--gatherer=java.package.GathererClassName`: specifies the [`KijiGatherer`]({{site.api_mr_DEVEL}}/gather/KijiGatherer.html) class to use.

*   `--combiner=java.package.CombinerClassName`: optionally specifies a Combiner class to use.

*   `--reducer=java.package.ReducerClassName`: optionally specifies a Reducer class to use.

## Input/output formats

Jobs inputs and outputs are specified with the following flags:

*   `--input=...`: specifies the input of the job.

    The job input specification is formatted as `--input="format= ..."`.
    Kiji recognizes the following job input formats:

    * `avro`: job input is an Avro container file, each input record is a pair (Avro key, NullWritable).

    * `avrokv`: job input is an Avro container file for key/value generic records.

    * `htable`: job input is an HTable, each input record is a pair (HBase row key, HBase Result).
      The address of the HBase cluster is pull from the local job configuration (ie. from the HBase configuration available on the classpath).
      Example: `--input="format=htable htable=htable-table-name"`.

    * `kiji`: job input is a Kiji table, each input record is a pair (row entity ID, KijiRowData).
      Example: `--input="format=kiji table=kiji://.env/default/input_table"`.

    * `seq`: job input is a Hadoop sequence file.
      Example: `--input="format=seq file=hdfs://dfsmaster:9000/path/to/sequence-file/"`.

    * `small-text-files`: job input is a set of small text files, each input record is a pair (text file path, text file content).
      Example: `--input="format=small-text-files file=hdfs://dfsmaster:9000/path/to/text-files/"`.

    * `text`: job input is a text file, each input record is a pair (position in the text file, line of text).
      Example: `--input="format=text file=hdfs://dfsmaster:9000/path/to/text-file/"`.

    * `xml`: job input is an xml file, each input record is all data between a specified open and close tag.
      Example: `--input=format=xml file=hdfs://dfsmaster:9000/path/to/xml-file/"`.

*   `--output=...`: specifies the output of the job.

    The job specification is formatted as: `--output="format= nsplits=N ..."`.
    Kiji recognizes the following job output formats:

    * `avro`: job output is an Avro container file, each output record is a pair (Avro key, NullWritable).

    * `avrokv`: job output is an Avro container file with key/value generic records.

    * `hfile`: job output is an HFile that will be bulk-loaded into a Kiji table.
      Example: `--output="format=hfile nsplits=10 table=kiji://.env/default/target_table file=hdfs://dfsmaster:9000/path/to/hfile/"`.

    * `kiji`: job output is a Kiji table.
      The use of this job output should be limited to development only and should not be used in production as it may incur high load on the target HBase cluster. The exception is producers, whose input and output must be the same kiji table.
      Example: `--output="format=kiji nsplits=10 table=kiji://.env/default/target_table"`.

    * `map`: job output is a Hadoop map file.
      Example: `--output="format=map nsplits=10 file=hdfs://dfsmaster:9000/path/to/map-file/"`.

    * `seq`: job output is a Hadoop sequence file.
      Example: `--output="format=seq nsplits=10 file=hdfs://dfsmaster:9000/path/to/sequence-file/"`.

    * `text`: job output is a text file; each (key, value) record is written to a text file with a separator
      (via the configuration parameter "mapred.textoutputformat.separator", which defaults to TAB) and a new line.
      Example: `--output="format=text nsplits=10 file=hdfs://dfsmaster:9000/path/to/text-file/"`.
