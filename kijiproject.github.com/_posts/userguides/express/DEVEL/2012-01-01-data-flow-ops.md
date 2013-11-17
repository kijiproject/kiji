---
layout: post
title: Data Flow Operations in KijiExpress
categories: [userguides, express, devel]
tags : [express-ug]
version: devel
order : 7
description: Data Flow Operations in KijiExpress.
---

Here are some common tasks done using Kiji and KijiExpress:

* Write an “importer” for each Kiji table you want to fill with source data.
* Perform some logic to manipulate the source data.
* Write the resulting data into Kiji tables.


#### Writing an Importer for the Source Data

KijiSchema provides a stock importer you can use to import JSON data.

If the stock importer isn’t appropriate for what you are doing (especially when you first
start out and are looking for something with a little less overhead), you can write your
own importer using Scala. The contents of the importer are as follows:

* Identify the source data location
* Identify the target Kiji table
* Define any functions you want to operate on the data before it is written to the target
* Define the mapping between the source data and the target columns

The fastest way to write an importer may be to copy one of the Scala importer files from a
Kiji tutorial project and modify it to meet your needs. Find an example here:

    ${KIJI_HOME}/examples/express-music/src/main/scala/org/kiji/express/music/SongMetadataimporter.scala

For a walkthrough of Scala syntax, see [Writing Basic KijiExpress Jobs](../basic-jobs).

