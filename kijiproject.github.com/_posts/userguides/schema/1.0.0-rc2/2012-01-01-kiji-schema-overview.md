---
layout: post
title: What is KijiSchema?
categories: [userguides, schema, 1.0.0-rc2]
tags : [schema-ug]
version: 1.0.0-rc2
order : 1
description: Overview.
---

## What is KijiSchema?

KijiSchema provides a simple Java API and command line interface for
importing, managing and retrieving data from HBase.


## Key Features

- Set-up HBase layouts using user-friendly tools including a DDL
- Implement HBase best practices in table management
- Use evolving Avro schema management to serialize complex data
- Perform both short-request and batch processing on data in HBase
- Import data from HDFS into structured HBase tables

KijiSchema promotes the use of entity-centric data modeling, where 
all information about a given entity, including both dimensional and
transaction data, is encoded within the same row. This approach is
particularly valuable for user-based analytics such as targeting,
recommendations, and personalization.
