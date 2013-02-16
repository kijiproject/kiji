---
layout: post
title: Data Model
categories: [userguides, schema, 1.0.0-rc3]
tags : [schema-ug]
order : 2
version: 1.0.0-rc3
description: KijiSchema's underlying data model.
---

KijiSchema’s data model is an extension of HBase’s columnar storage
model. Like HBase, KijiSchema’s tables contain many rows; each row
contains many columns. A given *cell* identified by a row and column can
hold many different timestamped values, representing the changing values
of the cell over time. Columns are grouped together into column
*families*, which provide namespacing for columns that store related
data. Families are grouped into *locality groups*, which help store
related families physically close to one another.

> What Kiji calls a "locality group", HBase calls a "family". The Kiji
> column "family" allows you to choose the logical grouping and namespace
> of columns separately from the physical configuration of how the data is stored.

As in HBase, rows in Kiji tables can have an arbitrary number of
columns. Individual rows may have hundreds or thousands (or more)
columns. Different rows may not necessarily have the same set of
columns.

Unlike HBase, each cell in a Kiji table has a schema associated with it.
Schemas in KijiSchema are versioned. The schema and layout system is
described in greater detail in [Managing Data]({{site.userguide_url}}managing-data)

### Entity-Centric Data Model
KijiSchema’s data model is *entity-centric*. Each row typically holds
information about a single *entity* in your information scheme. As an
example, a consumer e-commerce web site may have a row representing each
user of their site. The entity-centric data model enables easier analysis
of individual entities. For example, to recommend products to a user,
information such as the user’s past purchases, previously viewed items,
search queries, etc. all need to be brought together. The entity-centric
model stores all of these attributes of the user in the same row,
allowing for efficient access to relevant information.

The entity-centric data model stands in comparison to a more typical
log-based approach to data collection. Many MapReduce systems import log
files for analysis. Logs are *action-centric*; each action performed by
a user (adding an item to a shopping cart, checking out, performing a
search, viewing a product) generates a new log entry. Collecting all the
data required for a per-user analysis thus requires a scan of many logs.
The entity-centric model is a “pivoted” form of this same information.
By pivoting the information as the data is loaded into KijiSchema, later
analysis can be run more efficiently, either in a MapReduce job
operating over all users, or in a more narrowly-targeted fashion if
individual rows require further computation.
