---
layout: post
title: Developing Models in KijiExpress
categories: [userguides, express, devel]
tags : [express-ug]
version: devel
order : 7
description: Developing Models in KijiExpress.
---
##DRAFT##

### Entity-Centric Data Modeling

Kiji’s data model is entity-centric. Each row typically holds information about a single
entity in your information scheme. As an example, a consumer e-commerce web site may have
a row representing each user of its site. The entity-centric data model enables easier
analysis of individual entities. For example, to recommend products to a user, information
such as the user’s past purchases, previously viewed items, search queries, etc. all
need to be brought together. The entity-centric model stores all of these attributes of
the user in the same row, allowing for efficient access to relevant information.

The entity-centric data model stands in comparison to a more typical log-based approach to
data collection. Many MapReduce systems import log files for analysis. Logs are
action-centric; each action performed by a user (adding an item to a shopping cart,
checking out, performing a search, viewing a product) generates a new log entry.
Collecting all the data required for a per-user analysis thus requires a scan of many logs.
The entity-centric model is a “pivoted” form of this same information. By pivoting the
information as the data is loaded into a Kiji table, later analysis can be run more
efficiently, either in a MapReduce job operating over all users, or in a more
narrowly-targeted fashion if individual rows require further computation.
