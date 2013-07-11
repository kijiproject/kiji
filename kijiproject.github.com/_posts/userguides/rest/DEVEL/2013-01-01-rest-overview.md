---
layout: post
title: What is KijiREST?
categories: [userguides, rest, devel]
tags: [rest-ug]
version: devel
order: 1
description: Overview.
---

KijiREST provides a REST (Representational State Transfer) interface for Kiji, allowing
applications to interact with a Kiji cluster over HTTP by issuing the four standard actions:
GET, POST, PUT, and DELETE.

Use this interface to pull data from Kiji tables into other applications, such as web pages
making use of predictive modeling data to customize user interactions.

There's an example of a KijiREST client as a RubyGem in the GitHub project
[kiji-rest-client](https://github.com/kijiproject/kiji-rest-client) which can be installed
by executing:

    gem install kijirest-client

### REST Resources
Every REST request is parametrized by a resource path that uniquely identifies a server-side
resource. Other optional parameters relevant to the action are specified in the query and
body sections of the request. For example, to retrieve row data from the 'users' table, a
GET request might look like the following:

    GET /v1/instances/dev_instance/tables/users/rows/c51ce410c124a10e0db5e4b97fc2af39[?...]

All returned data is modeled as JSON. The appropriate MIME types to accept are
application/json for lists, metadata, and all non-row-data.

To make use of the interface, you need to know how to identify the KijiREST resources
and what operations can be applied to the resources. This guide describes these
items as well as how to setup and run KijiREST in your development and production environments.

### KijiREST Open Source Development Project

The [kiji-rest](https://github.com/kijiproject/kiji-rest) project will evolve over time to
ensure maximum user and developer benefit.

#### Development Status
The Kiji project encourages the use of Java annotations for indicating the stability of
API components. The levels of stability are as follows:

*  By default, unlabeled classes should be assumed to be Experimental.
*  __Stable__ APIs are guaranteed to change only in binary-compatible ways within a major version (e.g., all 1.x.x versions)
*  __Evolving__ APIs may change in binary-incompatible ways between minor versions (e.g., from 1.4.1 to 1.5.0) of the software. In particular, new features may be added in one minor version and altered or removed in the next.
*  __Experimental__ APIs may change at any time.
*  __Private__ APIs should all be considered Experimental.
