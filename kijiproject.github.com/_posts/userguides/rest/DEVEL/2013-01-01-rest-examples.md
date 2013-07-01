---
layout: post
title: KijiREST Examples
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 4
version: devel
description: Examples
---

The following examples show basic GET requests using cURL to help you get used to using KijiREST. 
You can also execute requests by inserting them in the address bar of your browser.

### Get Resources
A GET to the top-level entry point for KijiREST (“v1”) returns the two resources available 
at that level (“version”, “instances”) and the service name (“KijiREST”). This example 
assumes the default KijiREST configuration using port 8080:

    $ curl http://localhost:8080/v1
    {
     "resources": [ "version", "instances"],
     "service": "KijiREST"
    }

### Get Version

A GET to the version resource returns version metadata for the cluster, the REST protocol, 
and Kiji:

    $ curl http://localhost:8080/v1/version
    {
     "kiji-client-data-version": {"majorVersion": 1, "minorVersion": 0, "revision": 0, "protocolName": "system"},
     "rest-version": "0.1.0",
     "kiji-software-version": "0.1.0"
    }

### Get Instances

A GET to the instances resource returns the names and URIs for all instances defined for 
the cluster:

    $ curl http://localhost:8080/v1/instances
    [
     {"name": "default", "uri": "/v1/instances/default"},
     {"name": "dev_instance", "uri": "/v1/instances/dev_instance"},
     {"name": “prod_instance", "uri": "/v1/instances/prod_instance"}
    ]

### Get Tables

A GET to the tables resource in an instance called “default” returns 
the names and URIs for all tables defined for the instance:

    $ curl http://localhost:8080/v1/instances/default/tables
    [
     {"name": "users", "uri": "/v1/instances/default/tables/users”},
     {"name": "products", "uri": "/v1/instances/default/tables/products”}
    ]

From here you can access rows for a specific table:

    $ curl http://localhost:8080/v1/instances/default/tables/users
    …
    $ curl http://localhost:8080/v1/instances/default/tables/users/rows/
    …
