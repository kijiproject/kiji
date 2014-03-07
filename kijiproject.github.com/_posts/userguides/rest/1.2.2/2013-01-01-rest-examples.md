---
layout: post
title: KijiREST Examples
categories: [userguides, rest, 1.2.2]
tags: [rest-ug]
order: 5
version: 1.2.2
description: Examples
---

The following examples show basic GET requests using cURL to help you get used to using KijiREST.
You can also execute requests by inserting them in the address bar of your browser.

### Get Resources

A GET on the top-level entry point for KijiREST ("v1") returns the two resources available
at that level ("version", "instances") and the service name ("KijiREST"). This example
assumes the default KijiREST configuration using port 8080:

    $ curl http://localhost:8080/v1
    {
     "resources": [ "version", "instances"],
     "service": "KijiREST"
    }

### Get Version

A GET on the version resource returns version metadata for the cluster, the REST protocol,
and Kiji:

    $ curl http://localhost:8080/v1/version
    {
       "kiji-client-data-version": "system-2.0.0",
       "rest-version":"1.2.2",
       "kiji-software-version": "1.3.7"
    }

### Get Instances

A GET on the instances resource returns the names and URIs for all instances in the Kiji cluster
made visible to REST clients (as specified in the configuration.yml file):

    $ curl http://localhost:8080/v1/instances
    [
     {"name": "default", "uri": "/v1/instances/default"},
     {"name": "dev_instance", "uri": "/v1/instances/dev_instance"},
     {"name": "prod_instance", "uri": "/v1/instances/prod_instance"}
    ]

### Get Tables

A GET on the tables resource in an instance called "default" returns
the names and URIs for all tables defined for the instance:

    $ curl http://localhost:8080/v1/instances/default/tables
    [
     {"name": "users", "uri": "/v1/instances/default/tables/users"},
     {"name": "products", "uri": "/v1/instances/default/tables/products"}
    ]

From here you can access rows for a specific table:

    $ curl http://localhost:8080/v1/instances/default/tables/users
    ...
    $ curl http://localhost:8080/v1/instances/default/tables/users/rows/
    ...
