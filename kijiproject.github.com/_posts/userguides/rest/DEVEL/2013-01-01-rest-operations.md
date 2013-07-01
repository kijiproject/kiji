---
layout: post
title: KijiREST Operations
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 3
version: devel
description: Operations
---

The HTTP operations supported by the KijiREST API are the following:

* /v1

* /v1/version	[GET](#version-get)

* /v1/instances	[GET](#instances-get)

* /v1/instances/&lt;instance&gt;	[GET](#instance-get)

* /v1/instances/&lt;instance&gt;/tables	[GET](#tables-get)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;	[GET](#table-get)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/entityId	[GET](#entityID-get)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows	[GET](#rows-get), [POST](#rows-post)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows/&lt;row&gt;	[GET](#row-get), [PUT](#row-put)

The following sections describe these operations in two groups:

* [Operations on rows](#ops-on-rows)
* [Operations on non-row resources](#ops-on-non-rows)

<a id="ops-on-rows"> </a>
## Operations on Rows

Entity IDs serve as resource endpoints for both tables and rows.

<a id="entityID-get"> </a>
### Entity ID: GET

Given a list of components that comprise a table’s entity ID, a GET request returns the
hexadecimal representation (in ASCII) of the corresponding HBase row key:

    GET /v1/instances/<instance>/tables/<table>/entityId

#### Parameters:

*  `eid` (single, required) - JSON array of the the components of the entity ID in order
of their definition in the layout of the given table.

Note: Each component value from the passed-in string will be properly cast to the right
type as per the schema definition of the row key.

#### Examples:

Get one whole row as indicated by the entity ID. In this example, the row comes from the
“roster” table in the “training” instance.

    GET /v1/instances/training/tables/roster/entityId?eid=["Seleukos","Nikator"]

<a id="row-get"> </a>
### Row Hex Entity ID: GET

Retrieves a single JSON-encoded row from a Kiji table:

    GET /v1/instances/<instance>/tables/<table>/rows/<hexEntityId>

#### Parameters:

* `cols` (single, optional, default=`*`) - A comma-separated list of column family or column
        family:qualifier to return. Specify `*` for all columns. Specifying just a column
        family will yield values for all column family and qualifiers.

* `versions` (single, optional, default=1) - An integer representing the number of versions
        per cell to return for the given row.

* `timerange` (single, optional) - A string denoting the time range
        to return. Specify the time range `min..max` where `min` and `max` are the ms since UNIX epoch.
        `min` and `max` are both optional; however, if a value is provided for this parameter,
        at least one of min/max must be present.)

#### Examples:

Get one whole row.

    GET /v1/instances/training/tables/roster/rows/85a9b957350d9dbf7218632dd53b8a41

Get only a few columns.

    GET /v1/instances/training/tables/roster/rows/85a9b957350d9dbf7218632dd53b8a41?cols=info:age,info:location

Timestamp range and versions.

    GET /v1/instances/training/tables/roster/rows/85a9b957350d9dbf7218632dd53b8a41?timerange=0..123&versions=2

<a id="row-put"> </a>
### Row Hex Entity ID: PUT

A PUT request to a row's hex entity ID inserts or updates data in the identified
row in a Kiji table. The request returns the row with only the most recent version of the
columns that were inserted.

    PUT /v1/instances/<instance>/tables/<table>/rows/<hexEntityId>

This operation will not replace the entire existing row but will replace cells and append
where cells in the request aren’t present in the row to be updated. To enforce the
idempotency requirement of the [HTTP spec](http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html),
cell timestamps are required so as to avoid the creation of multiple cells with new timestamps
when multiple PUTs are invoked.

#### Parameters:

* `<family:qualifier>` (multiple, required) - value (as a JSON string) to put into the
        specified <family:qualifier> column.  At least one needs to be specified for a valid PUT.

*  `timestamp` (single, required) - global timestamp for each cell in this PUT.
        This parameter is overruled by the `timestamp.<family:qualifier>` parameter for
        individual columns.

* `timestamp.<family:qualifier>` (multiple, optional) - specific timestamp to use for
        putting the `<family:qualifier>` column in this put.  Note: this overrides the global
        timestamp specified by the `timestamp` parameter.

* `schema.<family:qualifier>` (multiple, optional, defaults to schema specified in the
        table layout) - When an application is manually managing one or more
        schemas applied to a cell in a Kiji table, a KijiREST operation writing to a
        cell can specify which schema to use using this parameter. It is the schema
        (as a JSON string) of the cell to be to put into the specified
        `<family:qualifier>` column.

#### Notes:

`<family:qualifier>` is a required parameter with many of the keys in this call.
It refers to the Kiji column in “family:qualifier” format that the value is associated with.

#### Examples:

Put a single cell.

    PUT /v1/instances/training/tables/roster/rows/85a9b957350d9dbf7218632dd53b8a41?info:name=”John Smith”&timestamp=0

Put many cells. In this example, the hex entity ID for the row is indicated by “{row}”:

    PUT /v1/instances/training/tables/roster/rows/{row}?info:name=”John Smith”&info:email=”john.smith@mail.com”&timestamp=0

Put many cells while controlling the schema used for a given cell:

    PUT /v1/instances/training/tables/roster/rows/{row}?info:name=”John Smith”&info:email=”john.smith@mail.com”&timestamp=0&schema.info:name="string"

<a id="rows-get"> </a>
### Rows: GET

Returns multiple Kiji rows. This request has similar results as the kiji `scan` command,
rows bounded by the constraints.

    GET /v1/instances/<instance>/tables/<table>/rows

#### Parameters:

* `eid` (single, optional) - A JSON representation of the list of components (for example:
        `['string1', 2, 'string3']`) of the entity ID to retrieve. When specified, it returns
        the row represented by the given entity ID.

* `start_rk` (single, optional) - If executing a range query, this is the row key (specified as a
        hexadecimal string) to inclusively start from.

* `end_rk` (single, optional) - If executing a range query, this is the row key (specified as
        a hexadecimal string) to exclusively end with.

* `limit` (single, optional, default=100) - The number of rows to return.

* `cols` (single, optional, default=`*`) - A comma-separated list of column family
        (and qualifiers) to return. Specify `*` to indicate all columns. Specifying just a column
        family will yield values for all columns in the family and their qualifiers.

* `versions` (single, optional, default=1) - An integer representing the number of versions
        per cell to return for the given row.

* `timerange` (single, optional) - A string denoting the time range
        to return. Specify the time range `min..max` where `min` and `max` are the ms since UNIX epoch.
        `min` and `max` are both optional; however, if a value is provided for this parameter,
        at least one of min/max must be present.)

#### Notes:

This API will stream the results to the client. Each resulting row will be sent as a single
line of JSON terminated by “\r\n”. To ensure that clients can parse the streaming result
properly, any new lines in the JSON message itself will be represented by “\n” (without
the carriage return).

If the `eid` parameter is specified, then `start_rk`, `end_rk`, and `limit` parameters will be ignored.

#### Examples:

In these examples, the rows returned are from the “roster” table in the "training" instance.

Get many rows.

    GET /v1/instances/training/tables/roster/rows

Get a row by formatted key.

    GET /v1/instances/training/tables/roster/rows?eid=["Seleukos","Nikator"]

Scan rows from start and end keys.

    GET /v1/instances/training/tables/roster/rows?start_rk=a18fa1540bde9432&end_rk=b00bde940889a223

Scan rows from a start key and limit count.

    GET /v1/instances/training/tables/roster/rows?start_rk=a18fa1540bde9432&limit=10

<a id="rows-post"> </a>
### Rows: POST

Creates or adds cells to a row in the table. The body of the request is the JSON
representation of the entity and must contain the entity ID so as to identify the row to
create/update. The request returns the entity ID of the newly created resource.

    POST /v1/instances/<instance>/tables/<table>/rows

#### Parameters:

None. Uses the POSTed JSON blob for a row as the input. The blob is identical to the JSON
returned by GETting rows.

#### Notes:

To create or update a record, the entity ID (`entityId`) must be specified in the JSON body.
Cell timestamps are optional and if not specified, will be specified by the server.
To avoid a heavy return payload, only a reference to the newly created/updated resource
will be returned.

#### Example:

    {
      "entityId": "['John', 'Smith']",
      "cells":
      [
          {
              "columnFamily": "info",
              "columnQualifier": "email",
              "value": “john.smith@mail.com”,
              "timestamp": 1
          }
      ]
    }


<a id="ops-on-non-rows"> </a>
## Operations on non-row resources

<a id="version-get"> </a>
### Version: GET

Returns version information for the cluster and REST server.

    GET /v1/version

<a id="instances-get"> </a>
### Instances: GET

Returns a map of visible instances to their URIs.

    GET /v1/instances

<a id="instance-get"> </a>
### Specific Instance: GET

Returns the metadata (as JSON) for the given instance.

    GET /v1/instances/<instance>

<a id="tables-get"> </a>
### Tables: GET

Returns list of tables within the instance in the cluster.

    GET /v1/instances/<instance>/tables

<a id="table-get"> </a>
### Specific Table: GET

Returns the layout of the table &lt;table&gt;.

    GET /v1/instances/<instance>/tables/<table>

