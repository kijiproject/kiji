---
layout: post
title: KijiREST Operations
categories: [userguides, rest, 1.3.0]
tags: [rest-ug]
order: 4
version: 1.3.0
description: Operations
---

The HTTP operations supported by the KijiREST API are the following:

* /v1

* /v1/version	[GET](#version-get)

* /v1/instances	[GET](#instances-get)

* /v1/instances/&lt;instance&gt;	[GET](#instance-get)

* /v1/instances/&lt;instance&gt;/tables	[GET](#tables-get)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;	[GET](#table-get)

* /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows	[GET](#rows-get), [POST](#rows-post)

The following sections describe these operations in two groups:

* [Operations on rows](#ops-on-rows)
* [Operations on non-row resources](#ops-on-non-rows)

Note that you can run these commands within your browser by visiting the URL
[http://localhost:8080](http://localhost:8080), followed by the path of the GET command below.  For
example, to view the first 100 rows within `kiji://.env/default/user_ratings`, you can visit
[http://localhost:8080/v1/instances/default/tables/user_ratings/rows](http://localhost:8080/v1/instances/default/tables/user_ratings/rows).

<a id="ops-on-rows"> </a>
## Operations on Rows

### Specifying entity IDs

In order to perform row operations, you need to be able to identity rows.
You can do this by specifying the entity ID of the row in the Kiji table.
There are three ways to specify the entity ID in KijiREST.

* `["component1", "component2", "component3", 123, 321]` - formatted (i.e. composite) entity IDs are specified as JSON arrays of string and numeric components.
* `hbase=?\x0Bcomponent1\x00component2\x00component3\x00\x80\x00\x00{\x80\x00\x00\x00\x00\x00\x01A` - the HBase row key encoded as a string. Unprintable characters are represented by \xDD.
* `hbase_hex=3f0b636f6d706f6e656e743100636f6d706f6e656e743200636f6d706f6e656e7433008000007b8000000000000141` - the ASCII-encoded hexadecimal HBase row key.

Note that if a row is specified in the address bar, it must be further [URL-encoded](http://en.wikipedia.org/wiki/Percent-encoding). If a row is specified in a JSON body (e.g. in a POST request), it must be a properly escaped JSON element.

<a id="rows-get"> </a>
### Rows: GET

Returns (streams) multiple Kiji rows. The results of this request are similar
to those of `kiji get` and `kiji scan` -- the request produces a list of rows, bounded by
constraints.  Each row is a JSON blob, delimited by the "\r\n" characters (and
with all "\r\n" characters within each JSON blob are replaced with "\n" to
avoid collision with the delimiter characters).

    GET /v1/instances/<instance>/tables/<table>/rows

#### Parameters:

* `eid` (single, optional) - A JSON representation of the list of components (for example:
        `["string1", 2, "string3"]`) of the entity ID to retrieve. When specified, it returns
        the row represented by the given entity ID. The values of `start_eid`,
        `end_eid`, and `limit` parameters are ignored. Advanced users: you may do partially-specified entity ID scans by 
        replacing components of an entity ID with wildcards (wildcards are empty arrays),
        e.g. `["string1", 2, []]` will match all values of the terminal component.

* `start_eid` (single, optional) - If executing a range query, this is the entity ID (represented
              with JSON or HBase row key, as described above) from which to start (inclusive).
              Wildcarded start entity IDs are not supported.

* `end_eid` (single, optional) - If executing a range query, this is the entity ID (represented with
            JSON or HBase row key, as described above) at which to end (exclusive).
            Wildcarded end entity IDs are not supported.

* `limit` (single, optional, default=100) - The number of rows to return. Specify -1 to return all rows.

* `cols` (single, optional, default=`*`) - A comma-separated list of column families
        (and qualifiers) to return. Specify `*` to indicate all columns. Specifying just a column
        family will return values for all columns in the family and their qualifiers.

* `versions` (single, optional, default=1) - An integer representing the number of versions
        per cell to return for the given row.

* `timerange` (single, optional) - A string denoting the time range
        to return. Specify the time range `min..max` where `min` and `max` are the milliseconds
        since UNIX epoch.  The `timerange` parameter requires at least one of `min` and `max` to be
        set (i.e., you can specify only a `min`, only a `max`, or both).

#### Examples:

Get the first 100 rows (the default number of rows) from the users table:

    GET /v1/instances/default/tables/users/rows

Get a single row identified by the entity_id:

    GET /v1/instances/default/tables/users/rows?eid=[1298404763]

Retrieve the first 100 rows (the default number of rows) between start and end entity IDs:

    GET /v1/instances/default/tables/users/rows?start_eid=[1298404763]&end_eid=[4591230321]

Retrieve the first 10 rows starting from a particular entity ID:

    GET /v1/instances/default/tables/users/rows?start_eid=[1298404763]&limit=10

In a table with composite entity IDs, perform a partially specified entity ID scan matching all values of the middle component. Return up to 10 rows.

    GET /v1/instances/default/tables/users/rows?eid=["component1",[],"component3"]&limit=10

<a id="rows-post"> </a>
### Rows: POST

Creates or adds cells to a row in the table. The body of the request is the JSON
representation of the entity and must contain the entity ID so as to identify the row to
create/update. The request returns the entity ID of the newly created/updated resource.

    POST /v1/instances/<instance>/tables/<table>/rows

#### Parameters:

None. Uses the POSTed JSON blob for a row as the input. The blob is identical to the JSON
returned by GETting rows.

#### Notes:

To create or update a record, the entity ID (`entityId`) must be specified in
the JSON body. Note that the entity ID as part of the JSON body must be a properly escaped JSON element.
Cell timestamps are optional (if not specified, the server will
set the cell timestamps to the time at which it creates or updates the record).
To avoid a heavy return payload, only a reference to the newly created/updated
resource will be returned.

The writer_schema field ensures that data returned by the server can be
modified and sent back. In Kiji, there are two representations of the writer
schema:

1. __JSON__ - This is standard JSON representation of an Avro schema.
2. __Integer ID__ - This is the internal Kiji representation of the specified Avro schema. For cells
being modified and sent back, this can be left alone; however for new cells, it's better to specify
the actual JSON schema of the cell.
#### Example:

{% highlight js %}
    {
      "entityId": [12345],
      "cells":
      {
        "info":
        {
          "name":
          [
            {
            "timestamp": 1,
            "value": "Jane Smith",
            "writer_schema": "string"
            }
          ]
        }
      }
    }
{% endhighlight %}

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

