---
layout: post
title: KijiREST Operations
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 4
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

The following sections describe these operations in two groups:

* [Operations on rows](#ops-on-rows)
* [Operations on non-row resources](#ops-on-non-rows)

<a id="ops-on-rows"> </a>
## Operations on Rows

<a id="entityID-get"> </a>
### Entity ID: GET

The GET request returns the hexadecimal representation (in ASCII) of the corresponding HBase
row key given a list of components comprising a row's entity ID:

    GET /v1/instances/<instance>/tables/<table>/entityId

#### Parameters:

*  `eid` (single, required) - JSON array of the the components of the entity ID in order
of their definition in the layout of the given table. String components must be single quoted
while numeric components must remain unquoted.

#### Example:

    GET /v1/instances/default/tables/users/entityId?eid=[1298404763]

Returns:
<pre>
{
  rowKey: "9ee9800000004d64159b"
}
</pre>
<a id="rows-get"> </a>
### Rows: GET

Returns (streams) multiple Kiji rows. This request has similar results as the kiji `scan` command,
rows bounded by the constraints. Each row is a JSON clob delimited by the "\r\n" characters and
all "\r\n" characters within each JSON clob are replaced with "\n" to avoid collision with the
delimiter characters.

    GET /v1/instances/<instance>/tables/<table>/rows

#### Parameters:

* `eid` (single, optional) - A JSON representation of the list of components (for example:
        `['string1', 2, 'string3']`) of the entity ID to retrieve. When specified, it returns
        the row represented by the given entity ID. If this is specified, then `start_rk`,
        `end_rk`, and `limit` parameters are ignored.

* `start_rk` (single, optional) - If executing a range query, this is the HBase row key (specified as a
        hexadecimal string) to inclusively start from.

* `end_rk` (single, optional) - If executing a range query, this is the HBase row key (specified as
        a hexadecimal string) to exclusively end with.

* `limit` (single, optional, default=100) - The number of rows to return. Specify -1 to return all rows.

* `cols` (single, optional, default=`*`) - A comma-separated list of column family
        (and qualifiers) to return. Specify `*` to indicate all columns. Specifying just a column
        family will return values for all columns in the family and their qualifiers.

* `versions` (single, optional, default=1) - An integer representing the number of versions
        per cell to return for the given row.

* `timerange` (single, optional) - A string denoting the time range
        to return. Specify the time range `min..max` where `min` and `max` are the ms since UNIX epoch.
        `min` and `max` are both optional; however, if a value is provided for this parameter,
        at least one of min/max must be present.)


#### Examples:

Get the first 100 rows (the default number of rows) from the users table:

    GET /v1/instances/default/tables/users/rows

Get a single row identified by the entity_id:

    GET /v1/instances/default/tables/users/rows?eid=[1298404763]

Retrieve the first 100 rows (the default number of rows) between start and end keys:

    GET /v1/instances/default/tables/users/rows?start_rk=9ee9800000004d64159b&end_rk=fca9800000004d64159c

Retrieve the first 100 rows starting from a particular key:

    GET /v1/instances/default/tables/users/rows?start_rk=9ee9800000004d64159b&limit=10

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

To create or update a record, the entity ID (`entityId`) must be specified in the JSON body.
Cell timestamps are optional and if not specified, will be specified by the server.
To avoid a heavy return payload, only a reference to the newly created/updated resource
will be returned.

The writer_schema field was recently introduced in KijiREST 0.5.0. This is required to ensure that data
returned by the server can be modified and sent back. In Kiji, there are two representations of the
writer schema:

1. __JSON__ - This is standard JSON representation of an Avro schema.
2. __Integer ID__ - This is the internal Kiji representation of the specified Avro schema. For cells
being modified and sent back, this can be left alone; however for new cells, it's better to specify
the actual JSON schema of the cell.
#### Example:

{% highlight js %}
    {
      "entityId": "[12345]",
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

