---
layout : post
title : Create a Table
categories: [tutorials, phonebook-tutorial, devel]
tags : [phonebook]
order : 3
description: To use a phonebook, you need a phonebook.
---

We need to create a table to store your bajillion phonebook contacts.
Creating a table in Kiji amounts to specifying a layout and registering
that layout with Kiji. Layouts can be specified using the Kiji
Data Definition Language, *DDL*, or in a JSON form. The DDL is the easiest
and most well-supported mechanism to use; we will introduce it first.

### Using the DDL

We assume that you have `kiji-schema-shell` installed. If not, you should
consult the [Get Started](http://www.kiji.org/getstarted) section of the Kiji website.

We have provided the phonebook layout in the `$KIJI_HOME/examples/phonebook/layout.ddl` file.
For more information about how to create this file, see the
[DDL Shell Reference]({{site.userguide_schema_devel}}/schema-shell-ddl-ref/).

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/schema-shell/bin/kiji-schema-shell \
    --file=$KIJI_HOME/examples/phonebook/layout.ddl
{% endhighlight %}
</div>

    OK.

#### Verify
Enter the KijiSchema shell using the following command:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/schema-shell/bin/kiji-schema-shell
{% endhighlight %}
</div>

Use the `show tables` and `describe` commands to see your newly created table.

<div class="userinput">
{% highlight bash %}
schema> show tables;
{% endhighlight %}
</div>

    Table       Description
    =========   ==================================
    phonebook   A collection of phone book entries

<div class="userinput">
{% highlight bash %}
schema> describe phonebook;
{% endhighlight %}
</div>

    Table: phonebook (A collection of phone book entries)
    Row key:
      key: STRING NOT NULL

    Column family: info
      Description: basic information

      Column info:firstname (First name)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column info:lastname (Last name)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column info:email (Email address)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column info:telephone (Telephone number)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column info:address (Street address)
        Default reader schema class name: org.kiji.examples.phonebook.Address
        1 reader schema(s) available.
        1 writer schema(s) available.

    Column family: derived
      Description: Information derived from an individual's address.

      Column derived:addr1 (Address line one.)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column derived:apt (Address Apartment number.)
        Default reader schema: ["string","null"]
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column derived:addr2 (Address line two.)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column derived:city (Address city.)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column derived:state (Address state.)
        Default reader schema: "string"
        1 reader schema(s) available.
        1 writer schema(s) available.

      Column derived:zip (Address zip code.)
        Default reader schema: "int"
        1 reader schema(s) available.
        1 writer schema(s) available.

    Column family: stats
      Description: Statistics about a contact.

      Column stats:talktime (Time spent talking with this person)
        Schema: (counter)


<div class="userinput">
{% highlight bash %}
schema> quit;
{% endhighlight %}
</div>

### Using JSON

A low level way of providing the layout is by using JSON. To learn more about specifying
the layout in JSON, take a look at [Managing Data]({{site.userguide_schema_devel}}/managing-data/).

The raw JSON view of table layouts is intended for use by system administrators, or
for low-level debugging purposes. Most users should use the `kiji-schema-shell` DDL tool
to modify table layouts instead. But for completeness, we introduce the "raw" layout
tool here as well.

First, we need to delete the table we just created, so that we can create it
another time using JSON. Use the following command to delete the table:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji delete --target=kiji://.env/default/phonebook
{% endhighlight %}
</div>

    Are you sure you want to delete Kiji table 'kiji://localhost:2181/default/phonebook/'?
    Type 'phonebook' without the quotes to confirm(or nothing to cancel):
    phonebook
    13/03/13 19:19:28 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Started disable of kiji.default.table.phonebook
    13/03/13 19:19:29 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Disabled kiji.default.table.phonebook
    13/03/13 19:19:29 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Deleted kiji.default.table.phonebook
    Kiji table 'kiji://localhost:2181/default/phonebook/' deleted.

The command below creates the same phonebook table with the layout specified in the `layout.json` file in your
`$KIJI_HOME/examples/phonebook` directory.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji create-table --table=kiji://.env/default/phonebook \
    --layout=$KIJI_HOME/examples/phonebook/layout.json
{% endhighlight %}
</div>

{% highlight bash %}
Parsing table layout: $KIJI_HOME/examples/phonebook/layout.json
Creating Kiji table: kiji://localhost:2181/default/phonebook/
{% endhighlight %}


#### Verify

To ensure that your table exists, use the `kiji ls` command to show the available
tables in your Kiji instance.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls kiji://.env/default
{% endhighlight %}
</div>

The above command should list `phonebook` as a table.
