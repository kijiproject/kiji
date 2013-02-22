---
layout : post
title : Create a Table
categories: [tutorials, phonebook-tutorial, 1.0.0-rc4]
tags : [phonebook]
order : 3
description: To use a phonebook, you need a phonebook.
---

We need to create a table to store your bajillion phonebook contacts.
Creating a table in Kiji amounts to specifying a layout and registering
that layout with Kiji. Layouts can be specified using the Kiji
Data Definition Language, *DDL*, or in a JSON form.

### Using the DDL

We assume that you have `kiji-schema-shell` installed. If not, you should
consult the [Get Started](http://www.kiji.org/getstarted) section of the Kiji website.

We have provided the phonebook layout in the `$KIJI_HOME/examples/phonebook/layout.ddl` file.
For more information about how to create this file, see the
[DDL Shell Reference]({{site.userguide_url}}/schema/1.0.0-rc4/schema-shell-ddl-ref/).

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
    Column family: info
        Description: basic information

        Column info:firstname (First name)
            Schema: "string"

        Column info:lastname (Last name)
            Schema: "string"

        Column info:email (Email address)
            Schema: "string"

        Column info:telephone (Telephone number)
            Schema: "string"

        Column info:address (Street address)
            Schema: "org.kiji.examples.phonebook.Address"

    Column family: derived
        Description: Information derived from an individual's address.

        Column derived:addr1 (Address line one.)
            Schema: "string"

        Column derived:apt (Address Apartment number.)
            Schema: [ "string", "null" ]

        Column derived:addr2 (Address line two.)
            Schema: "string"

        Column derived:city (Address city.)
            Schema: "string"

        Column derived:state (Address state.)
            Schema: "string"

        Column derived:zip (Address zip code.)
            Schema: "int"

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
the layout in JSON, take a look at [Managing Data]({{site.userguide_url}}/schema/1.0.0-rc4/managing-data/).

But first, we need to delete the table we just created, just so that we can create it
another time using JSON! Use the following command to delete the table:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji delete-table --target=kiji://.env/default/phonebook
{% endhighlight %}
</div>

    Deleting kiji table: kiji://localhost:2181/default/phonebook/
    Are you sure? This action will remove this table and all its data from kiji and cannot be undone!
    Please answer yes or no.
    yes
    00/11/00 20:33:16 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Started disable of kiji.default.table.phonebook
    00/11/00 20:33:18 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Disabled kiji.default.table.phonebook
    00/11/00 20:33:19 INFO org.apache.hadoop.hbase.client.HBaseAdmin: Deleted kiji.default.table.phonebook
    Deleted kiji table: kiji://localhost:2181/default/phonebook/

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
Creating kiji table: kiji://localhost:2181/default/phonebook/...
{% endhighlight %}


#### Verify

To ensure that your table exists, use the `kiji ls` command to show the available
tables in your Kiji instance.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls
{% endhighlight %}
</div>

The above command should list `phonebook` as a table.
