---
layout : post
title : Create a Table
category : tutorial
tags : [article]
order : 3
description: To use a phonebook, you need a phonebook.
---

We need to create a table to store your bajillion phonebook contacts.
Creating a table in Kiji amounts to specifying a layout and registering
that layout with Kiji. Layouts can be specified using the Kiji
Data Definition Language, *DDL*, or in a JSON form. In this section,
we will cover using the DDL. <!-- TODO(DOCS-21): Re-enable JSON section.
after fixes. -->

### Using the DDL

We assume that you have `kiji-schema-shell` installed. If not, you should
consult the [Get Started](http://www.kiji.org/getstarted) section of the Kiji website.

We have provided the phonebook layout in the `$KIJI_HOME/examples/phonebook/layout.ddl` file.
For more information about how to create this file, see the
[DDL Shell Reference]({{site.userguide_url}}schema-shell-ddl-ref/).

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/schema-shell/bin/kiji-schema-shell \
    --file=$KIJI_HOME/examples/phonebook/layout.ddl
{% endhighlight %}
</div>

    OK.

#### Verify
Enter the Kiji schema shell using the following command:

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
            Schema: "string"

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

<!--
TODO(DOCS-21): JSON section has been removed from this document due to bugs in the
release.

Re-enable this section after the next release fixes the
layout.json file.
-->
#### Verify

To ensure that your table exists, use the `kiji ls` command to show the available
tables in your Kiji instance.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls
{% endhighlight %}
</div>

The above command should list `phonebook` as a table.
