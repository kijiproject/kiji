---
layout: post
title : Read and Write in Kiji
categories: [tutorials, phonebook-tutorial, 1.0.0-rc4]
tags: [phonebook]
order : 4
description: Add and lookup and single entry.
---

Now that KijiSchema is managing your phonebook table, the next step is to start writing
and reading contacts.

## Writing to a Table
Clearly, you need a way to add your ever-increasing set of friends to the phonebook.
KijiSchema supports writing to Kiji tables with the
[`KijiTableWriter`]({{site.api_url}}KijiTableWriter.html) class. The phonebook example
includes code that uses a `KijiTableWriter` to write to the phonebook table.

### AddEntry.java
The class `AddEntry.java` is included in the phonebook example source (located under
`$KIJI_HOME/examples/phonebook/src/main/java`). It implements a command-line tool 
that asks a user for contact information and then uses that information to populate 
the columns in a row in the Kiji table `phonebook` for that contact. 
To start, `AddEntry.java` loads an HBase configuration. 

{% highlight java %}
setConf(HBaseConfiguration.addHbaseResources(getConf()));
{% endhighlight %}

The code then connects to Kiji and opens the phonebook table for writing.
The `KijiConfiguration.DEFAULT_INSTANCE_NAME` is `default`.
{% highlight java %}
kiji = Kiji.open(new KijiConfiguration(getConf(),
    KijiConfiguration.DEFAULT_INSTANCE_NAME));
table = kiji.openTable(TABLE_NAME); //TABLE_NAME is "phonebook"
writer = table.openTableWriter();
{% endhighlight %}

#### Adding the phonebook entry
We then create an `EntityId` using the contact's first and last name. 
The `EntityId` uniquely identifies the row for the contact in the Kiji table. 

{% highlight java %}
EntityId user = table.getEntityId(first + "," + last);
{% endhighlight %}

We write the contact information gained from the user to the appropriate columns
in the contact's row of the Kiji table `phonebook`. 
The column names are specified as constants in the `Fields.java` class. For example,
the first name is written as:

{% highlight java %}
writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, first);
{% endhighlight %}

#### Finalization
We are done with the Kiji instance, table and writer we opened earlier.
We close these objects to free resources (for example, connections to HBase) 
that they use. We close these objects in the reverse order we opened them in.

{% highlight java %}
IOUtils.closeQuietly(writer);
IOUtils.closeQuietly(table);
IOUtils.closeQuietly(kiji);
{% endhighlight %}

### Running the Example
You run the class `AddEntry` with the `kiji` command-line tool as follows:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc4.jar \
    org.kiji.examples.phonebook.AddEntry
{% endhighlight %}
</div>

__The syntax shown here is the preferred mechanism to run your own `main(...)`
method with Kiji and its dependencies properly on the classpath.__

The interactive prompts (with sample responses) should look like:

<div class="userinput">
{% highlight bash %}
First name: Renuka
Last name: Apte
Email address: ra@wibidata.com
Telephone: 415-111-2222
Address line 1: 375 Alabama St
Apartment:
Address line 2:
City: SF
State: CA
Zip: 94110
{% endhighlight %}
</div>

#### Verify
Now we can verify that our entry got into the phonebook table.

Beforehand, you must tell `kiji ls` where the `org.kiji.examples.phonebook.Address`
Avro record class (mentioned in the DDL and used by `AddEntry`) is.
If you have not already done so, put the phonebook jar file on your Kiji classpath:

<div class="userinput">
{% highlight bash %}
export KIJI_CLASSPATH=$KIJI_HOME/examples/phonebook/lib/kiji-phonebook-*.jar
{% endhighlight %}
</div>

Now use `kiji ls`:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --kiji=kiji://.env/default/phonebook
{% endhighlight %}
</div>

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831108322] info:firstname
                                     Renuka
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831108322] info:lastname
                                     Apte
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831108322] info:email
                                     ra@wibidata.com
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831108322] info:telephone
                                     415-111-2222
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831108322] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

## Reading From a Table
Now that we've added a contact to your phonebook, we should be able to read this
contact from the table. KijiSchema supports reading from Kiji tables with the
[`KijiTableReader`]({{site.api_url}}KijiTableReader.html) class. We have included an
example of retrieving a single contact from the Kiji table using the contact's first
and last names.

### Lookup.java
We connect to Kiji and our phonebook table in the same way we did above.

{% highlight java %}
setConf(HBaseConfiguration.create(getConf()));
kiji = Kiji.open(new KijiConfiguration(getConf(),
    KijiConfiguration.DEFAULT_INSTANCE_NAME));
table = kiji.openTable(TABLE_NAME); // TABLE_NAME is "phonebook"
{% endhighlight %}

Since we are interested in reading from our table, we open a
[`KijiTableReader`]({{site.api_url}}KijiTableReader.html).
{% highlight java %}
reader = table.openTableReader();
{% endhighlight %}

#### Looking up the requested entry
Create an [`EntityId`]({{site.api_url}}/EntityId.html) to retrieve a contact
using the contact's first and last name:
{% highlight java %}
final EntityId entityId = table.getEntityId(mFirst + "," + mLast);
{% endhighlight %}

Create a data request to specify the desired columns from the Kiji Table.
{% highlight java %}
final KijiDataRequest dataReq = new KijiDataRequest()
    .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.FIRST_NAME))
    .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.LAST_NAME))
    .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.EMAIL))
    .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.TELEPHONE))
    .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.ADDRESS));
{% endhighlight %}

We now retrieve our result by passing the
[`EntityId`]({{site.api_url}}/EntityId.html) and data request to our table reader.
Doing so results in a [`KijiRowData`]({{site.api_url}}/KijiRowData.html) containing
the data read from the table.

{% highlight java %}
final KijiRowData rowData = reader.get(entityId, dataRequest);
{% endhighlight %}

### Running the Example
You can run the following command to perform a lookup using the `Lookup.java` example:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc4.jar \
    org.kiji.examples.phonebook.Lookup --first=Renuka --last=Apte
{% endhighlight %}
</div>

    Renuka Apte: email=ra@wibidata.com, tel=415-111-2222, addr={"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}
