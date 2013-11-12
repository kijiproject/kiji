---
layout: post
title : Read and Write in Kiji
categories: [tutorials, phonebook-tutorial, 1.1.1]
tags: [phonebook]
order : 4
description: Add and lookup and single entry.
---

Now that KijiSchema is managing your phonebook table, the next step is to start writing
and reading contacts.

## Writing to a Table
Clearly, you need a way to add your ever-increasing set of friends to the phonebook.
KijiSchema supports writing to Kiji tables with the
[`KijiTableWriter`]({{site.api_schema_1_3_3}}/KijiTableWriter.html) class. The phonebook example
includes code that uses a [`KijiTableWriter`]({{site.api_schema_1_3_3}}/KijiTableWriter.html) to
write to the phonebook table.

### AddEntry.java
The class `AddEntry.java` is included in the phonebook example source (located under
`$KIJI_HOME/examples/phonebook/src/main/java/org/kiji/examples/phonebook/`). It implements a command-line tool
that asks a user for contact information and then uses that information to populate
the columns in a row in the Kiji table `phonebook` for that contact.
To start, `AddEntry.java` loads an HBase configuration.

{% highlight java %}
setConf(HBaseConfiguration.addHbaseResources(getConf()));
{% endhighlight %}

The code then connects to Kiji and opens the phonebook table for writing. A [`Kiji`]({{site.api_schema_1_3_3}}/Kiji.html)
instance is specified by a [`KijiURI`]({{site.api_schema_1_3_3}}/KijiURI.html). A Kiji URI specifies an HBase cluster to
connect to (identified by its Zookeeper quorum) and a Kiji instance name.
The value of `KConstants.DEFAULT_INSTANCE_NAME` is `"default"`.
For example, if ZooKeeper is running on `zkhost:2181`, the name of the default
Kiji instance on the cluster would be `kiji://zkhost:2181/default`.

Rather than specify a ZooKeeper cluster yourself, you can rely on the quorum
specified in your `hbase-site.xml` file by using the "hostname" of `.env`, like
this: `kiji://.env/default`.

To create a [`KijiURI`]({{site.api_schema_1_3_3}}/KijiURI.html), you use a
[`KijiURI.KijiURIBuilder`]({{site.api_schema_1_3_3}}/KijiURI.KijiURIBuilder.html)
instance. By default, this will use the `".env"` pseudo-host so that you connect
to your normal HBase cluster.

{% highlight java %}
kiji = Kiji.Factory.open(
    KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build(),
    getConf());
table = kiji.openTable(TABLE_NAME); // TABLE_NAME is "phonebook"
writer = table.openTableWriter();
{% endhighlight %}

#### Adding the phonebook entry
We then create an [`EntityId`]({{site.api_schema_1_3_3}}/EntityId.html) using the contact's first
and last name. The [`EntityId`]({{site.api_schema_1_3_3}}/EntityId.html) uniquely identifies the
row for the contact in the Kiji table.

{% highlight java %}
final EntityId user = table.getEntityId(first + "," + last);
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
We close or release these objects to free resources (for example, connections to HBase)
that they use. We close or release these objects in the reverse order we opened them in.

{% highlight java %}
ResourceUtils.closeOrLog(writer);
ResourceUtils.releaseOrLog(table);
ResourceUtils.releaseOrLog(kiji);
{% endhighlight %}

Something important to note is that the Kiji instance and Kiji table are _released_ rather than closed.
Kiji instances and tables are often long-lived objects that many aspects of your system may hold
reference to. Rather than require that you define a single "owner" of this object who
closes it when the system is finished using it, you can use reference counting to manage
this object's lifetime.

When a [`Kiji`]({{site.api_schema_1_3_3}}/Kiji.html) instance is created with `Kiji.Factory.open()`,
or a ['KijiTable']({{site.api_schema_1_3_3}}/KijiTable.html) is opened with `Kiji.openTable(name)`,
it has an automatic reference count of 1. You should call `kiji.release()` or `table.release()` or use
[`ResourceUtils`]({{site.api_schema_1_3_3}}/util/ResourceUtils.html)`.releaseOrLog(kiji)` or
`ResourceUtils.releaseOrLog(table)` to discard these reference.

If another class or method gets a reference to an already-opened Kiji instance,
you should call `kiji.retain()` to increment its reference count. That same
class or method is responsible for calling `kiji.release()` when it no longer
holds the reference.

A [`Kiji`]({{site.api_schema_1_3_3}}/Kiji.html) object or
[`KijiTable`]({{site.api_schema_1_3_3}}/KijiTable.html) will close itself and free its underlying
resources when its reference count drops to 0.

### Running the Example
You run the class `AddEntry` with the `kiji` command-line tool as follows:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.1.1.jar \
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

Beforehand, you must tell `kiji scan` where the `org.kiji.examples.phonebook.Address`
Avro record class (mentioned in the DDL and used by `AddEntry`) is.
If you have not already done so, put the phonebook jar file on your Kiji classpath:

<div class="userinput">
{% highlight bash %}
export KIJI_CLASSPATH="$KIJI_HOME/examples/phonebook/lib/*"
{% endhighlight %}
</div>

Now use `kiji scan`:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji scan kiji://.env/default/phonebook
{% endhighlight %}
</div>

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    entity-id=['Renuka,Apte'] [1384235579766] info:firstname
                                     Renuka
    entity-id=['Renuka,Apte'] [1384235579766] info:lastname
                                     Apte
    entity-id=['Renuka,Apte'] [1384235579766] info:email
                                     ra@wibidata.com
    entity-id=['Renuka,Apte'] [1384235579766] info:telephone
                                     415-111-2222
    entity-id=['Renuka,Apte'] [1384235579766] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

## Reading From a Table
Now that we've added a contact to your phonebook, we should be able to read this
contact from the table. KijiSchema supports reading from Kiji tables with the
[`KijiTableReader`]({{site.api_schema_1_3_3}}/KijiTableReader.html) class. We have included an
example of retrieving a single contact from the Kiji table using the contact's first
and last names.

### Lookup.java
We connect to Kiji and our phonebook table in the same way we did above.

{% highlight java %}
setConf(HBaseConfiguration.create(getConf()));
kiji = Kiji.Factory.open(
    KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build(),
    getConf());
table = kiji.openTable(TABLE_NAME); // TABLE_NAME is "phonebook"
{% endhighlight %}

Since we are interested in reading from our table, we open a
[`KijiTableReader`]({{site.api_schema_1_3_3}}/KijiTableReader.html).
{% highlight java %}
reader = table.openTableReader();
{% endhighlight %}

#### Looking up the requested entry
Create an [`EntityId`]({{site.api_schema_1_3_3}}/EntityId.html) to retrieve a contact
using the contact's first and last name:
{% highlight java %}
final EntityId entityId = table.getEntityId(mFirst + "," + mLast);
{% endhighlight %}

Create a data request to specify the desired columns from the Kiji Table.
{% highlight java %}
final KijiDataRequestBuilder reqBuilder = KijiDataRequest.builder();
reqBuilder.newColumnsDef()
    .add(Fields.INFO_FAMILY, Fields.FIRST_NAME)
    .add(Fields.INFO_FAMILY, Fields.LAST_NAME)
    .add(Fields.INFO_FAMILY, Fields.EMAIL)
    .add(Fields.INFO_FAMILY, Fields.TELEPHONE)
    .add(Fields.INFO_FAMILY, Fields.ADDRESS);
final KijiDataRequest dataRequest = reqBuilder.build();
{% endhighlight %}

We now retrieve our result by passing the
[`EntityId`]({{site.api_schema_1_3_3}}/EntityId.html) and data request to our table reader.
Doing so results in a [`KijiRowData`]({{site.api_schema_1_3_3}}/KijiRowData.html) containing
the data read from the table.

{% highlight java %}
final KijiRowData rowData = reader.get(entityId, dataRequest);
{% endhighlight %}

### Running the Example
You can run the following command to perform a lookup using the `Lookup.java` example:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.1.1.jar \
    org.kiji.examples.phonebook.Lookup --first=Renuka --last=Apte
{% endhighlight %}
</div>

    Renuka Apte: email=ra@wibidata.com, tel=415-111-2222, addr={"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}
