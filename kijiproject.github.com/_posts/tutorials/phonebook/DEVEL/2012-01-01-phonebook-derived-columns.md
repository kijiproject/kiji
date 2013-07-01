---
layout: post
title : Derive Data
categories: [tutorials, phonebook-tutorial, devel]
tags: [phonebook]
order: 6
description: Decompose an address into its individual fields.
---

<div class="hero-unit">
  <h3>This page is deprecated.</h3>
  <p>
    For a more up-to-date look at how to derive data using Producers, see the
    <a href="{{site.tutorial_music_devel}}/music-overview/">Music
    recommendation tutorial</a>.
    This section is preserved for your reference, but the APIs referenced herein
    are deprecated and may be removed in a future release of KijiSchema.
  </p>
  <p>
    Note that later sections of this tutorial rely on you running the commands
    in this section. But you should not model your future MapReduce analyses on
    the code in <tt>AddressFieldExtractor</tt>.
  </p>
</div>

Your friends have been terribly disorganized about giving you their contact details.
Being the perfectionist you are, you would like to be able to, at any given point, know
how many friends you have in a certain zip code... because obviously, such questions
need answering.

We'll show you a way to decompose your contacts’ addresses into their street address, city,
and zip code (the derived columns) to make it easier for you to get this information quickly.

### AddressFieldExtractor.java

The run function begins by creating an HBase configuration, and configuring the MapReduce task.
Note that we need to ship certain jars that we depend on during the *map* task. Here\'s how we
do this:

{% highlight java %}
GenericTableMapReduceUtil.addAllDependencyJars(job);
DistributedCacheJars.addJarsToDistributedCache(job,
    new File(System.getenv("KIJI_HOME"), "lib"));
job.setUserClassesTakesPrecedence(true);
{% endhighlight %}

The AddressMapper extends Hadoop's Mapper class. The map function is run per row of the Kiji table.
It extracts the address field from each row as follows:

{% highlight java %}
final Address address = row.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS);
{% endhighlight %}

Address is the same Avro type you read about on the
[Phonebook Importer](../phonebook-import/) page. The JSON
description for it can be found at
`$KIJI_HOME/examples/phonebook/src/main/avro/Address.avsc`. More information
about Avro types can be found
[here](http://avro.apache.org/docs/current/spec.html).

We decompose and write the individual fields into a derived column using `mWriter.put(...)`. For
example, the zip code can be extracted from the Address object and written as follows:

{% highlight java %}
mWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ZIP, address.getZip());
{% endhighlight %}

### Running the Example
We assume that you have already imported the contacts from
`$KIJI_HOME/examples/phonebook/input-data.txt` into the phonebook Kiji table by this point.
You can execute this example using the `kiji jar` command with the class name:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-{{site.phonebook_devel_version}}.jar \
    org.kiji.examples.phonebook.AddressFieldExtractor
{% endhighlight %}
</div>

#### Verify
You can use the following command to see if your contacts' address data was successfully extracted:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji scan kiji://.env/default/phonebook/derived
{% endhighlight %}
</div>

    Scanning kiji table: kiji://localhost:2181/default/phonebook/derived/
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228186202] derived:addr1
                                     1600 Pennsylvania Ave
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228186203] derived:city
                                     Washington
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228186205] derived:state
                                     DC
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228186207] derived:zip
                                     99999

    ...
