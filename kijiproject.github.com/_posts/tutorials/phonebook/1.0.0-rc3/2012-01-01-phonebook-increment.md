---
layout: post
title : Use Counters
categories: [tutorials, phonebook-tutorial, 1.0.0-rc3]
tags: [article]
order : 7
description: Use atomic increment in the Phonebook example to calculate talktime.
---

You have just run out of minutes on your phone plan and want to find the person responsible
for inconveniencing you this way. Luckily, we have provided you with a phone log with the
time spent per phone call at `$KIJI_HOME/examples/phonebook/input-phone-log.txt`. You want to
be able to get the total time spent talking to each of your contacts this month.

To support operations like this Kiji provides atomic counter type cells like HBase that
permit multiple sources to be simultaneously incrementing the cell without conflict. In
this example, we will use the `stats:talktime` column of counters to count the total
talktime for each of your contacts.

Running the `describe phonebook;` command within the kiji-schema-shell should print the
`stats:talktime` as a column of counter cells:

    ...

    Column family: stats
      Description: Statistics about a contact.

      Column stats:talktime (Time spent talking with this person)
        Schema: (counter)

We'll show you a way to use counters to get the amount of time that each of your contacts has
spent calling you with the IncrementTalkTime example.

### IncrementTalkTime.java

The `stats:talktime` column of the phonebook table is of the type COUNTER. You can see this by
looking at the layout files. As Kiji tables are based on Hbase, they also provide the ability to
treat columns as counters.

IncrementTalkTime uses MapReduce to calculate the talk time per person in the call log by
extending the Hadoop Mapper class. You can find more information about Hadoop MapReduce [here](http://hadoop.apache.org/).

The application starts by configuring a Hadoop job. Note that we need to ship certain jars
that we depend on during the *map* task. Here\'s how we do this:

{% highlight java %}
GenericTableMapReduceUtil.addAllDependencyJars(job);
DistributedCacheJars.addJarsToDistributedCache(job,
    new File(System.getenv("KIJI_HOME"), "lib"));
job.setUserClassesTakesPrecedence(true);
{% endhighlight %}

The map function is run once per each line in the phone log file. The input to the function
is a line of the form:

> firstname | lastname | call_duration

The `setup` function is called once per mapper. It opens the phonebook kiji table and creates
a context to be able to write to it. Specifically, the calculated total talk time per contact
will be written to the contact's record.

The map task breaks the input line up into its individual components. It then generates a row ID
for this user in the Kiji table as follows:

{% highlight java %}
EntityId user = mKijiTable.getEntityId(firstName + "," + lastName);
{% endhighlight %}

The following code increments the existing value of the `stats:talktime` column by the call
duration in the call log.

{% highlight java %}
mWriter.increment(user, "stats", "talktime", talkTime);
{% endhighlight %}

### Running the Example

First you need to add the phone log to hdfs. You can do this by using the `hdfs -copyFromLocal`
command.

<div class="userinput">
{% highlight bash %}
$HADOOP_HOME/bin/hadoop fs -copyFromLocal \
    $KIJI_HOME/examples/phonebook/input-phone-log.txt /tmp
{% endhighlight %}
</div>

You can then run the `kiji jar` command, much like the previous examples, providing the path
to the file in hdfs.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc3.jar \
    org.kiji.examples.phonebook.IncrementTalkTime /tmp/input-phone-log.txt
{% endhighlight %}
</div>

#### Verify
Now we can look up the derived talktime value from the stats column for the user John Doe using the `kiji ls` command:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --table=phonebook \
    --entity-id="John,Doe" --columns="stats"
{% endhighlight %}
</div>

    Looking up entity: U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB from kiji table: kiji://localhost:2181/default/phonebook/
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352838746735] stats:talktime
                                     15
