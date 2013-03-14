---
layout: post
title: Import Data
categories: [tutorials, phonebook-tutorial, 1.0.0-rc5]
tags: [phonebook]
order: 5
description: How to import data into the phonebook table.
---

At this point, you should be worried about wearing out your keyboard
by manually running an add command for each of your (incredibly large set of)
friends. First, we will discuss and give examples of the naive way of extending `AddEntry`
to load a table, and then present the recommended MapReduce approach.

### StandalonePhonebookImporter.java
An example of writing puts in order to load a table is given in
`StandalonePhonebookImporter`. This approach is easier than running the add command
for every user, but is still suboptimal because you are using one machine to perform every put.

#### Parsing the Delimited Phonebook Entries
First, let's take a closer look at the data we want to import.  In
`$KIJI_HOME/examples/phonebook/input-data.txt` you will see records like:

> John|Doe|johndoe@gmail.com|202-555-9876|{"addr1":"1600 Pennsylvania Ave","apt":null,"addr2":null,"city":"Washington","state":"DC","zip":99999}

The fields in each record are delimited by a `|` character. The last field is actually
a complete JSON Avro record representing an address.

At the top of the `importLine(...)` method in `StandalonePhonebookImporter`,  you'll
see we're using a delimiter to split up the record into its specific fields:
{% highlight java %}
String[] fields = value.toString().split("\\|");
//...

String firstName = fields[0];
String lastName = fields[1];
String email = fields[2];
String telephone = fields[3];
String addressJson = fields[4];
{% endhighlight %}

Since our last field is a complex Avro type represented in JSON, we use a JSON decoder
with an Avro schema (in this case `Address`), to decode the field into a complete Avro Address
object.

{% highlight java %}
// addressJson contains a JSON-encoded Avro "Address" record. Parse this into
// an object and write it to the person's record.
// The Address record type is generated from src/main/avro/Address.avsc as part
// of the build process (see avro-maven-plugin in pom.xml).
SpecificDatumReader<Address> datumReader =
    new SpecificDatumReader<Address>(Address.SCHEMA$);
JsonDecoder decoder =
    DecoderFactory.get().jsonDecoder(Address.SCHEMA$, addressJson);
Address streetAddr = datumReader.read(null, decoder);
{% endhighlight %}

Next we create a unique [`EntityId`]({{site.api_schema_rc5}}/EntityId.html) that will be used to reference this row.  As before, we will use
the combination of first and last name as a unique reference to this row:
{% highlight java %}
EntityId user = table.getEntityId(firstName + "," + lastName);
{% endhighlight %}

Finally we just retrieve the current system timestamp and write these record fields.
{% highlight java %}
long timestamp = System.currentTimeMillis();
writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, firstName);
writer.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, timestamp, lastName);
writer.put(user, Fields.INFO_FAMILY, Fields.EMAIL, timestamp, email);
writer.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, timestamp, telephone);
writer.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, timestamp, streetAddr);
{% endhighlight %}

### Running the Example
You can execute `StandalonePhonebookImporter` just like you would the `AddEntry`
example - using the `kiji jar` command.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc5.jar \
    org.kiji.examples.phonebook.StandalonePhonebookImporter \
    $KIJI_HOME/examples/phonebook/input-data.txt
{% endhighlight %}
</div>

You now have data in your phonebook table!

#### Verify
Verify that the user records were added properly by executing:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji scan kiji://.env/default/phonebook
{% endhighlight %}
</div>

## Importing Data using MapReduce
While the above example does import the requisite data, it doesn't scale to large
data sets, because it doesn't take advantage of the cluster you are writing to.
Instead of generating all puts locally like we did above, we will write a MapReduce
job that reads from text file in HDFS and writes to your Kiji table by generating
puts in a distributed fashion.

### PhonebookImporter.java
Our example of importing data into a table with MapReduce can be found in the class
`PhonebookImporter`. PhonebookImporter defines a special type of MapReduce job called a
Kiji bulk import job that reads each line of our input file, parses it, and writes it to a table.
Kiji bulk import jobs are created by implementing a [`KijiBulkImporter`]({{site.api_mr_rc5}}/bulkimport/KijiBulkImporter.html),
not a `Mapper` and `Reducer`. This API is provided by KijiMR. The
[Music recommendation tutorial](/tutorials/music-recommendation/1.0.0-rc5/music-overview/)
covers KijiMR in much greater detail, but we will take a look at using the
[`KijiBulkImporter`]({{site.api_mr_rc5}}/bulkimport/KijiBulkImporter.html) API below.

Instead of a `map()` method, we provide a `produce()` method definition; this method processes
an input record from a file like an ordinary mapper, except its `context` argument is
specifically targeted to output to a row in a Kiji table.

At the top of the `produce()` method, you'll see that we extract the fields from
the lines as in the above example.  Then using the
[`KijiTableContext`]({{site.api_mr_rc5}}/KijiTableContext.html) context argument,
we'll write all of the fields to the phonebook table:

{% highlight java %}
@Override
public void produce(LongWritable byteOffset, Text line, KijiTableContext context)
    throws IOException {
  ...
  context.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, firstName);
  context.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, lastName);
  context.put(user, Fields.INFO_FAMILY, Fields.EMAIL, email);
  context.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, telephone);
  context.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, streetAddr);
}
{% endhighlight %}

The `context.put()` calls are identical in form to using a
[`KijiTableWriter`]({{site.api_schema_rc5}}/KijiTableWriter.html).

If you are writing a custom bulk importer and require specialized setup and teardown steps,
these can be placed in `setup()` and `cleanup()` methods like in a Mapper. We don't need
any in this example.

The outer `PhonebookImporter` class contains `configureJob(...)` and `run(...)` methods
that handle the setup and execution
of the MapReduce job.  Instead of constructing a Hadoop `Job` object directly, we use a
`KijiBulkImportJobBuilder`. This builder object lets us specify Kiji-specific arguments,
and construct a [`MapReduceJob`]({{site.api_mr_rc5}}/MapReduceJob.html) (A Kiji-specific wrapper around `Job`):

{% highlight java %}
MapReduceJob configureJob(Path inputPath, KijiURI tableUri) throws IOException {
  return KijiBulkImportJobBuilder.create()
      .withConf(getConf())
      .withInput(new TextMapReduceJobInput(inputPath))
      .withOutput(new DirectKijiTableMapReduceJobOutput(tableUri))
      .withBulkImporter(PhonebookBulkImporter.class)
      .build();
}
{% endhighlight %}

The HDFS file path to the sample input data is set to the first command line argument.
A [`KijiURI`]({{site.api_schema_rc5}}/KijiURI.html) is constructed that specifies the `phonebook` table as the target:

{% highlight java %}
public int run(String[] args) throws Exception {
  final KijiURI tableUri =
      KijiURI.newBuilder(String.format("kiji://.env/default/%s", TABLE_NAME)).build();
  final MapReduceJob job = configureJob(new Path(args[0]), tableUri);
}
{% endhighlight %}

The [`TextMapReduceJobInput`]({{site.api_mr_rc5}}/input/TextMapReduceJobInput.html) and
[`DirectKijiTableMapReduceJobOutput`]({{site.api_mr_rc5}}/output/DirectKijiTableMapReduceJobOutput.html)
classes are abstractions that, under the hood, configure an `InputFormat` and `OutputFormat`
for the MapReduce job. Different KijiMR job types (bulk importer, producer, or gatherer)
support different subsets of available formats (files, tables, etc). These classes allow the
system to ensure that the correct type is used. For example, bulk import jobs require that the
target is a table. "Regular" MapReduce jobs configured through
[`KijiMapReduceJobBuilder`]({{site.api_mr_rc5}}/KijiMapReduceJobBuilder.html) can use
any [`MapReduceJobInput`]({{site.api_mr_rc5}}/MapReduceJobInput.html) and
[`MapReduceJobOutput`]({{site.api_mr_rc5}}/MapReduceJobOutput.html) that makes sense
in the context of the application.

### Running the Example
First you'll need to put the text file containing your friends' contact info into HDFS.
You can do this by using the HDFS `-copyFromLocal` command:

<div class="userinput">
{% highlight bash %}
$HADOOP_HOME/bin/hadoop fs -copyFromLocal \
    $KIJI_HOME/examples/phonebook/input-data.txt /tmp
{% endhighlight %}
</div>

You can then use the `kiji jar` command to execute the MapReduce job.  To use
the jar command, specify the jar file the `PhonebookImporter` class and the
path to the `input-data.txt` file in HDFS.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc5.jar \
    org.kiji.examples.phonebook.PhonebookImporter \
    /tmp/input-data.txt
{% endhighlight %}
</div>

You now have data in your phonebook table!


#### Running with the bulkimport Command

The `BulkImportJobBuilder` allows you to programmatically configure and launch
a bulk import MapReduce job. KijiMR extends Kiji with a `bulkimport` command-line
tool that can perform substantially the same steps. The following command would
run the same bulk import job without requiring that you write a "`main()`" method:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji bulk-import \
    --importer='org.kiji.examples.phonebook.PhonebookImporter$PhonebookBulkImporter' \
    --input="format=text file=/tmp/input-data.txt" \
    --output="format=kiji nsplits=1 table=kiji://.env/default/phonebook" \
    --lib=$KIJI_HOME/examples/phonebook/lib
{% endhighlight %}
</div>

The `--input` and `--output` arguments specify in text form the same
[`MapReduceJobInput`]({{site.api_mr_rc5}}/MapReduceJobInput.html) and
[`MapReduceJobOutput`]({{site.api_mr_rc5}}/MapReduceJobOutput.html)
objects as are created programmatically in this example.

#### Verify
Verify that the user records were added properly by executing:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji scan kiji://.env/default/phonebook
{% endhighlight %}
</div>

Here's what the first entry should look like:

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228117784] info:firstname
                                     John
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228117787] info:lastname
                                     Doe
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228117789] info:email
                                     johndoe@gmail.com
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228117792] info:telephone
                                     202-555-9876
    entity-id=hbase=hex:551e50c1f2632437ccbacb16100f11db [1363228117793] info:address
                                     {"addr1": "1600 Pennsylvania Ave", "apt": null, "addr2": null, "city": "Washington", "state": "DC", "zip": 99999}
