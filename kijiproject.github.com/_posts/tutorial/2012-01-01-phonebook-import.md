---
layout: post
title: Import Data
category: tutorial
tags: [article]
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
`$KIJI_HOME/examples/phonebook/input-data.txt` you will see records like:\

> John|Doe|johndoe@gmail.com|202-555-9876|{"addr1":"1600 Pennsylvania Ave","apt":null,"addr2":null,"city":"Washington","state":"DC","zip":99999}

The fields in each record are delimited by a "|", the last field is actually
 a complete JSON Avro record representing an address.

At the top of the `importLine(...)` method in `StandalonePhonebookImporter`,  you'll
see we're using a delimiter to split up the record into its' specific fields:
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

Next we create a unique [`EntityId`]({{site.api_url}}/EntityId.html) that will be used to reference this row.  As before, we will use
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
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc2.jar \
    org.kiji.examples.phonebook.StandalonePhonebookImporter \
    $KIJI_HOME/examples/phonebook/input-data.txt
{% endhighlight %}
</div>

You now have data in your phonebook table!

#### Verify
Verify that the user records were added properly by executing:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --table=phonebook
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
`PhonebookImporter`. PhonebookImporter defines a map-only MapReduce job that reads each
line of our input file, parses it, and writes it to a table.

The first thing we'll want to look at is the `setup()` method (which Hadoop will call before
executing any map tasks):

{% highlight java %}
/** {@inheritDoc} */
@Override
protected void setup(Context hadoopContext)
    throws IOException, InterruptedException {
  super.setup(hadoopContext);
  final Configuration conf = hadoopContext.getConfiguration();
  KijiURI tableURI;
  try {
    tableURI = KijiURI.parse(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI));
  } catch (KijiURIException kue) {
    throw new IOException(kue);
  }
  mKiji = Kiji.open(tableURI, conf);
  mTable = mKiji.openTable(TABLE_NAME);
  mWriter = mTable.openTableWriter();
}
{% endhighlight %}

This method sets up all the resources necessary for map tasks. Note that we use a different
way to specify Kiji table and instance names here, a [`KijiURI`]({{site.api_url}}KijiURI.html).
This newer way to specify Kiji instance addresses is more robust than specifying the instance
name as a string.

At the top of the map method, you'll see that we extract the fields from the lines as the above
example.  Then using the `KijiTableWriter` mWriter that is initialized earlier in setup,
we'll write all of the fields to the phonebook table:

{% highlight java %}
@Override
public void map(LongWritable byteOffset, Text line, Context context)
    throws IOException, InterruptedException {
  ...
  mWriter.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, firstName);
  mWriter.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, lastName);
  mWriter.put(user, Fields.INFO_FAMILY, Fields.EMAIL, email);
  mWriter.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, telephone);
  mWriter.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, streetAddr);
}
{% endhighlight %}

When the map task is complete, a `cleanup()` method will close the Kiji resources opened
in `setup()`.

The outer `PhonebookImporter` class contains a `run(...)` method that handles the setup
of the MapReduce job.  This is a typical MapReduce job setup. For detailed description
you can refer to [Accessing Data]({{site.userguide_url}}/accessing-data/).  While the
logic here closely resembles the non-MapReduce importer above, the heavy lifting of
configuring the MapReduce job is done within the `run(...)` method.

The mapper class is specified as the inner class `PhonebookImportMapper`:

{% highlight java %}
job.setMapperClass(PhonebookImportMapper.class);
{% endhighlight %}

The hdfs file path to the sample input data is set to the first command line argument here:

{% highlight java %}
FileInputFormat.setInputPaths(job, new Path(args[0]));
{% endhighlight %}

Also note that we've set the number of reduce tasks to 0 because  we don't have
any reduce tasks to perform.

{% highlight java %}
job.setNumReduceTasks(0);
{% endhighlight %}

Since we use the `KijiTableWriter` directly, and don't emit key-value pairs to an
OutputFormat, we disable this feature of MapReduce:

{% highlight java %}
job.setOutputFormatClass(NullOutputFormat.class);
{% endhighlight %}

### Running the Example
First you'll need to put the text file of friends into hdfs.  You can do this
by using the hdfs `-copyFromLocal` command:

<div class="userinput">
{% highlight bash %}
$HADOOP_HOME/bin/hadoop fs -copyFromLocal \
    $KIJI_HOME/examples/phonebook/input-data.txt /tmp
{% endhighlight %}
</div>

You can then use the `kiji jar` command to execute the MapReduce job.  To use
the jar command, specify the jar file the `PhonebookImporter` class and the
path to the `input-data.txt` file in hdfs.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc2.jar \
    org.kiji.examples.phonebook.PhonebookImporter \
    /tmp/input-data.txt
{% endhighlight %}
</div>

You now have data in your phonebook table!

#### Verify
Verify that the user records were added properly by executing:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --table=phonebook
{% endhighlight %}
</div>

Here's what the first entry should look like:

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352513223503] info:firstname
                                     John
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352513223504] info:lastname
                                     Doe
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352513223505] info:email
                                     johndoe@gmail.com
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352513223505] info:telephone
                                     202-555-9876
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352513223506] info:address
                                     {"addr1": "1600 Pennsylvania Ave", "apt": null, "addr2": null, "city": "Washington", "state": "DC", "zip": 99999}
