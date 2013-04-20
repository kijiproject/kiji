# KijiExpress #

KijiExpress provides a simple data analysis language using
[KijiSchema](https://github.com/kijiproject/kiji-schema/) and
[Scalding](https://github.com/twitter/scalding/), allowing you to analyze data stored in Kiji
and other data stores.


## Getting Started ##

There are a couple different ways to get started with KijiExpress.  The easiest way is running a
script using the uncompiled scripts runner.  You can also run KijiExpress on compiled jobs, or on
arbitrary jar files.

You can run KijiExpress in either local mode or HDFS mode.  Local mode uses Scalding's local mode
and runs the job locally on your machine.  In HDFS mode, the job runs on a cluster.


### Running Uncompiled Scripts ###

Write a script:

    // This is a KijiExpress wordcount script!
    import org.kiji.express.DSL._

    // Read from the "columnfamily:inputqualifier" column of your Kiji table
    KijiInput("kiji://your/kiji/uri")("columnfamily:inputqualifier" -> 'word)
        // Group by the count of each word
        .groupBy('word) { words => words.size('count) }
        // Write the counts out to an output file
        .write(Tsv("outputFile"))

To run a script, use the command:

    express script /path/to/scriptfile

or, to run it in hdfs mode:

    express script /path/to/scriptfile --hdfs


In addition to the KijiInput and KijiOutput functions provided in our DSL (see the scaladocs of
the DSL object for more about reading from and writing to Kiji tables), you can use any of
[Scalding's provided sources](https://github.com/twitter/scalding/wiki/Scalding-Sources), which
includes useful sources that can read from and write to TSV, CSV, and other file formats.


### Running Compiled Jobs ###

If you want, you can run compiled jobs as well.  This requires that you write a Scalding Job and
compile it yourself into a .jar file.  Then you can run your job with the command:

    express job path/to/your/jarfile.jar name.of.your.jobclass <any arguments to your job>

or

    express job path/to/your/jarfile.jar name.of.your.jobclass <any arguments to your job> --hdfs

You can see some examples of Jobs in the
[kiji-express-music tutorial](https://github.com/kijiproject/kiji-express-music).


### Running Arbitrary Jars ###

The `express` tool can also run arbitrary jars, with KijiExpress and its dependencies on the
classpath, with the command:

    express jar /path/to/your/jarfile.jar name.of.main.class <any arguments to your main class>

This requires you to have a Java or Scala main class.

Jars on the classpath are automatically added to the classpath of tasks run in MapReduce jobs.
If you want to add jars to the classpath, you can set the `EXPRESS_CLASSPATH` variable to a
colon-separated list of paths to your jars and they will be appended to the KijiExpress classpath.

To see the classpath that KijiExpress is running with, you can run:

    express classpath


## More Examples ##

See the [kiji-express-examples project](https://github.com/kijiproject/kiji-express-examples)
and [kiji-express-music tutorial](https://github.com/kijiproject/kiji-express-music)
for more detailed examples of KijiExpress computations.
