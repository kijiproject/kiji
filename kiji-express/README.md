# KijiExpress #

KijiExpress provides a simple data analysis language using
[KijiSchema](https://github.com/kijiproject/kiji-schema/) and
[Scalding](https://github.com/twitter/scalding/), allowing you to analyze data stored in Kiji
and other data stores.


## Getting Started ##

There are a couple different ways to get started with KijiExpress.  The easiest way is running a
script using KijiExpress shell.  You can also run KijiExpress on compiled jobs, or on
arbitrary jar files.

You can run KijiExpress in either local mode or HDFS mode.  Local mode uses Scalding's local mode
and runs the job locally on your machine.  In HDFS mode, the job runs on a cluster.


### Running Compiled Jobs ###

If you want, you can run compiled jobs as well.  You must have python3 and the python-base package installed. To install python3 on your machine we recommend using the pyenv project found at: https://github.com/yyuu/pyenv. In order to install the python base package use pip install python-base, if you're on a system whose 'python' is still python2, you may have to: $ sudo apt-get install python3-pip.

Once python is installed running a job requires that you write a Scalding Job and compile it yourself into a .jar file.  Then you can run your job with the command:

    express.py job path/to/your/jarfile.jar name.of.your.jobclass <any arguments to your job>

or

    express.py job path/to/your/jarfile.jar name.of.your.jobclass <any arguments to your job> --hdfs

You can see some examples of Jobs in the
[kiji-express-music tutorial](https://github.com/kijiproject/kiji-express-music).


### Running Arbitrary Jars ###

The `express` tool can also run arbitrary jars, with KijiExpress and its dependencies on the
classpath, with the command:

    express.py jar /path/to/your/jarfile.jar name.of.main.class <any arguments to your main class>

This requires you to have a Java or Scala main class.

Jars on the classpath are automatically added to the classpath of tasks run in MapReduce jobs.
If you want to add jars to the classpath, you can set the `EXPRESS_CLASSPATH` variable to a
colon-separated list of paths to your jars and they will be appended to the KijiExpress classpath.

To see the classpath that KijiExpress is running with, you can run:

    express.py classpath

### Interactive Shell ###
You can use the `express` tool to run a Scala REPL. You can use the shell to run and prototype
queries. The shell can be run in local mode (such that all jobs run in Cascading's local runner) or
in HDFS mode (such that all jobs run on a Hadoop cluster). Use

    express.py shell
    express.py shell --local

to use local mode, and

    express.py shell --hdfs

to use HDFS mode.

Scalding flows created in the REPL must be explicitly run. For example, this REPL query gets the
latest value from the column `info:track_plays` of a Kiji table and writes the results to a TSV
file.

    express> KijiInput("kiji://.env/default/users", "info:track_plays" -> 'playSlice)
    res0: org.kiji.express.flow.KijiSource = org.kiji.express.flow.KijiSource@5f8f9190
    express> res0.mapTo('playSlice -> 'play) { slice: KijiSlice[String] => slice.getFirstValue() }
    res1: cascading.pipe.Pipe = Each(org.kiji.express.flow.KijiSource@5f8f9190)[MapFunction[decl:'play']]
    express> res1.write(Tsv("songPlays"))
    res2: cascading.pipe.Pipe = Each(org.kiji.express.flow.KijiSource@4791e9e6)[MapFunction[decl:'play']]
    express> res2.run

## More Examples ##

See the [kiji-express-examples project](https://github.com/kijiproject/kiji-express-examples)
and [kiji-express-music tutorial](https://github.com/kijiproject/kiji-express-music)
for more detailed examples of KijiExpress computations.
