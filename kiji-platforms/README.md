kiji-platforms
==============

This repository contains Kiji "Hadoop platform" definitions. Several artifacts
are published in the org.kiji.platforms groupId; each of these is a Java
project that contains no code; but each contains dependencies on versions of
Hadoop and HBase that are known to work well together.

Kiji supports running on a variety of Hadoop distributions. At runtime, based
on the Hadoop and HBase jars in the `HADOOP_HOME` and `HBASE_HOME`
directories, Kiji will determine what compatibility layer to load in order to
work with different platforms.

Usage
-----

When you compile and test your application, you should do so against the
version of Hadoop that you intend to run. Your project should declare
provided-scope dependencies on KijiSchema, as well as a support platform. For
example, to run on CDH4:

    <dependencies>
      <dependency>
        <groupId>org.kiji.schema</groupId>
        <artifactId>kiji-schema</artifactId>
        <version>1.0.0</version>
        <scope>provided</scope>
      </dependency>

      <dependency>
        <groupId>org.kiji.platforms</groupId>
        <artifactId>kiji-cdh4-platform</artifactId>
        <version>1.3.0</version>
        <scope>provided</scope>
      </dependency>
    </dependencies>


Available platforms
-------------------

To ensure platform compatibility with Kiji, you should compile and deploy
against a Hadoop version specified in a Kiji platform definition. The
following platform definitions are provided:

* kiji-cdh4-platform (Latest Kiji-supported CDH4 release)
* kiji-cdh4-4-platform (Latest CDH4.4 update)
* kiji-cdh4-3-platform (Latest CDH4.3 update)
* kiji-cdh4-2-platform (Latest CDH4.2 update)
* kiji-cdh4-1-platform (Latest CDH4.1 update)
* kiji-hadoop1-hbase94-platform (Apache Hadoop 1.x and HBase 0.94.x)
* kiji-hadoop1-hbase92-platform (Apache Hadoop 1.x and HBase 0.92.x)

Platform compatibility
----------------------

Newer versions of KijiSchema may not work against older Hadoop distributions.
KijiSchema 1.0 can run against HBase 0.92-based platforms (kiji-cdh4-1 and
kiji-hadoop1-hbase92). KijiSchema 1.1 can run against HBase 0.94-backed
platforms (kiji-cdh4-2 and kiji-hadoop1-hbase94).

Maven usage notes
-----------------

If you depend on such a module, you can use the `mvn dependency:tree` command
to visualize the actual dependencies being used:

    [INFO] org.kiji.foo:foo:jar:1.0.0-SNAPSHOT
    [INFO] ...
    [INFO] \- org.kiji.platforms:kiji-cdh4-platform:jar:1.0.0:provided
    [INFO]    +- org.apache.hadoop:hadoop-client:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    +- org.apache.hadoop:hadoop-core:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    +- org.apache.hadoop:hadoop-minicluster:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    \- org.apache.hbase:hbase:jar:0.92.1-cdh4.1.2:provided
    [INFO]       +- ...


See the example-pom.xml file to see a concrete example of this being used.
