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

When you compile and test your application, you should do so against the
version of Hadoop that you intend to run. Your project should declare
provided-scope dependencies on KijiSchema, as well as a support platform. For
example, to run on CDH4:

    <dependencies>
      <dependency>
        <groupId>org.kiji.schema</groupId>
        <artifactId>kiji-schema</artifactId>
        <version>1.0.0-rc2</version>
        <scope>provided</scope>
      </dependency>

      <dependency>
        <groupId>org.kiji.platforms</groupId>
        <artifactId>kiji-cdh4-platform</artifactId>
        <version>1.0.0</version>
        <scope>provided</scope>
      </dependency>
    </dependencies>

If you depend on such a module, you can use the `mvn dependency:tree` command
to visualize the actual dependencies being used:

    [INFO] org.kiji.foo:foo:jar:1.0.0-SNAPSHOT
    [INFO] ...
    [INFO] \- org.kiji.platforms:kiji-cdh4-platform:jar:1.0.0-rc2-SNAPSHOT:provided
    [INFO]    +- org.apache.hadoop:hadoop-client:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    +- org.apache.hadoop:hadoop-core:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    +- org.apache.hadoop:hadoop-minicluster:jar:2.0.0-mr1-cdh4.1.2:provided
    [INFO]    |  +- ...
    [INFO]    \- org.apache.hbase:hbase:jar:0.92.1-cdh4.1.2:provided
    [INFO]       +- ...


See the example-pom.xml file to see a concrete example of this being used.
