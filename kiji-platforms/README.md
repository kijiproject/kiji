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
example, to run on CDH5.1:

    <dependencies>
      <dependency>
        <groupId>org.kiji.schema</groupId>
        <artifactId>kiji-schema</artifactId>
        <version>1.6.0</version>
        <scope>provided</scope>
      </dependency>

      <dependency>
        <groupId>org.kiji.platforms</groupId>
        <artifactId>kiji-cdh5-1-platform</artifactId>
        <version>1.4.0</version>
        <scope>provided</scope>
      </dependency>
    </dependencies>


Available platforms
-------------------

To ensure platform compatibility with Kiji, applications should compile and
deploy against a Hadoop version specified in a Kiji platform definition. The
following platform definitions are provided:

* kiji-cdh5-1-platform (Latest CDH5.1 update)
* kiji-cdh5-0-platform (Latest CDH5.0 update)
* kiji-cdh4-4-platform (Latest CDH4.4 update)
* kiji-cdh4-3-platform (Latest CDH4.3 update)
* kiji-cdh4-2-platform (Latest CDH4.2 update)
* kiji-cdh4-1-platform (Latest CDH4.1 update)

See the `example-application-pom.xml` file for an example.

Test platforms
--------------

Many Kiji components provide test frameworks for writing unit or integration
tests against interactions with Kiji, and Hadoop. To use these test frameworks,
depend on the corresponding `kiji-<version>-test-platfrom` in `test` scope.

Platform compatibility
----------------------

All versions of KijiSchema may not work against all Hadoop platform versions.
The version of KijiPlatforms depended on by the KijiSchema version should contain
only compatible version.

Library dependencies
--------------------

Libraries which rely on KijiSchema or other Kiji dependencies should include a
dependency on the `kiji-compile-platform` in `provided` scope and
`kiji-test-platform` in `test` scope. This will supply the required
dependencies at compile time while allowing downstream applications the
flexibility to specify a different HBase and Hadoop version at runtime. See the
`example-library-pom.xml` file for an example.

Maven usage notes
-----------------

If you depend on such a module, you can use the `mvn -f example-application-pom.xml dependency:tree`
command to visualize the actual dependencies being used:

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
