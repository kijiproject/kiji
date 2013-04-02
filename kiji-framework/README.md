kiji-framework
==============

This repository contains a definition for the entire Kiji framework.

Each module (KijiSchema, KijiMR, etc) can be depended upon individually by users. But
if you are depending on many such modules, it can be difficult to ensure that you
have selected compatible versions of each. This repository includes two pom files
that you can use to depend on the most recent consistent release of all Kiji
components.


Including The Entire Kiji Framework
-----------------------------------

The easiest way to get started is to declare a direct dependency on the entire Kiji
framework.  This will include all Kiji client artifacts as part of your project's
dependencies.  This may be more artifacts than you require, but is the simplest way
to get started.

To depend on the entire framework:

    <dependencies>
      ...

      <dependency>
        <groupId>org.kiji.framework</groupId>
        <artifactId>kiji-framework</artifactId>
        <version>1.0.0</version>
      </dependency>

      ...
    </dependencies>


This will automatically depend on the `kiji-cdh4-platform`. You can choose a
different platform if you'd like by explicitly depending on one.

You will also need to tell Maven where to find Kiji components. You should add
a `<repositories>` block like so:

    <repositories>
      <repository>
        <id>kiji-repos</id>
        <name>kiji-repos</name>
        <url>https://repo.wibidata.com/artifactory/kiji</url>
      </repository>
    </repositories>

Note that if you need to depend on any Kiji test resources, you must do so
explicitly; test dependencies cannot be used transitively.

For example:

    <dependencies>
      ...

      <dependency>
        <groupId>org.kiji.testing</groupId>
        <artifactId>fake-hbase</artifactId>
        <version>0.0.4</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.kiji.schema</groupId>
        <artifactId>kiji-schema</artifactId>
        <version>1.0.0</version>
        <type>test-jar</type>
        <scope>test</scope>
      </dependency>

      ...
    </dependencies>

If instead you used `framework-pom` as your parent pom, you could include these
dependencies without explicitly specifying the version and scope, because of
the shared `<dependencyManagement>` section.

The `framework-pom` parent also allows you to pick and choose which Kiji modules you
depend on (for example, KijiSchema but not KijiMR). See the next section for more
details.


Selecting Individual Components
-------------------------------

If you want to have control over which individual components are included in your
project, you should declare the following as the parent pom:

    <parent>
      <groupId>org.kiji.framework</groupId>
      <artifactId>framework-pom</artifactId>
      <version>1.0.0</version>
    </parent>


This includes a `<dependencyManagement>` section that describes the version numbers of
compatible Kiji framework components. Your pom file's `<dependencies>` section can then
include just the `groupId` and `artifactId` components for each dependency; the version
will be populated from the parent pom.

For example:

    <dependency>
      <groupId>org.kiji.schema</groupId>
      <artifactId>kiji-schema</artifactId>
    </dependency>

You will still need to specify which components you rely on. e.g., KijiSchema, KijiMR,
etc. You must also select a `kiji-platform` definition to specify which versions of
Hadoop and HBase you want to use.

For example:

    <dependency>
      <groupId>org.kiji.platforms</groupId>
      <artifactId>kiji-cdh4-platform</artifactId>
    </dependency>

