kiji-framework
==============

This repository contains a definition for the entire Kiji framework.

Each module (KijiSchema, KijiMR, etc) can be depended upon individually by users. But if
you are depending on many such modules, it can be difficult to ensure that you have
selected compatible versions of each. This repository includes two pom files that you can
use to depend on the most recent consistent release of all Kiji components.

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

    <dependencies>
      <groupId>org.kiji.schema</groupId>
      <artifactId>kiji-schema</artifactId>
    </dependencies>

You will still need to specify which components you rely on. e.g., KijiSchema, KijiMR,
etc. You must also select a `kiji-platform` definition to specify which versions of
Hadoop and HBase you want to use.

For example:

    <dependencies>
      <groupId>org.kiji.platforms</groupId>
      <artifactId>kiji-cdh4-platform</artifactId>
    </dependencies>


Including The Entire Kiji Framework
-----------------------------------

Alternatively, you may declare a direct dependency on the entire Kiji framework.
This will include all Kiji client artifacts as part of your project's dependencies.
This may be more artifacts than you require, but is the simplest way to get started.

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


You will still need to declare a dependency on a specific Hadoop/HBase platform,
as described above.
