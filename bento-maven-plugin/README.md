Bento Cluster Plugin ${project.version}
=======================================

A Maven plugin to start and stop a Bento cluster running in a Docker container to be used in integration testing.

See the [bento cluster project](https://github.com/kijiproject/bento-cluster) for more on the Bento running in Docker.

Usage
-----

Ensure Docker is installed and that the user is in the `docker` user group.

Ensure that the `docker` branch of the `bento-cluster` is available on the local machine and pull `kijiproject/bento-cluster` from Docker Hub.

> `git clone git@github.com:kijiproject/bento-cluster /path/to/bento-cluster`

> `docker pull kijiproject/bento-cluster`

Ensure that the `maven-failsafe-plugin` and the `bento-maven-plugin` are part of your module's `pom.xml`, by inserting the following block:
```xml
      <plugin>
        <artifactId>maven-failsafe-plugin</artifactId>
        <version>2.17</version>
        <executions>
          <execution>
            <goals>
              <goal>integration-test</goal>
              <goal>verify</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.kiji.maven.plugins</groupId>
        <artifactId>bento-maven-plugin</artifactId>
        <executions>
          <execution>
            <goals>
              <goal>start</goal>
              <goal>stop</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
```


In order to run `bento-maven-plugin`, specify the following parameters when running `mvn install`.

`-Dbento.dir.path=/path/to/bento-cluster/` - Where on this machine is the bento-cluster? This parameter is mandatory.

`-Dskip=false` - Whether to skip setting up this plugin. Optional, defaults to `false`.

`-Dsite.files.dir.path=/path/to/site-files/` - Directory where to output generate site files to connect to HDFS, HBase, Yarn, etc. Optional, defaults to `target/test-classes/`.

`-Dpersist=false` - Should the start Bento cluster container persist after the integration test? Optional, defaults to `false`.

The most common use-case is:

> `mvn clean install -Dbento.dir.path=/path/to/bento-cluster/`
