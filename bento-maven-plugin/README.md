Bento Maven Plugin
==================

A Maven plugin to start and stop a Bento cluster running in a Docker container to be used in integration testing.

See the [bento cluster project](https://github.com/kijiproject/bento-cluster) for more on the Bento running in Docker.

Installation
------------

Dependencies:

- [Python 3](https://www.python.org) (we recommend installing python with
  [pyenv](https://github.com/yyuu/pyenv))
- [Docker](https://www.docker.com)

Usage
-----

Ensure Docker is installed and that the current user is in the `docker` user group.

Ensure that the `maven-failsafe-plugin` and the `bento-maven-plugin` are part of your module's
`pom.xml`, by inserting the following block:
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


The plugin exposes the following properties for adjusting its behavior:

Property                 | Type      | Description
------------------------ | --------- | -----------
`bento.name`             | `string`  | Name of the bento instance to use. If not specified, a random name will be generated.
`bento.docker.address`   | `string`  | Address of the docker daemon to use to manage bento instances.
`bento.skip`             | `boolean` | Skips all goals of the bento-maven-plugin.
`bento.skip.create`      | `boolean` | Skips creating the bento instance. Must be used in conjunction with an externally created bento instance specified through the `bento.name` property.
`bento.skip.start`       | `boolean` | Skips starting the bento instance. Must be used in conjunction with an externally created and started bento instance specified through the `bento.name` property.
`bento.skip.stop`        | `boolean` | Skips stopping the bento instance.
`bento.skip.rm`          | `boolean` | Skips deleting the bento instance.
`bento.conf.dir`         | `string`  | Directory to place configuration files in. Defaults to the test-classes so that configuration files are on the classpath.
`bento.venv`             | `string`  | Python venv root to install the bento cluster to. If not specified, a random name will be generated.
`bento.platform`         | `string`  | The version of the hadoop/hbase stack to run in the bento cluster.
`bento.pypi.repository`  | `string`  | Pypi repository to install kiji-bento-cluster from.
`bento.timeout.start`    | `long`    | Amount of time to wait for the bento cluster to start.
`bento.timeout.stop`     | `long`    | Amount of time to wait for the bento cluster to stop.
`bento.timeout.interval` | `long`    | Interval at which to poll the bento cluster's status when starting or stopping it.


These properties can be specified on the command line by placing a `-D` in front of the property
name and adding a `=` followed by its value:

    mvn clean verify -Dproperty.name="property value" ...
    
    
Development
-----------

This section of the README's code/examples may reference names in between `<` and `>`. These names
(including the `<` and `>`) represent portions for the user to provide.

Pypi-server
===========

To build and test packaging and deploying bento-cluster we recommend using pypi-server. Pypi-server
can also be used to test changes to bento-cluster in bento-maven-plugin by deploying the new
bento-cluster to the local pypi-server and setting the
`-D bento.pypi.repository=http://localhost:8080/simple` flag when running integration tests with
maven.

To install pypi-server:

```bash
pip3 install pypiserver

# For upload access control, you will need the 'htpasswd' command:
pip3 install passlib
aptitude install apache2-utils

# If you plan on using authbind (see below).
aptitude install authbind
```

Create a username and password and password to authenticate with (this is required right now due to
a bug in pypi-server):

```bash
htpasswd /<path-to-pypi-server-config>/pypi.htpasswd ${HTPASSWD_LOGIN}

# You will be prompted for the password to assign to the specified username
```

Create a ~/.pypirc file :

    [distutils]
    index-servers =
        local

    [local]
    username: <htpasswd-login>
    password: <htpasswd-password>
    repository: http://localhost:8080/simple

And start it with:

```bash
pypi-server \
    --port=8080 \
    --passwords=/path-to-pypi-server-config/pypi.htpasswd \
    /path-to-pypi-server-config/packages/
```
