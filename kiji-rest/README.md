KijiREST ${project.version}
===========================

KijiREST is a REST interface for interacting with KijiSchema.

For more information about KijiREST, see
[the KijiREST user guide](http://docs.kiji.org/userguides.html).

For more information about KijiSchema, see
[the Kiji project homepage](http://www.kiji.org).

Further documentation is available at the Kiji project
[Documentation Portal](http://docs.kiji.org)

Installing KijiREST (requires root privileges)
--------------------------------

* It's assumed that where KijiREST is unpacked is called $KIJI\_REST\_HOME By default, this is
assumed to be /opt/wibi/kiji-rest. This can be changed by modifying the KIJI\_REST\_HOME variable
in bin/kiji-rest.initd script.
* Create a non-privileged user called "kiji" which will be used to run the service. This can be
changed by modifying the KIJI\_REST\_USER variable in bin/kiji-rest.initd script.
  * sudo useradd kiji
* Copy $KIJI\_REST\_HOME/bin/kiji-rest.initd as /etc/init.d/kiji-rest
  * sudo cp $KIJI\_REST\_HOME/bin/kiji-rest.initd /etc/init.d/kiji-rest
* chkconfig --add kiji-rest

Starting a local KijiREST server
--------------------------------

Any relevant Avro classes that are necessary for interaction of KijiREST with the underlying Kiji
tables must be included on the classpath upon instantiation
of the server. This is done by placing the jar containing the necessary Avro classes in the
$KIJI\_REST\_HOME/lib folder.

$ cd $KIJI\_REST\_HOME

$ ./bin/kiji-rest start

This will launch the service in the background with the pid of the process located in
$KIJI\_REST\_HOME/kiji-rest.pid. The application and request logs can be found
under $KIJI\_REST\_HOME/logs.

### Alternatively as root:
$ /sbin/service kiji-rest start

Stopping a local KijiREST server
--------------------------------

### If run as non-root:
$ cat $KIJI\_REST\_HOME/kiji-rest.pid | xargs kill

### As root:
$ /sbin/service kiji-rest stop

Setting up configuration.yml
----------------------------

The configuration.yml file (located in $KIJI\_REST\_HOME/conf/configuration.yml) is a YAML file used
to configure the KijiREST server. The following key is required:

- "cluster" is the base cluster's kiji URI.

The following is an example of the contents of a proper configuration.yml file:

"cluster" : "kiji://localhost:2181/" #The base cluster URI

There is optional key to turn on global cross-origin resource sharing (CORS).

"cors" : "true" #If not set, defaults to false

"cacheTimeout" sets the timeout in minutes before clearing the cache of instances and tables

"remote-shutdown" #when set to true, the admin task to shutdown the kiji-rest via REST command is enabled

KijiREST is implemented using DropWizard. See
[Dropwizard's User Manual](http://dropwizard.codahale.com/manual/core/#configuration-defaults)
for additional Dropwizard-specific configuration options such as server settings
and logging options (console-logging, log files, and syslog).

Creating a KijiRest Plugin
--------------------------

To create a KijiRest plugin project from the maven archetype, use the following command:

mvn archetype:generate \
-DarchetypeCatalog=https://repo.wibidata.com/artifactory/kiji-packages/archetype-catalog.xml

From there you can choose the org.kiji.rest:plugin-archetype (A skeleton KijiRest plugin project.)
option.

Issues and mailing lists
------------------------

Users are encouraged to join the Kiji mailing lists: user@kiji.org and dev@kiji.org (for developers).

Please report your issues at [the KijiREST JIRA project](https://jira.kiji.org/browse/REST).
