KijiREST ${project.version}
===========================

KijiREST is a REST interface for interacting with KijiSchema.

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
to configure the KijiREST server. The following keys are required:

- "cluster" is the base cluster's kiji URI.

- "instance" is an array of instances which are allowed to be visible to users of this REST service.
All instances specified here must exist.

The following are example contents of a proper configuration.yml file:

"cluster" : "kiji://localhost:2181/" #The base cluster URI <br />
"instances" : ["default", "prod", "dev", "test"] #Visible instances

KijiREST is implemented using DropWizard. See
[Dropwizard's User Manual](http://dropwizard.codahale.com/manual/core/#configuration-defaults)
for additional Dropwizard-specific configuration options such as server settings
and logging options (console-logging, log files, and syslog).


Development Warning
-------------------

This project is still under heavy development and has not yet had a formal release.
The APIs and code in this project may change in severely incompatible ways while we
redesign components for their presentation-ready form.

End users are advised to not depend on any functionality in this repository until a
release is performed. See [the Kiji project homepage](http://www.kiji.org) to download
an existing release of KijiSchema, and follow [@kijiproject](http://twitter.com/kijiproject)
for announcements of future releases, including KijiREST.

Issues are being tracked at [the Kiji JIRA instance](https://jira.kiji.org/browse/SCHEMA).
