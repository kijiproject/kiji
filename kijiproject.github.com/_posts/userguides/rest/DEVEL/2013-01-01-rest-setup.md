---
layout: post
title: Setup
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 5
version: devel
description: Setup
---

KijiREST runs as part of the Kiji environment. A BentoBox cluster includes KijiREST: you don't
need to download anything else, just enable the KijiREST server.
If you want to run KijiREST on hardware separate from your
cluster, the KijiREST package includes the Kiji libraries need to access the cluster remotely.

## Setting up KijiREST on a Local Server

Follow these steps to get KijiREST running in your development environment. If you are ready
to run KijiREST in a production environment, see [Setup and Run (Production)](#setup-production).

### Download the Package

If you are already running a Kiji BentoBox, you can skip this step.

The KijiREST tarball can be found on the Kiji [Downloads](http://www.kiji.org/getstarted/#Downloads)
page. Unpacked, the KijiREST contents are:

    ~/kiji-rest-0.1.0$  ls .

<dl>
<dt>  bin/ </dt>
    <dd>Contains initialization/start/stop scripts that we will use to control the KijiREST application</dd>
<dt>conf/ </dt>
    <dd>Basic KijiREST and Dropwizard configuration settings</dd>
<dt>docs/ </dt>
    <dd>API docs</dd>
<dt>lib/ </dt>
    <dd>A placeholder directory of additional classpath jars (especially Avro classes)</dd>
<dt>kiji-rest-0.1.0.jar  </dt>
    <dd>The KijiREST executable.</dd>
<dt>README.md  </dt>
    <dd> A terse version of this document.</dd>
</dl>

### Startup with Basic Configuration

KijiREST configuration parameters are located in `kiji-rest/conf/configuration.yml`. This
file is divided into two major sections: The top portion of the file contains configurations
relevant to KijiREST and the bottom portion configures Dropwizard (including the HTTP and
logging configurations).

To configure and run KijiREST for your cluster and instance:

1.  Start HBase and Hadoop with a configured Kiji environment. Make sure necessary Avro
classes are accessible either in `$KIJI_CLASSPATH` or in the `kiji-rest-0.1.0/lib/` directory.

    If you are running a BentoBox, you can start Hadoop and HBase with `start bento`.

1.  Set the cluster key to the URI of our Kiji environment, e.g. `kiji://.env`.

1.  Set the instance key to the list of instances we would like to surface through the REST
service, for example default, prod_instance, dev_instance:

        cluster: kiji://.env/
        instances:
         - default
         - prod_instance
         - dev_instance

1.  Start KijiREST.

        ~/kiji-rest-0.1.0$  ./bin/kiji-rest

The vanilla `configuration.yml` file sets up the REST service through port 8080 and writes
all logs to the `kiji-rest/logs` directory. The process will run in the background.

You can find the process ID in the `kiji-rest.pid` file:

    ~/kiji-rest-0.1.0$  cat kiji-rest.pid
    1234

### Get Instance Status
You can check on the status of Kiji instances using Dropwizard’s healthcheck portal
(default port: 8081).

    $ curl http://localhost:8081/healthcheck
    * deadlocks: OK
    * kiji://localhost:2182/default/: OK
    * kiji://localhost:2182/dev_instance/: OK
    * kiji://localhost:2182/prod_instance/: OK

### Logging

There are three types of logs in the logs directory:

    $  ls logs/

<dl>
<dt>app.log</dt>
    <dd>Contains Kiji client logs.</dd>
<dt>requests.log</dt>
    <dd>Contains HTTP logs.</dd>
<dt>console.out</dt>
    <dd>Any other logs which may have been sent to the console.</dd>
</dl>



The logging options (console-logging, log files, syslog, archiving, etc.) are set in the
Dropwizard section of `configuration.yml`. The available options and defaults are detailed
in the [Dropwizard User Manual](http://dropwizard.codahale.com/manual/).

<a id="setup-production"> </a>
## Setup and Run (Production)

Installing KijiREST in a production environment has additional considerations beyond those
described for development systems.

1. Unpack the `kiji-rest` tarball as the `/opt/kiji/kiji-rest directory`:

        $ tar -xvzf kiji-rest-0.1.0-release.tar.gz -C /opt/kiji/
        $ ln -s kiji-rest-0.1.0 kiji-rest

    The KijiREST package includes KijiSchema libraries need to be able to access Kiji tables.

1. Add the KijiREST user (with appropriately limited permissions) named “kiji”.

        $ sudo useradd kiji

1. Edit `kiji-rest/bin/kiji-rest.initd` to update `KIJI_REST_HOME` and `KIJI_REST_USER`.

        KIJI_REST_USER="kiji"
        KIJI_REST_HOME="/opt/kiji/kiji-rest"

1. Move this script to `/etc/init.d/kiji-rest` and register it as a service with `chkconfig`.

        $ sudo cp /opt/kiji/kiji-rest/bin/kiji-rest.initd  /etc/init.d/kiji-rest
        $ chkconfig --add kiji-rest

To start the service, run:

    $ /sbin/service kiji-rest start

To stop the service, run:

    $ /sbin/service kiji-rest stop

Customize these scripts (found in `$KIJI_REST_HOME/src/main/scripts/kiji-rest.initd`) as necessary.

