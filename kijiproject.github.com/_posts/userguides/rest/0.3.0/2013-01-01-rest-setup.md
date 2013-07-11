---
layout: post
title: Setup
categories: [userguides, rest, 0.3.0]
tags: [rest-ug]
order: 2
version: 0.3.0
description: Setup
---

KijiREST runs as part of the Kiji environment. A BentoBox cluster includes KijiREST meaning nothing else
needs to be downloaded --  simply start the KijiREST server.

To run KijiREST on hardware independent of the HBase cluster, the KijiREST package includes
the necessary Kiji libraries required to access the cluster remotely.

## Setting up KijiREST on a local server

To run KijiREST in a production environment, see [Setup and Run (Production)](#setup-production).

### Download the Package

Skip this step if running from within a Kiji BentoBox.

The KijiREST tarball can be found on the Kiji [Downloads](http://www.kiji.org/getstarted/#Downloads)
page. Unpacked, the KijiREST contents are:

    $ cd ~/kiji-rest-0.3.0
    $ ls .

<dl>
<dt>  bin/ </dt>
    <dd>Contains start/stop scripts to control the KijiREST application</dd>
<dt>conf/ </dt>
    <dd>Basic KijiREST and Dropwizard configuration settings</dd>
<dt>docs/ </dt>
    <dd>API docs</dd>
<dt>lib/ </dt>
    <dd>A placeholder directory of additional classpath jars (especially Avro classes)</dd>
<dt>kiji-rest-0.3.0.jar  </dt>
    <dd>The KijiREST executable.</dd>
<dt>README.md  </dt>
    <dd> A terse version of this document.</dd>
</dl>

### Startup with Basic Configuration

KijiREST configuration parameters are located in `kiji-rest/conf/configuration.yml`. This
file is divided into two major sections:
* The top portion of the file contains configurations relevant to KijiREST
* The bottom portion configures Dropwizard (including the HTTP and logging configurations).

To configure and run KijiREST for your cluster and instance:

1.  Start HBase and Hadoop with a configured Kiji environment. Make sure necessary Avro
classes are accessible either in `$KIJI_CLASSPATH` or in the `kiji-rest-0.3.0/lib/` directory.

    If you are running a BentoBox, start Hadoop and HBase with `bento start`.

2.  Set the cluster key to the URI of our Kiji environment, e.g. `kiji://.env`.

3.  Set the instance key to the list of instances we would like to surface through the REST
service, for example default, prod_instance, dev_instance:

        cluster: kiji://.env/
        instances:
         - default
         - prod_instance
         - dev_instance

4.  Start KijiREST.

        ~/kiji-rest-0.3.0$  ./bin/kiji-rest

The vanilla `configuration.yml` file sets up the REST service through port 8080 and writes
all logs to the `kiji-rest/logs` directory. The process will run in the background.

You can find the process ID in the `kiji-rest.pid` file:

    ~/kiji-rest-0.3.0$  cat kiji-rest.pid
    1234

### Get Instance Status
You can check on the status of Kiji instances using Dropwizard's healthcheck portal
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

1. Unpack the `kiji-rest` tarball as the `/opt/wibi/kiji-rest directory`:

        $ tar -xzf kiji-rest-0.3.0-release.tar.gz -C /opt/wibi/
        $ ln -s kiji-rest-0.3.0 kiji-rest

    The KijiREST package includes KijiSchema libraries need to be able to access Kiji tables.

1. Add the KijiREST user (with appropriately limited permissions) named "kiji".

        $ sudo useradd kiji

1. Move this script to `/etc/init.d/kiji-rest` and register it as a service with `chkconfig`.

        $ sudo cp /opt/wibi/kiji-rest/bin/kiji-rest.initd  /etc/init.d/kiji-rest
        $ chkconfig --add kiji-rest

To start the service, run:

    $ /sbin/service kiji-rest start

To stop the service, run:

    $ /sbin/service kiji-rest stop
