---
layout: post
title: Setup
categories: [userguides, rest, 1.2.1]
tags: [rest-ug]
order: 2
version: 1.2.1
description: Setup
---

KijiREST runs as part of the Kiji environment. A BentoBox cluster includes KijiREST, meaning nothing else
needs to be downloaded --  simply start the KijiREST server.

To run KijiREST on hardware independent of the HBase cluster, the KijiREST package includes
the necessary Kiji libraries required to access the cluster remotely.

### Setting up KijiREST on a local server

To run KijiREST in a production environment, see [Setup and Run (Production)](#setup-production).

### Set up

If you are using a BentoBox, set the environment variable `${KIJI_REST_HOME}` to point to the `rest` directory
within your BentoBox installation:

    export KIJI_REST_HOME=/path/to/bento/box/rest

If you are not using a BentoBox, download the KijiREST tarball from the Kiji
[Downloads](http://www.kiji.org/getstarted/#Downloads) page, untar the archive, and set `${KIJI_REST_HOME}` to
point to the root directory:

    export KIJI_REST_HOME=/path/to/tarball/kiji-rest-1.2.1

Within `${KIJI_REST_HOME}`, you should see the following:

<dl>
<dt>  bin/ </dt>
    <dd>Contains start/stop scripts to control the KijiREST application</dd>
<dt>conf/ </dt>
    <dd>Basic KijiREST and Dropwizard configuration settings</dd>
<dt>docs/ </dt>
    <dd>API docs</dd>
<dt>lib/ </dt>
    <dd>A placeholder directory of additional classpath jars (especially Avro classes)</dd>
<dt>README.md  </dt>
    <dd> A terse version of this document.</dd>
</dl>

(Non-BentoBox users will also see `kiji-rest-1.2.1.jar`.)

Installing a Kiji instance (see the [Get Started](http://www.kiji.org/getstarted/) page for details)
is necessary before running KijiREST (KijiREST cannot run on a system without an installed Kiji
instance).  We also recommend that users running the example commands in this tutorial first run
through one of the [Kiji tutorials](http://docs.kiji.org/tutorials.html).

### Startup with Basic Configuration

The KijiREST configuration parameters are located in
`${KIJI_REST_HOME}/conf/configuration.yml`
(`rest/conf/configuration.yml` for BentoBox users). This file is divided into
two major sections:
* The top portion of the file configures KijiREST
* The bottom portion configures [Dropwizard](http://dropwizard.codahale.com/) (including HTTP and logging).

To configure and run KijiREST for your cluster and instance:

1.  Start HBase and Hadoop with a configured Kiji environment. Make sure necessary Avro
classes are accessible either in `$KIJI_CLASSPATH` or in the `${KIJI_REST_HOME}/lib/` directory.

    If you are running a BentoBox, start Hadoop and HBase with `bento start`.

2.  Set the cluster key in `configuration.yml` to the URI of our Kiji
environment, for example `kiji://.env`.

3.  Set the instance key to the list of instances we would like to surface
through the REST service, for example `default`, `prod_instance`,
`dev_instance`:

        cluster: kiji://.env/
        instances:
         - default
         - prod_instance
         - dev_instance

4.  Start KijiREST.

        ~/REST$  ./bin/kiji-rest start

    The default `configuration.yml` file sets up the REST service through port 8080
    and writes all logs to the `${KIJI_REST_HOME}/logs`
    directory. The process will run in the background.

5. Check that KijiREST is running correctly.

        ~/REST$  ./bin/kiji-rest status
        Kiji REST is running. PID is 1234

    (You can also find the process ID in the `${KIJI_REST_HOME}/kiji-rest.pid` file.)

    If KijiREST is not running, consult the logs in `${KIJI_REST_HOME}/logs`.  Often if your KijiREST process
    stops during this stage in the setup, the reason is that KijiREST cannot find the Kiji instances
    described in `${KIJI_REST_HOME}/conf/configuration.yml`.  You will then see something similar to the
    following in `${KIJI_REST_HOME}/logs/console.out`:

            Exception in thread "main" org.kiji.schema.KijiNotInstalledException: Kiji instance
            kiji://localhost:2181/default/ is not installed.

    If so, run `kiji install` and try again.

### Get Instance Status
You can check on the status of Kiji instances using the Dropwizard healthcheck
portal (default port: 8081).

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

        $ tar -xzf kiji-rest-1.2.1-release.tar.gz -C /opt/wibi/
        $ ln -s kiji-rest-1.2.1 kiji-rest

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
