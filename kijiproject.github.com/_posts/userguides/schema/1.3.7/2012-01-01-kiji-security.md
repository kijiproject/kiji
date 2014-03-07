---
layout: post
title: Kiji Security
categories: [userguides, schema, 1.3.7]
tags : [schema-ug]
version: 1.3.7
order : 6
description: Security and Access Features in Kiji.
---

## Permissions and Access Control in Kiji

### Requirements
Kiji security requires a secure HBase cluster.  Instructions for installing secure HBase can be
found [here](http://hbase.apache.org/book/security.html).

Secure Kiji, like secure HBase, uses the [Kerberos](http://web.mit.edu/kerberos/) authentication
system.

### Installing a Secure Kiji Instance

Kiji security is not installed by default.  If you have a secure HBase cluster, you can install a
new Kiji instance with security enabled by providing a path to a properties file:

    kiji install --kiji=kiji://localhost:2181/default \
      --properties-file=/path/to/kiji-properties.properties

In this properties file, properties are specified in the format:

    security-version = security-0.1

If any Kiji system properties are not specified, the default values are used.  The default
security-version is 'security-0.0', which is no security.  'security-0.1' is currently the only
other valid value for security-version, and it is our first, experimental step at providing for
secure Kiji instances.

There is currently no way to upgrade from a non-secure Kiji instance to a secure Kiji instance.


### security-0.1 Features Overview

Permissions for a Kiji instance:

READ allows reading from Kiji tables in the instance.

WRITE allows writing to Kiji tables in the instance.  It does not imply READ permission, which can
be granted separately.

GRANT allows granting permissions for the instance to other users.

The user that installs a Kiji instance automatically has GRANT permissions on the instance.

Permissions for a Kiji instance that was installed with security enabled can be modified using the
security module in the Kiji shell:

    kiji-schema-shell > MODULE security;
    kiji-schema-shell > GRANT READ WRITE PERMISSIONS ON INSTANCE ‘kiji://localhost:2181/default’
      TO USER fred;

See the userguide page for the DDL shell for more information on shell commands.

### Security in Other Modules in the Kiji Ecosystem
Security has been tested only with KijiSchema and KijiMR.  Other modules in the Kiji ecosystem may
not interact properly with a secure Kiji instance without some modification (They will not allow
access that hasn’t been explicitly granted, of course, but they may also fail on legal actions where
permission has been granted).

#### Security in KijiMR
Security in KijiMR should "just work", except that users without WRITE permissions on the instance
will not have their jobs recorded in the Job History Table.  For example, users with only READ
permissions on the instance will be able to run Gatherers, but those jobs will not be recorded.
