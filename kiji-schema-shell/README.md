

Introduction
============

The KijiSchema DDL Shell implements a _layout definition language_ that
allows you to create, modify, inspect, and delete kiji table layouts. A
_table layout_ is the KijiSchema data dictionary element that defines a set
of column families, qualifiers, etc. and associates these columns with
Avro schemas.

With this shell, you can quickly create tables to use in your KijiSchema
applications. You can add, drop, or modify columns quickly too. These
modifications can be performed without any downtime associated with the
table; the only exception is that modifying a locality group causes
the table to be taken offline while applying changes. But locality group
changes should be rare in running applications.

Finally, this shell allows you to quickly understand the layout of an
existing table. The `DESCRIBE` command will pretty-print the layout of a
table, eliminating the guesswork as to where your coworker has stashed an
important piece of data.


For more information about KijiSchema and the Kiji system, see
[www.kiji.org](http://www.kiji.org).


Running
=======

* Export `$KIJI_HOME` to point to your KijiSchema installation.
  If you source `kiji-env.sh` in the BentoBox, this is taken care of.
* Run `bin/kiji-schema-shell`

This command takes a few options (e.g., to load a script out of a file).
See `bin/kiji-schema-shell --help` for all the available options.

An Example
----------

    CREATE TABLE users WITH DESCRIPTION 'some data'
    ROW KEY FORMAT (username STRING)
    WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
      MAXVERSIONS = INFINITY,
      TTL = FOREVER,
      INMEMORY = false,
      COMPRESSED WITH GZIP,
      FAMILY info WITH DESCRIPTION 'basic information' (
        name "string" WITH DESCRIPTION 'The user\'s name',
        email "string",
        age "int"),
      MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
    );

    ALTER TABLE users ADD MAP TYPE FAMILY strings { "type" : "string" }
      WITH DESCRIPTION 'ad-hoc string data' TO LOCALITY GROUP default;

    SHOW TABLES;
    DESCRIBE EXTENDED users;
    DROP TABLE users;
    SHOW TABLES;


Language Reference
==================

To read a full reference manual for this component, see 
[The DDL reference in the online KijiSchema user guide](http://docs.kiji.org/userguides/schema/${project.version}/schema-shell-ddl-ref/).

API
===

You can use the `org.kiji.schema.shell.api.Client` object to programmatically
invoke DDL commands from a Java or Scala-based system.

Example:

    import org.kiji.schema.KijiURI;
    import org.kiji.schema.shell.api.Client;

    void exampleMethod() {
      Client client = Client.newInstance(KijiURI.parse("kiji://.env/default"));
      try {
        // One or more calls to executeUpdate(). These will throw DDLException
        // if there's a problem with your DDL statement.
        client.executeUpdate("DROP TABLE t");
      } finally {
        // Always call close when you're done.
        client.close();
      }
    }


You will need to include this artifact in your project's dependency list.
For maven users:

    &lt;dependency&gt;
      &lt;groupId&gt;org.kiji.schema-shell&lt;/groupId&gt;
      &lt;artifactId&gt;kiji-schema-shell&lt;/artifactId&gt;
      &lt;version&gt;${project.version}&lt;/version&gt;
    &lt;/dependency&gt;

