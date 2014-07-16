KijiMR
======

KijiMR is a framework for writing MapReduce-based computation
over data stored in KijiSchema.

For more information about KijiSchema, see
[the Kiji project homepage](http://www.kiji.org).

Further documentation is available at the Kiji project
[Documentation Portal](http://docs.kiji.org)

Issues are being tracked at [the Kiji JIRA instance](https://jira.kiji.org/browse/KIJIMR).

Note for Cassandra users
------------------------

The `kiji-mapreduce-cassandra` submodule of this project contains the implementation of a
Cassandra-backed version of KijiMR.  Because Cassandra (and therefore also the Cassandra version of
KijiMR) requires Java 7, we have disabled building this submodule by default.

If you have Java 7, you may build the Cassandra version of KijiMR by using the "cassandra"
profile:

    mvn clean package -Pcassandra
