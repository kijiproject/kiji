KijiREST ${project.version}
===========================

KijiREST is a REST interface for interacting with KijiSchema.

For more information about KijiSchema, see
[the Kiji project homepage](http://www.kiji.org).

Further documentation is available at the Kiji project
[Documentation Portal](http://docs.kiji.org)

Starting a local KijiREST server
--------------------------------

KijiREST is built as an executable JAR and therefore can be run via:

$ java -jar target/kiji-rest-${project.version}-SNAPSHOT.jar server
        target/test-classes/configugation.yml

Any relevant Avro classes that are necessary for interaction of KijiREST with
the underlying Kiji tables must be included on the classpath upon instantiation
of the server.

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

