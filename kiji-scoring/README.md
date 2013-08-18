kiji-scoring ${project.version}
===============================

A module for applying trained models to score Kiji entities in real-time.

If running from inside a BentoBox, you'll want to add the `kiji-scoring/lib`
directory to your KIJI\_CLASSPATH. Assuming `$KIJI_HOME` is set to the root
of the BentoBox, you can execute the following:

`export KIJI_CLASSPATH=${KIJI_HOME}/kiji-scoring/lib/*:${KIJI_CLASSPATH}`

This will make the `kiji fresh` command available. The `fresh` tool takes in
a KijiURI and requires a `--do=<command>` flag with one of the following
commands:

* `register` for registering fresheners.
* `unregister` for removing an installed freshener.
* `retrieve` for describing the freshener attached to a column or map-family.
* `retrieve-all` for listing all fresheners registered to a table.
* `validate` for running sanity checks on a freshener.
* `validate-all` for sanity checking all fresheners registered to a table.

For more information consult `kiji fresh --help` and the KijiScoring javadocs
on the Kiji Project [Documentation Portal](http://docs.kiji.org).
