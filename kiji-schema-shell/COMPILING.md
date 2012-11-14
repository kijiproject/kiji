
Compiling
=========

    mvn install

This depends on KijiSchema. If you depend on local changes to KijiSchema, you
will need to run `mvn install` in that project first.

Running
=======

* Export `$KIJI_HOME` to point to your KijiSchema installation.
* Run `bin/kiji-schema-shell`

This command takes a few options (e.g., to load a script out of a file).
See `bin/kiji-schema-shell --help` for all the available options.
