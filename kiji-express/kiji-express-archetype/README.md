KijiExpress Archetype
===========================

This Maven archetype allows a KijiExpress user to create a basic KijiExpress project, including
some basic dependencies and files, with only a few commands. Learn more about Maven archetypes
[here](http://maven.apache.org/guides/introduction/introduction-to-archetypes.html).

## Installing and testing your archetype locally ##

Check out the KijiExpress project, then compile and install the archetype:

    cd kiji-express-archetype
    mvn clean compile install

This adds the Express archetype to your local archetype catalog.

To create a basic project, create a new directory in which to gernate the archetype. `cd` into
that directory and run

    mvn archetype:generate -DarchetypeCatalog=local

You'll see output listing all local archetypes - enter the number corresponding to `local -> org.kiji-express:kiji-express-archetype (kiji-express-archetype)`.
Follow the prompts to enter a groupId, artifactId, version number, and package.

Your basic project will now be ready to be edited, packaged, and used.

