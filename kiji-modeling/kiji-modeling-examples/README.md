Examples for KijiModeling
=========================

The KijiModeling library provides certain precanned algorithms for building predictive models.
The examples module for KijiModeling demonstrates how to train these models and provides datasets
for various use cases.

Each algorithm provides a README file which describes specific setup instructions.

The general setup is described below.

Setup
-----

*   Set up a functioning [KijiBento](https://github.com/kijiproject/kiji-bento/) environment. For
    installation instructions see: [http://www.kiji.org/](http://www.kiji.org/#trykijinow).

*   Start a bento cluster:

        bento start

*   If you haven't installed the default Kiji instance yet, do so first:

        kiji install

Building from source
--------------------

These examples are set up to be built using [Apache Maven](http://maven.apache.org/). To build a jar
containing the following examples

    git clone git@github.com:kijiprojct/kiji-modeling.git
    cd kiji-modeling/kiji-modeling-examples
    mvn package
