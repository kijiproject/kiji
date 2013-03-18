KijiChopsticks
==============

KijiChopsticks provides a simple data analysis language using
[KijiSchema](https://github.com/kijiproject/kiji-schema/) and
[Scalding](https://github.com/twitter/scalding/).

Compilation
-----------

KijiChopsticks requires [Apache Maven 3](http://maven.apache.org/download.html)
to build. It may built by running the command

    mvn clean package

from the root of the KijiChopsticks repository. This will create a release in
the target directory.

Running the NewsgroupWordCounts example
---------------------------------------

The following instructions assume that a functional
[KijiBento](https://github.com/kijiproject/kiji-bento/) minicluster has been
setup and is running. This example uses the
[20Newsgroups](http://qwone.com/~jason/20Newsgroups/) dataset.

First, create and populate the 'words' table:

    kiji-schema-shell --file=lib/words.ddl
    chop jar target/kiji-chopsticks-0.1.0-SNAPSHOT.jar org.kiji.chopsticks.NewsgroupLoader \
        kiji://.env/default/words <path/to/newsgroups/root/>

Run the word count, outputting to hdfs:

    chop hdfs target/kiji-chopsticks-0.1.0-SNAPSHOT.jar \
        org.kiji.chopsticks.NewsgroupWordCount \
        --input kiji://.env/default/words --output ./wordcounts.tsv

Check the results of the job:

    hadoop fs -cat ./wordcounts.tsv/part-00000 | grep "\<foo\>"

You should see something similar to:

    "'foo'\''bar'". 1
    "foo"); 1
    "foo'bar",  1
    "foo.txt  1
    "foo.txt" 1
    "foo:0",  1
    <foo> 1
    <foo@cs.rice.edu> 1
    >foo  1
    `foo' 1
    bar!foo!frotz 1
    foo 2
    foo%bar.bitnet@mitvma.mit.edu 1
    foo-boo 1
    foo/file  1
    foo:  1
    foo@mhfoo.pc.my 1

