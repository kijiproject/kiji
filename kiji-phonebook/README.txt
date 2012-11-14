Kiji Phonebook Example
version ${project.version}


  (c) Copyright 2012 WibiData, Inc.

  See the NOTICE file distributed with this work for additional
  information regarding copyright ownership.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.


OVERVIEW:

  This project will define and create a Kiji table that holds a "phonebook"
of user data. It includes sample data and a bulk loader that will put the
phonebook data into the table.

  This project is designed to demonstrate a simple project you can use as
a reference when creating your Kiji tables.


COMPILING:

  This project is already compiled; you can skip to the "running" section
below if you just want to get started playing with the data set. (The jar is
in the lib/ directory.)

  However, the source code for this example is included along with a Maven
project, to provide you with example source and a basis for experimentation.

The following tools are required to compile this project:

  * Maven 3.0
  * Java 6

To compile, run 'mvn package'. The output will be placed in the
target/kiji-phonebook-${project.version}-release/ directory.


RUNNING:

  To run this example, you will need to configure Kiji ${project.version},
HBase, and Hadoop from CDH4. The commands in this section refer to the
'kiji' executable; you will need $KIJI_HOME/bin in your path for this to
work as-is.

  If you haven't already done so, run 'kiji install' to create the Kiji
system tables.

  To create the phonebook table:
  $ kiji create-table --table=phonebook --layout=layout.json


LOOKING UP A SINGLE ENTRY:

  This system includes an example application that retrieves a single user's
record from the database based on their first and last names. You can run this
with:

  $ kiji jar lib/kiji-phonebook-${project.version}.jar \
      org.kiji.examples.phonebook.Lookup --first=<firstname> --last=<lastname>

  You can try this with first and last names in the first two columns of
input-data.txt.
  The syntax shown here is the preferred mechanism to run your own "main()"
method with Kiji and its dependencies properly on the classpath.


ADDING AN ENTRY:

  You can add individual users to the phonebook with an interactive
command-line tool. You can run this with:

  $ kiji jar lib/kiji-phonebook-${project.version}.jar \
      org.kiji.examples.phonebook.AddEntry


IMPORTING RECORDS

  You can add users listed in a file using a map reduce job.  In this example
you'll find an input-data.txt file filled with sample phonebook, this should
be placed somewhere in your hdfs.
  You can run this with:

  $ kiji jar kiji-phonebook-${project.version}.jar \
      org.kiji.examples.phonebook.PhonebookImporter <input-data directory>


DERIVING COLUMNS

  You can run a map reduce job to fulfill new derived fields from already
existing data in the same row.  The AddressFieldExtractor example will
take the Address avro record field that was fulfilled by the previous
IMPORTING RECORDS step and use the data in the Address field to fill
simple data types in the derived column family.
  You can run this with:

  $ kiji jar kiji-phonebook-${project.version}.jar \
      org.kiji.examples.phonebook.AddressFieldExtractor


DELETING ENTRIES USING MAPREDUCE:

  You can delete many entries from within a MapReduce job. For example, the
following command will delete all entries that have an address from California:

  $ kiji jar lib/kiji-phonebook-${project.version}.jar \
      org.kiji.examples.phonebook.DeleteEntriesByState --state=CA
