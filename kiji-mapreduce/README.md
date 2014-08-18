KijiMR
======

KijiMR is a framework for writing MapReduce-based computation
over data stored in KijiSchema.

For more information about KijiSchema, see
[the Kiji project homepage](http://www.kiji.org).

Further documentation is available at the Kiji project
[Documentation Portal](http://docs.kiji.org)

Issues are being tracked at [the Kiji JIRA instance](https://jira.kiji.org/browse/KIJIMR).

CDH5.0.3 Notes
--------------

* TotalOrderPartitioner is now in a new package structure which might be incompatible.
  This should be bridgeable.
* The switch to mr2 jars means a lot of mapred.* conf keys will no longer be valid or populated.
  Be suspicious of any of these found in the code. Prefer their mr2 counterparts.
* The profiles should be removed from kiji-mapreduce/pom.xml and it should just be a 'flat'
  pom.xml.
