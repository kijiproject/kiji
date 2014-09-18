Bento Cluster Plugin ${project.version}
=======================================

A Maven plugin to start and stop a Bento cluster running in a Docker container to be used in integration testing.

See the [bento cluster project](https://github.com/kijiproject/bento-cluster) for more on the Bento running in Docker.

In order to run this maven plugin, specify the following parameters when running `mvn install`.

`-Dbento.dir.path=/path/to/bento-cluster/` - Where on this machine is the bento-cluster?

`-Dskip=false` - Whether to skip setting up this plugin. Optional, defaults to false.

`-Dsite.files.dir.path=/path/to/site-files/` - Directory where to output generate site files to connect to HDFS, HBase, Yarn, etc. Optional, defaults to target/test-classes/

`-Dpersist=false` - Should the start Bento cluster container persist after the integration test? Optional, defaults to false.

