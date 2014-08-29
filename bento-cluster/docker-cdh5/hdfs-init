#!/bin/bash

set -o nounset   # Fail when referencing undefined variables
set -o errexit   # Script exits on the first error
set -o pipefail  # Pipeline status failure if any command fails

# Should be run as the 'hdfs' user

/usr/bin/hadoop fs -mkdir -p /user/history
/usr/bin/hadoop fs -chmod -R 1777 /user/history
/usr/bin/hadoop fs -chown mapred:hadoop /user/history

/usr/bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
/usr/bin/hadoop fs -chown -R mapred:mapred /tmp/hadoop-yarn/staging
/usr/bin/hadoop fs -chmod -R 1777 /tmp

/usr/bin/hadoop fs -mkdir -p /var/log/hadoop-yarn
/usr/bin/hadoop fs -chown yarn:mapred /var/log/hadoop-yarn
/usr/bin/hadoop fs -chmod -R 1777 /var/log/hadoop-yarn


# Initialize HBase directory

/usr/bin/hadoop fs -mkdir -p /hbase
/usr/bin/hadoop fs -chown hbase /hbase
