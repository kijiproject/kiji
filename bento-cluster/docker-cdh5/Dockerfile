FROM kijiproject/bento-cluster-base:cdh5
MAINTAINER Kiji Project <dev@kiji.org>

RUN yum-config-manager --add-repo http://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/5/

RUN yum install -y --nogpgcheck \
  zookeeper-server \
  hadoop-hdfs-namenode \
  hadoop-hdfs-datanode \
  hadoop-yarn-resourcemanager \
  hadoop-yarn-nodemanager \
  hadoop-yarn-proxyserver \
  hadoop-mapreduce \
  hadoop-mapreduce-historyserver \
  hbase-master \
  hbase-regionserver

# Install Hadoop configuration
RUN alternatives --verbose --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.bento 50
RUN alternatives --set hadoop-conf /etc/hadoop/conf.bento

# Install ZooKeeper configuration
RUN alternatives --verbose --install /etc/zookeeper/conf zookeeper-conf /etc/zookeeper/conf.bento 50
RUN alternatives --set zookeeper-conf /etc/zookeeper/conf.bento

# Install HBase configuration
RUN alternatives --verbose --install /etc/hbase/conf hbase-conf /etc/hbase/conf.bento 50
RUN alternatives --set hbase-conf /etc/hbase/conf.bento

# Format namenode
USER hdfs
RUN /usr/bin/hdfs namenode -format
RUN mkdir -p /var/log/hadoop-hdfs

# Initialize ZooKeeper
USER zookeeper
RUN /usr/bin/zookeeper-server-initialize
RUN mkdir -p /var/log/zookeeper

# Initialize HBase
USER hbase
RUN mkdir -p /var/log/hbase

# Initialize YARN
USER yarn
RUN mkdir -p /var/log/hadoop-yarn

# Initialize MapReduce
USER mapred
RUN mkdir -p /var/log/hadoop-mapreduce

USER root
CMD ["/start"]
