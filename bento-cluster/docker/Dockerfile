FROM centos:centos6
MAINTAINER Kiji Project <user@kiji.org>

RUN yum clean all

# Install Java 7 and Yum utils (needed for the `yum-config-manager` tool)
RUN yum install -y java-1.7.0-openjdk.x86_64 yum-utils python-setuptools

# Add the Cloudera and Datastax repositories
RUN yum-config-manager --add-repo http://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/5.0.3/
RUN yum-config-manager --add-repo http://rpm.datastax.com/community

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
  hbase-regionserver \
  jna \
  dsc20 \
  opscenter \
  datastax-agent

# Install Hadoop configuration
ADD hadoop-conf /etc/hadoop/conf.bento
RUN alternatives --verbose --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.bento 50
RUN alternatives --set hadoop-conf /etc/hadoop/conf.bento

# Install ZooKeeper configuration
ADD zookeeper-conf /etc/zookeeper/conf.bento
RUN alternatives --verbose --install /etc/zookeeper/conf zookeeper-conf /etc/zookeeper/conf.bento 50
RUN alternatives --set zookeeper-conf /etc/zookeeper/conf.bento

# Install HBase configuration
ADD hbase-conf /etc/hbase/conf.bento
RUN alternatives --verbose --install /etc/hbase/conf hbase-conf /etc/hbase/conf.bento 50
RUN alternatives --set hbase-conf /etc/hbase/conf.bento

# Install Cassandra configuration
ADD cassandra-conf /etc/cassandra/conf.bento
RUN alternatives --verbose --install /etc/cassandra/conf cassandra-conf /etc/cassandra/conf.bento 50
RUN alternatives --set cassandra-conf /etc/cassandra/conf.bento

# Install supervisord and configuration
RUN /usr/bin/easy_install supervisor
ADD supervisord.conf /etc/supervisor/supervisord.conf

# Format namenode
USER hdfs
RUN /usr/bin/hdfs namenode -format

# Initialize ZooKeeper
USER zookeeper
RUN /usr/bin/zookeeper-server-initialize

# Initialize HBase
ADD hbase-init /hbase-init

# Initialize mapreduce
ADD mapreduce-init /mapreduce-init

USER root

ADD start /start

CMD ["/start"]
