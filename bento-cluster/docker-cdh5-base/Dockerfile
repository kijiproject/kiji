FROM centos:centos6
MAINTAINER Kiji Project <dev@kiji.org>

RUN yum clean all

RUN yum install -y yum-utils python-setuptools

RUN yum-config-manager --add-repo http://archive.cloudera.com/cm5/redhat/6/x86_64/cm/cloudera-manager.repo
RUN yum-config-manager --add-repo http://rpm.datastax.com/community

RUN yum install -y --nogpgcheck \
  cassandra-2.0.9 \
  jna \
  dsc20 \
  opscenter \
  datastax-agent

RUN /usr/sbin/alternatives --install \
  /usr/bin/java java /usr/java/jdk1.7.0_67-cloudera/bin/java 999999999 \
  --slave /usr/bin/keytool     keytool     /usr/java/jdk1.7.0_67-cloudera/jre/bin/keytool \
  --slave /usr/bin/orbd        orbd        /usr/java/jdk1.7.0_67-cloudera/jre/bin/orbd \
  --slave /usr/bin/pack200     pack200     /usr/java/jdk1.7.0_67-cloudera/jre/bin/pack200 \
  --slave /usr/bin/rmid        rmid        /usr/java/jdk1.7.0_67-cloudera/jre/bin/rmid \
  --slave /usr/bin/rmiregistry rmiregistry /usr/java/jdk1.7.0_67-cloudera/jre/bin/rmiregistry \
  --slave /usr/bin/servertool  servertool  /usr/java/jdk1.7.0_67-cloudera/jre/bin/servertool \
  --slave /usr/bin/tnameserv   tnameserv   /usr/java/jdk1.7.0_67-cloudera/jre/bin/tnameserv \
  --slave /usr/bin/unpack200   unpack200   /usr/java/jdk1.7.0_67-cloudera/jre/bin/unpack200

RUN /usr/sbin/alternatives --install \
  /usr/bin/javac javac /usr/java/jdk1.7.0_67-cloudera/bin/javac 999999999 \
  --slave /usr/bin/appletviewer appletviewer /usr/java/jre/jdk1.7.0_67-cloudera/bin/appletviewer \
  --slave /usr/bin/apt          apt          /usr/java/jre/jdk1.7.0_67-cloudera/bin/apt \
  --slave /usr/bin/extcheck     extcheck     /usr/java/jre/jdk1.7.0_67-cloudera/bin/extcheck \
  --slave /usr/bin/idlj         idlj         /usr/java/jre/jdk1.7.0_67-cloudera/bin/idlj \
  --slave /usr/bin/jar          jar          /usr/java/jre/jdk1.7.0_67-cloudera/bin/jar \
  --slave /usr/bin/jarsigner    jarsigner    /usr/java/jre/jdk1.7.0_67-cloudera/bin/jarsigner \
  --slave /usr/bin/javadoc      javadoc      /usr/java/jre/jdk1.7.0_67-cloudera/bin/javadoc \
  --slave /usr/bin/javah        javah        /usr/java/jre/jdk1.7.0_67-cloudera/bin/javah \
  --slave /usr/bin/javap        javap        /usr/java/jre/jdk1.7.0_67-cloudera/bin/javap \
  --slave /usr/bin/jcmd         jcmd         /usr/java/jre/jdk1.7.0_67-cloudera/bin/jcmd \
  --slave /usr/bin/jconsole     jconsole     /usr/java/jre/jdk1.7.0_67-cloudera/bin/jconsole \
  --slave /usr/bin/jdb          jdb          /usr/java/jre/jdk1.7.0_67-cloudera/bin/jdb \
  --slave /usr/bin/jhat         jhat         /usr/java/jre/jdk1.7.0_67-cloudera/bin/jhat \
  --slave /usr/bin/jinfo        jinfo        /usr/java/jre/jdk1.7.0_67-cloudera/bin/jinfo \
  --slave /usr/bin/jmap         jmap         /usr/java/jre/jdk1.7.0_67-cloudera/bin/jmap \
  --slave /usr/bin/jmc          jmc          /usr/java/jre/jdk1.7.0_67-cloudera/bin/jmc \
  --slave /usr/bin/jps          jps          /usr/java/jre/jdk1.7.0_67-cloudera/bin/jps \
  --slave /usr/bin/jrunscript   jrunscript   /usr/java/jre/jdk1.7.0_67-cloudera/bin/jrunscript \
  --slave /usr/bin/jsadebugd    jsadebugd    /usr/java/jre/jdk1.7.0_67-cloudera/bin/jsadebugd \
  --slave /usr/bin/jstack       jstack       /usr/java/jre/jdk1.7.0_67-cloudera/bin/jstack \
  --slave /usr/bin/jstat        jstat        /usr/java/jre/jdk1.7.0_67-cloudera/bin/jstat \
  --slave /usr/bin/jstatd       jstatd       /usr/java/jre/jdk1.7.0_67-cloudera/bin/jstatd \
  --slave /usr/bin/keytool      keytool      /usr/java/jre/jdk1.7.0_67-cloudera/bin/keytool \
  --slave /usr/bin/native2ascii native2ascii /usr/java/jre/jdk1.7.0_67-cloudera/bin/native2ascii \
  --slave /usr/bin/policytool   policytool   /usr/java/jre/jdk1.7.0_67-cloudera/bin/policytool \
  --slave /usr/bin/rmic         rmic         /usr/java/jre/jdk1.7.0_67-cloudera/bin/rmic \
  --slave /usr/bin/schemagen    schemagen    /usr/java/jre/jdk1.7.0_67-cloudera/bin/schemagen \
  --slave /usr/bin/serialver    serialver    /usr/java/jre/jdk1.7.0_67-cloudera/bin/serialver \
  --slave /usr/bin/wsgen        wsgen        /usr/java/jre/jdk1.7.0_67-cloudera/bin/wsgen \
  --slave /usr/bin/wsimport     wsimport     /usr/java/jre/jdk1.7.0_67-cloudera/bin/wsimport \
  --slave /usr/bin/xjc          xjc          /usr/java/jre/jdk1.7.0_67-cloudera/bin/xjc

# Add CDH configuration
ADD hadoop-conf /etc/hadoop/conf.bento
ADD zookeeper-conf /etc/zookeeper/conf.bento
ADD hbase-conf /etc/hbase/conf.bento

# Install Cassandra configuration
ADD cassandra-conf /etc/cassandra/conf.bento
RUN alternatives --verbose --install /etc/cassandra/conf cassandra-conf /etc/cassandra/conf.bento 50
RUN alternatives --set cassandra-conf /etc/cassandra/conf.bento

# Install supervisord and configuration
RUN /usr/bin/easy_install supervisor
ADD supervisord.conf /etc/supervisor/supervisord.conf

USER root

ADD hdfs-init /hdfs-init

ADD start /start
