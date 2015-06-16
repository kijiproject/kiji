# -*- mode: python; coding: utf-8 -*-
#
# KijiBuild v2 build descriptor
#

# Default version ID to use when emitting Maven artifacts:
# All artifacts are currently pegged on a single version ID.
maven_artifact_version = "3.0.0-SNAPSHOT"

# --------------------------------------------------------------------------------------------------
# Python base libraries

# PEP-302 (pkg_resources) allows for namespace packages:
python_library(
    name="//python:pep-302",
    sources=[
        source("//python/pep-302/src/main/python", "pkg_resources.py"),
    ],
)

python_library(
    name="//python:base",
    sources=[
        source("//python-base/src/main/python", "**/*.py"),
        source("//python-base/src/main/python", "base/VERSION"),
    ],
    deps=["//python:pep-302"],
)

python_test(
    name="//python:base-test",
    sources=[
        source("//python-base/src/test/python", "**/*.py"),
        source("//python-base/src/test/python", "base/package/resource.txt"),
    ],
    deps=["//python:base"],
)


python_library(
    name="//python:workflow",
    sources=[source("//python-workflow/src/main/python", "**/*.py")],
    deps=["//python:base"],
)

python_test(
    name="//python:workflow-test",
    sources=[source("//python-workflow/src/test/python", "**/*.py")],
    deps=["//python:workflow"],
)

python_library(
    name="//python:avro",
    sources=[
        source("//avro/src/main/python", "**/*.py"),

        # Resources to include for IPC support:
        source("//avro/src/main/python", "avro/HandshakeRequest.avsc"),
        source("//avro/src/main/python", "avro/HandshakeResponse.avsc"),

        # Obsolete but necessary for now:
        source("//avro/src/main/python", "avro/VERSION.txt"),
    ],
)

python_test(
    name="//python:avro-test",
    sources=[
        source("//avro/src/test/python", "**/*.py"),
        source("//avro/src/test/python", "avro/interop.avsc"),
    ],
    deps=["//python:avro"],
)

# Avro CLI to inspect Avro data files:
python_binary(
    name="//python:avro-tool",
    sources=[source("//avro/src/main/scripts", "avro/avro_tool.py")],
    main_module="avro.avro_tool",
    deps=["//python:avro"],
)

# --------------------------------------------------------------------------------------------------
# DevTools

python_library(
    name="//devtools:devtools-lib",
    sources=[
        source("//devtools/src/main/python", "**/*.py"),
        source("//devtools/src/main/python", "wibi/devtools/VERSION"),
    ],
    deps=[
        "//python:base",
        "//python:workflow",
    ],
)

python_binary(
    name="//devtools:dump-record",
    main_module="wibi.scripts.dump_record",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:java-linker",
    main_module="wibi.scripts.java_linker",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:loader",
    main_module="wibi.scripts.loader",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:kiji-build",
    main_module="wibi.scripts.kiji_build",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:kiji-review",
    main_module="wibi.scripts.kiji_review",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:reviewboard",
    main_module="wibi.scripts.reviewboard",
    deps=["//devtools:devtools-lib"],
)

python_test(
    name="//devtools:devtools-test",
    sources=[source("//devtools/src/test/python", "**/*.py")],
    deps=["//devtools:devtools-lib"],
)

java_binary(
    name="//devtools:checkstyle",
    main_class="com.puppycrawl.tools.checkstyle.Main",
    deps=[
        maven("com.puppycrawl.tools:checkstyle:6.1.1"),
    ],
)

java_binary(
    name="//devtools:findbugs",
    main_class="edu.umd.cs.findbugs.FindBugs2",
    deps=[
        maven("com.google.code.findbugs:findbugs:3.0.0"),
    ],
)

python_binary(
    name="//devtools:flake8",
    main_module="flake8.entrypoint",
    sources=[
        source("//devtools/src/main/scripts", "flake8/entrypoint.py"),
    ],
    deps=[
        pypi("flake8", "2.3.0"),
    ],
)

python_binary(
    name="//devtools:delinearize",
    main_module="wibi.scripts.delinearize",
    deps=["//devtools:devtools-lib"],
)

# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------
# Checkstyle globals:

checkstyle_rules_kiji = "//.workspace_config/checkstyle/kiji_checkstyle.xml"
checkstyle_rules_wibi = "//.workspace_config/checkstyle/wibi_checkstyle.xml"
checkstyle_header_kiji = "//.workspace_config/checkstyle/kiji_header.txt"
checkstyle_header_wibi = "//.workspace_config/checkstyle/wibi_header.txt"
checkstyle_empty_suppressions = "//.workspace_config/checkstyle/empty_suppressions.xml"
checkstyle_test_suppressions = "//.workspace_config/checkstyle/test_suppressions.xml"

checkstyle_kiji = checkstyle(
    config=checkstyle_rules_kiji,
    suppressions=checkstyle_empty_suppressions,
    header=checkstyle_header_kiji,
)
checkstyle_kiji_test = checkstyle(
    config=checkstyle_rules_kiji,
    suppressions=checkstyle_test_suppressions,
    header=checkstyle_header_kiji,
)
checkstyle_wibi = checkstyle(
    config=checkstyle_rules_wibi,
    suppressions=checkstyle_empty_suppressions,
    header=checkstyle_header_wibi,
)
checkstyle_wibi_test = checkstyle(
    config=checkstyle_rules_wibi,
    suppressions=checkstyle_test_suppressions,
    header=checkstyle_header_wibi,
)

# --------------------------------------------------------------------------------------------------
# Scalastyle globals:

scalastyle_kiji = "//.workspace_config/scalastyle/kiji_config.xml"
scalastyle_wibi = "//.workspace_config/scalastyle/wibi_config.xml"

# --------------------------------------------------------------------------------------------------
# pom.xml generation globals:

# This should be used in generated_pom build definitions as a pom_template for projects that have
# scala sources.
scala_pom_template="//.workspace_config/maven/scala-pom.xml"

# --------------------------------------------------------------------------------------------------
# Maven dependencies:

avro = maven(group="org.apache.avro", id="avro", version=avro_version)
avro_ipc = maven(group="org.apache.avro", id="avro-ipc", version=avro_version)
avro_mapred = maven(
    group="org.apache.avro", id="avro-mapred", classifier="hadoop2", version=avro_version,
)

cascading_core = "cascading:cascading-core:2.6.1"
cascading_hadoop = "cascading:cascading-hadoop:2.6.1"
cascading_local = "cascading:cascading-local:2.6.1"
cascading_kryo = "cascading.kryo:cascading.kryo:0.4.7"
commons_codec = "commons-codec:commons-codec:1.6"
commons_compress = "org.apache.commons:commons-compress:1.4.1"
commons_io = "commons-io:commons-io:2.1"
commons_lang = "commons-lang:commons-lang:2.6"
commons_lang3 = "org.apache.commons:commons-lang3:3.0"
commons_pool = "commons-pool:commons-pool:1.6"
dropwizard_core="io.dropwizard:dropwizard-core:0.7.1"
dropwizard_testing = "io.dropwizard:dropwizard-testing:0.7.1"
gson = "com.google.code.gson:gson:2.2.2"
guava = "com.google.guava:guava:15.0"
httpclient = "org.apache.httpcomponents:httpclient:4.2.3"
fasterxml_jackson_module_jaxb_annotations = "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.3.3"
fasterxml_jackson_core = "com.fasterxml.jackson.core:jackson-core:2.3.3"
fasterxml_jackson_databind = "com.fasterxml.jackson.core:jackson-databind:2.3.3"
fasterxml_jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:2.3.3"
fasterxml_jackson_module_scala = "com.fasterxml.jackson.module:jackson-module-scala_2.10:2.3.3"
joda_convert = "org.joda:joda-convert:1.3.1"
joda_time = "joda-time:joda-time:2.3"
jsr305 = "com.google.code.findbugs:jsr305:1.3.9"
kryo = "com.esotericsoftware.kryo:kryo:2.21"
netty = "io.netty:netty:3.9.0.Final"
scala_compiler = "org.scala-lang:scala-compiler:2.10.4"
scala_jline = "org.scala-lang:jline:2.10.4"
scala_reflect = "org.scala-lang:scala-reflect:2.10.4"
scalatest = "org.scalatest:scalatest_2.10:2.0"
scalding_args = "com.twitter:scalding-args_2.10:0.9.1"
scalding_core = "com.twitter:scalding-core_2.10:0.9.1"
scallop = "org.rogach:scallop_2.10:0.9.5"
slf4j_api = "org.slf4j:slf4j-api:1.7.5"
slf4j_log4j12 = "org.slf4j:slf4j-log4j12:1.7.5"
spark_core = "org.apache.spark:spark-core_2.10:1.2.0-cdh5.3.1"
spark_mllib = "org.apache.spark:spark-mllib_2.10:1.2.0-cdh5.3.1"
specs2 = "org.specs2:specs2_2.10:2.3.8"
solr_core = "org.apache.solr:solr-core:4.10.3"
solr_solrj = "org.apache.solr:solr-solrj:4.10.3"

jackson_core_asl = "org.codehaus.jackson:jackson-core-asl:1.9.13"
jackson_jaxrs = "org.codehaus.jackson:jackson-jaxrs:1.9.13"
jackson_mapper_asl = "org.codehaus.jackson:jackson-mapper-asl:1.9.13"
jackson_xc = "org.codehaus.jackson:jackson-xc:1.9.13"

json_simple = "com.googlecode.json-simple:json-simple:1.1"

riemann_java_client = "com.aphyr:riemann-java-client:0.2.10"
dropwizard_metrics_core = "io.dropwizard.metrics:metrics-core:3.1.0"
dropwizard_metrics_jvm = "io.dropwizard.metrics:metrics-jvm:3.1.0"

latency_utils = "org.latencyutils:LatencyUtils:2.0.2"

# Maven dependencies for tests:
easymock = "org.easymock:easymock:3.0"
hamcrest = "org.hamcrest:hamcrest-all:1.1"
junit = "junit:junit:4.10"

# --------------------------------------------------------------------------------------------------

java_library(
    name="//org/kiji/deps:jackson",
    deps=[
        maven(jackson_core_asl),
        maven(jackson_jaxrs),
        maven(jackson_mapper_asl),
        maven(jackson_xc),
    ],
)

java_library(
    name="//org/kiji/deps:riemann-java-client",
    deps=[
        maven(riemann_java_client),
    ],
    maven_exclusions=[
        "io.netty:netty:*:*:*",
        "com.yammer.metrics:metrics-core:*:*:*",
        "com.codahale.metrics:metrics-core:*:*:*",
    ],
)

# --------------------------------------------------------------------------------------------------
# Internal dependencies

java_library(
    name="//testing:test-annotation-collector",
    sources=["//testing/test-collector/src/main/java"],
    deps=[
        maven(guava),
        maven(junit),
    ],
    checkstyle=checkstyle_wibi,
)

generated_pom(
    name="//testing:test-annotation-collector-pom",
    pom_name="//testing:test-annotation-collector",
    pom_file="//testing/test-collector/pom.xml",
    main_deps=["//testing:test-annotation-collector"],
)

java_library(
    name="//testing:test-runner",
    sources=["//testing/test-runner/src/main/java"],
    deps=[
        maven(guava),
        maven(junit),
        maven(slf4j_log4j12),
        "//org/kiji/common:kiji-common-flags",
    ],
    checkstyle=checkstyle_wibi,
)

# --------------------------------------------------------------------------------------------------
# Fake HBase

scala_library(
    name="//org/kiji/testing:fake-hbase",
    sources=[
        "//fake-hbase/src/main/java",
        "//fake-hbase/src/main/scala",
    ],
    deps=[
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.0-platform"),
        maven(easymock),
    ],
)

scala_test(
    name="//org/kiji/testing:fake-hbase-test",
    sources=["//fake-hbase/src/test/scala"],
    deps=["//org/kiji/testing:fake-hbase"],
)

generated_pom(
    name="//org/kiji/testing:fake-hbase-pom",
    pom_name="//org/kiji/testing:fake-hbase",
    pom_file="//fake-hbase/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/testing:fake-hbase"],
    test_deps=["//org/kiji/testing:fake-hbase-test"],
)

# --------------------------------------------------------------------------------------------------
# Platforms

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.1-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.hbase:hbase:0.92.1-cdh4.1.4"),
#         maven("org.apache.zookeeper:zookeeper:3.4.3-cdh4.1.4"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.2-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.hbase:hbase:0.94.2-cdh4.2.2"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.2.2"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.3-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.hbase:hbase:0.94.6-cdh4.3.2"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.3.2"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.4-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.hbase:hbase:0.94.6-cdh4.4.0"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.4.0"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

java_library(
    name="//org/kiji/platforms:cdh5.0-platform",
    deps=[
        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.3.0-cdh5.0.3"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.3.0-cdh5.0.3"),
        maven("org.apache.hadoop:hadoop-common:2.3.0-cdh5.0.3"),
        maven("org.apache.hbase:hbase-client:0.96.1.1-cdh5.0.3"),
        maven("org.apache.hbase:hbase-server:0.96.1.1-cdh5.0.3"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.0.3"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
    ],
    provides=["kiji_platform"],
)

java_library(
    name="//org/kiji/platforms:cdh5.1-platform",
    deps=[
        "//org/kiji/deps:jackson",

        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-common:2.3.0-cdh5.1.3"),
        maven("org.apache.hbase:hbase-client:0.98.1-cdh5.1.3"),
        maven("org.apache.hbase:hbase-server:0.98.1-cdh5.1.3"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.1.3"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
    ],
    provides=["kiji_platform"],
)

java_library(
    name="//org/kiji/platforms:cdh5.2-platform",
    deps=[
        "//org/kiji/deps:jackson",
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.5.0-cdh5.2.1"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.5.0-cdh5.2.1"),
        maven("org.apache.hadoop:hadoop-common:2.5.0-cdh5.2.1"),
        maven("org.apache.hbase:hbase-client:0.98.6-cdh5.2.1"),
        maven("org.apache.hbase:hbase-server:0.98.6-cdh5.2.1"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.2.1"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
        "org.apache.hbase:hbase-common:*:tests:*",
    ],
    provides=["kiji_platform"],
)

java_library(
    name="//org/kiji/platforms:cdh5.3-platform",
    deps=[
        "//org/kiji/deps:jackson",
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),
        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.5.0-cdh5.3.1"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.5.0-cdh5.3.1"),
        maven("org.apache.hadoop:hadoop-common:2.5.0-cdh5.3.1"),
        maven("org.apache.hbase:hbase-client:0.98.6-cdh5.3.1"),
        maven("org.apache.hbase:hbase-server:0.98.6-cdh5.3.1"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.3.1"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
        "org.apache.hbase:hbase-common:*:tests:*",
    ],
    provides=["kiji_platform"],
)

java_library(
    name="//org/kiji/platforms:cassandra-platform",
    deps=[
        maven(guava),

        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-common:2.3.0-cdh5.1.3"),
        maven("org.apache.hbase:hbase-client:0.98.1-cdh5.1.3"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.1.3"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
        maven("org.apache.hadoop:hadoop-hdfs:2.3.0-cdh5.1.3"),

        maven("com.datastax.cassandra:cassandra-driver-core:2.1.4"),
        maven(dropwizard_metrics_core),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",

        # Cassandra driver 2.1.4 uses an old version of Coda Hale metrics under a different package
        # name, so exclude it and instead include the newer correct version.
        "com.codahale.metrics:metrics-core:*:*:3.0.2",

        # Exclude junit from non-test platforms:
        "junit:junit:*:*:*",

        # This conflicts with newer asm maven artifact org.ow2:asm.
        "asm:asm:*:*:*",
    ],
    provides=["kiji_platform"],
)

java_library(
    name="//org/kiji/platforms:compile-platform",
    deps=["//org/kiji/platforms:cdh5.1-platform"],
)


# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.1-test-platform",
#     deps=["//org/kiji/platforms:cdh4.1-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.2-test-platform",
#     deps=["//org/kiji/platforms:cdh4.2-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.3-test-platform",
#     deps=["//org/kiji/platforms:cdh4.3-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/platforms:cdh4.4-test-platform",
#     deps=["//org/kiji/platforms:cdh4.4-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["kiji_platform"],
# )

java_library(
    name="//org/kiji/platforms:cdh5.1-test-platform",
    deps=[
        # "//org/kiji/deps:jackson",  # still needed?
        "//org/kiji/platforms:cdh5.1-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.3.0-cdh5.1.3"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.1-cdh5.1.3"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.1-cdh5.1.3"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//org/kiji/platforms:cdh5.2-test-platform",
    deps=[
        # "//org/kiji/deps:jackson",  # still needed?
        "//org/kiji/platforms:cdh5.2-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.5.0-cdh5.2.1"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.6-cdh5.2.1"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.6-cdh5.2.1"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//org/kiji/platforms:cdh5.3-test-platform",
    deps=[
        # "//org/kiji/deps:jackson",  # still needed?
        "//org/kiji/platforms:cdh5.3-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.5.0-cdh5.3.1"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.6-cdh5.3.1"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.6-cdh5.3.1"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//org/kiji/platforms:cassandra-test-platform",
    deps=[
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.3.0-cdh5.1.3"),
        maven("org.apache.curator:curator-test:2.4.1"),

        maven("com.datastax.cassandra:cassandra-driver-core:2.1.4"),
        maven("org.apache.cassandra:cassandra-all:2.0.9"),

        "//org/kiji/platforms:cdh5.1-platform",
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//org/kiji/platforms:test-platform",
    deps=["//org/kiji/platforms:cdh5.1-test-platform"],
)

# --------------------------------------------------------------------------------------------------

python_binary(
    name="//bento-cluster:bento",
    main_module="bento.cli",
    sources=[
        source("//bento-cluster/src/main/python", "**/*.py"),
        source("//bento-cluster/src/main/python", "**/*.xml"),
        source("//bento-cluster/src/main/python", "**/*.sh"),
        source("//bento-cluster/src/main/python", "bento/bento-sudoers-template"),
    ],
    deps=[
        "//python:base",
        pypi("docker-py", "0.7.2"),
    ],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//org/kiji/hadoop:hadoop-configurator",
    sources=["//hadoop-configurator/src/main/java"],
    deps=[
        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/hadoop:hadoop-configurator-test",
    sources=["//hadoop-configurator/src/test/java"],
    deps=[
        "//org/kiji/hadoop:hadoop-configurator",
        maven(junit),
        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/hadoop:hadoop-configurator-pom",
    pom_name="//org/kiji/hadoop:hadoop-configurator",
    pom_file="//hadoop-configurator/pom.xml",
    main_deps=["//org/kiji/hadoop:hadoop-configurator"],
    test_deps=["//org/kiji/hadoop:hadoop-configurator-test"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//org/kiji/annotations:annotations",
    sources=["//annotations/src/main/java"],
)

generated_pom(
    name="//org/kiji/annotations:annotations-pom",
    pom_name="//org/kiji/annotations:annotations",
    pom_file="//annotations/pom.xml",
    main_deps=["//org/kiji/annotations:annotations"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//org/kiji/delegation:kiji-delegation",
    sources=["//kiji-delegation/src/main/java"],
    deps=[
        maven(slf4j_api),
        "//org/kiji/annotations:annotations",
    ],
    checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/delegation:kiji-delegation-test",
    sources=["//kiji-delegation/src/test/java"],
    resources=["//kiji-delegation/src/test/resources/"],
    deps=[
        maven(junit),
        "//org/kiji/delegation:kiji-delegation",
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/delegation:kiji-delegation-pom",
    pom_name="//org/kiji/delegation:kiji-delegation",
    pom_file="//kiji-delegation/pom.xml",
    main_deps=["//org/kiji/delegation:kiji-delegation"],
    test_deps=["//org/kiji/delegation:kiji-delegation-test"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//org/kiji/common:kiji-common-flags",
    sources=["//kiji-common-flags/src/main/java"],
    resources=["//kiji-common-flags/src/main/resources/"],
    deps=[
        maven(guava),
        maven(slf4j_api),
        "//org/kiji/delegation:kiji-delegation",
    ],
    # TODO: Checkstyle was never run on this project. Needs corresponding cleanup before this is
    # re-enabled.
    # checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/common:kiji-common-flags-test",
    sources=["//kiji-common-flags/src/test/java",],
    resources=["//kiji-common-flags/src/test/resources",],
    deps=["//org/kiji/common:kiji-common-flags"],
    # TODO: Checkstyle was never run on this project. Needs corresponding cleanup before this is
    # re-enabled.
    # checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/common:kiji-common-flags-pom",
    pom_name="//org/kiji/common:kiji-common-flags",
    pom_file="//kiji-common-flags/pom.xml",
    main_deps=["//org/kiji/common:kiji-common-flags"],
    test_deps=["//org/kiji/common:kiji-common-flags-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiCommons

java_library(
    name="//org/kiji/commons:kiji-commons-java",
    sources=["//kiji-commons/kiji-commons-java/src/main/java"],
    deps=[
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),
        maven(fasterxml_jackson_module_jaxb_annotations),

        "//org/kiji/annotations:annotations",
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/commons:kiji-commons-monitoring",
    sources=["//kiji-commons/kiji-commons-monitoring/src/main/java"],
    deps=[
        maven(guava),
        maven(slf4j_api),
        maven(jsr305),

        maven(dropwizard_metrics_core),
        maven(dropwizard_metrics_jvm),
        maven(latency_utils),

        "//org/kiji/commons:kiji-commons-java",
        "//org/kiji/deps:riemann-java-client",
    ],
    checkstyle=checkstyle_kiji,
)

scala_library(
    name="//org/kiji/commons:kiji-commons-scala",
    sources=["//kiji-commons/kiji-commons-scala/src/main/scala"],
    deps=[
        maven(guava),
        maven(fasterxml_jackson_module_scala),
        maven(jsr305),
        maven(slf4j_api),

        "//org/kiji/annotations:annotations",
        "//org/kiji/commons:kiji-commons-java",
    ],
    scalastyle=scalastyle_kiji,
)

java_library(
    name="//org/kiji/checkin:kiji-checkin",
    sources=["//kiji-checkin/src/main/java"],
    deps=[
        maven(commons_io),
        maven(commons_lang),
        maven(gson),
        maven(guava),
        maven(httpclient),
        maven(slf4j_api),

        "//org/kiji/common:kiji-common-flags",
    ],
    checkstyle=checkstyle_kiji,
)

avro_java_library(
    name="//org/kiji/commons:kiji-commons-java-test-avro-lib",
    sources=["//kiji-commons/kiji-commons-java/src/test/avro/*.avdl"],
)

java_test(
    name="//org/kiji/commons:kiji-commons-java-test",
    jvm_args=["-Dorg.kiji.commons.ResourceTracker.tracking_level=REFERENCES"],
    sources=["//kiji-commons/kiji-commons-java/src/test/java"],
    deps=[
        "//org/kiji/commons:kiji-commons-java",
        "//org/kiji/commons:kiji-commons-java-test-avro-lib",
    ],
    checkstyle=checkstyle_kiji_test,
)

java_test(
    name="//org/kiji/commons:kiji-commons-monitoring-test",
    sources=["//kiji-commons/kiji-commons-monitoring/src/test/java"],
    deps=[
        "//org/kiji/commons:kiji-commons-monitoring",
    ],
    checkstyle=checkstyle_kiji_test,
)

scala_test(
    name="//org/kiji/commons:kiji-commons-scala-test",
    sources=["//kiji-commons/kiji-commons-scala/src/test/scala"],
    deps=[
        maven(junit),
        "//org/kiji/commons:kiji-commons-scala",
        "java_library(//org/kiji/commons:kiji-commons-java-test)",
    ],
)

# TODO: Re-enable this once we have support for setting jar MANIFEST.mf files. TestVersionInfo
#     relies on being able to read the "package title" from the MANIFEST.mf in the kiji-delegation
#     jar.
java_test(
    name="//org/kiji/checkin:kiji-checkin-test",
    sources=["//kiji-checkin/src/test/java"],
    deps=[
        maven(easymock),
        "//org/kiji/checkin:kiji-checkin",
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/commons:kiji-commons-java-pom",
    pom_name="//org/kiji/commons:kiji-commons-java",
    pom_file="//kiji-commons/kiji-commons-java/pom.xml",
    main_deps=["//org/kiji/commons:kiji-commons-java"],
    test_deps=["//org/kiji/commons:kiji-commons-java-test"],
)

generated_pom(
    name="//org/kiji/commons:kiji-commons-monitoring-pom",
    pom_name="//org/kiji/commons:kiji-commons-monitoring",
    pom_file="//kiji-commons/kiji-commons-monitoring/pom.xml",
    main_deps=["//org/kiji/commons:kiji-commons-monitoring"],
    test_deps=["//org/kiji/commons:kiji-commons-monitoring-test"],
)

generated_pom(
    name="//org/kiji/commons:kiji-commons-scala-pom",
    pom_name="//org/kiji/commons:kiji-commons-scala",
    pom_file="//kiji-commons/kiji-commons-scala/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/commons:kiji-commons-scala"],
    test_deps=["//org/kiji/commons:kiji-commons-scala-test"],
)

generated_pom(
    name="//org/kiji/checkin:kiji-checkin-pom",
    pom_name="//org/kiji/checkin:kiji-checkin",
    pom_file="//kiji-checkin/pom.xml",
    main_deps=["//org/kiji/checkin:kiji-checkin"],
    test_deps=["//org/kiji/checkin:kiji-checkin-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiSolr

java_library(
    name="//org/kiji/solr:kiji-solr-lib",
    sources=["//kiji-solr/src/main/java"],
    deps=[
        maven(slf4j_api),
        maven(solr_core),
        maven(solr_solrj),
        "//org/kiji/schema:kiji-schema",
    ],
    checkstyle=checkstyle_kiji,
)

java_binary(
    name="//org/kiji/solr:kiji-solr",
    main_class="org.kiji.solr.HelloWorld",
    deps=[
        "//org/kiji/solr:kiji-solr-lib",
    ],
    maven_exclusions=[],
)

java_test(
    name="//org/kiji/solr:kiji-solr-test",
    sources=["//kiji-solr/src/test/java"],
    deps=[
        maven(junit),
        "//org/kiji/solr:kiji-solr-lib",
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/solr:kiji-solr-pom",
    pom_name="//org/kiji/solr:kiji-solr",
    pom_file="//kiji-solr/pom.xml",
    main_deps=["//org/kiji/solr:kiji-solr-lib"],
    test_deps=["//org/kiji/solr:kiji-solr-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiSchema

avro_java_library(
    name="//org/kiji/schema:kiji-schema-avro",
    sources=[
        "//kiji-schema/kiji-schema/src/main/avro/Layout.avdl",
        "//kiji-schema/kiji-schema/src/main/avro/Security.avdl",
    ],
)

java_library(
    name="//org/kiji/schema:schema-platform-api",
    sources=["//kiji-schema/platform-api/src/main/java"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/delegation:kiji-delegation",

        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_kiji,
)

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/schema:cdh41mr1-bridge",
#     sources=["//kiji-schema/cdh41mr1-bridge/src/main/java"],
#     resources=["//kiji-schema/cdh41mr1-bridge/src/main/resources/"],
#     deps=[
#         "//org/kiji/annotations:annotations",
#         "//org/kiji/schema:schema-platform-api",
#
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.1-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//org/kiji/schema:cdh42mr1-bridge",
#     sources=["//kiji-schema/cdh42mr1-bridge/src/main/java"],
#     resources=["//kiji-schema/cdh42mr1-bridge/src/main/resources/"],
#     deps=[
#         "//org/kiji/annotations:annotations",
#         "//org/kiji/schema:schema-platform-api",
#
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.2-platform"),
#     ],
# )

java_library(
    name="//org/kiji/schema:cdh5-bridge",
    sources=["//kiji-schema/cdh5-bridge/src/main/java"],
    resources=["//kiji-schema/cdh5-bridge/src/main/resources/"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/schema:schema-platform-api",

        dynamic(kiji_platform="//org/kiji/platforms:cdh5.1-platform"),
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/schema:kiji-schema",
    sources=["//kiji-schema/kiji-schema/src/main/java"],
    resources=["//kiji-schema/kiji-schema/src/main/resources/"],
    deps=[
        avro,
        maven(commons_pool),
        maven(gson),
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        "//org/kiji/schema:kiji-schema-avro",
        "//org/kiji/annotations:annotations",
        "//org/kiji/checkin:kiji-checkin",
        "//org/kiji/common:kiji-common-flags",
        "//org/kiji/commons:kiji-commons-java",
        "//org/kiji/delegation:kiji-delegation",

        "//org/kiji/schema:schema-platform-api",  # brings compile-platform
        "//org/kiji/schema:cdh5-bridge",          # brings cdh5.1-platform
        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_kiji,
        suppressions="//kiji-schema/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_kiji,
    ),
)

java_library(
    name="//org/kiji/schema:kiji-schema-cassandra",
    sources=["//kiji-schema/kiji-schema-cassandra/src/main/java"],
    resources=["//kiji-schema/kiji-schema-cassandra/src/main/resources/"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/delegation:kiji-delegation",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-avro",
        "//org/kiji/schema:schema-platform-api",  # brings compile-platform
        avro,
        avro_ipc,
        avro_mapred,
        dynamic(kiji_platform="//org/kiji/platforms:cassandra-platform"),
        maven(guava),
        maven(jsr305),
        maven(netty),
        maven(slf4j_api),
    ],
    checkstyle=checkstyle_kiji,
)

java_binary(
    name="//org/kiji/schema:kiji",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
    ],
)

# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//org/kiji/schema:kiji-cdh4.1",
#     main_class="org.kiji.schema.tools.KijiToolLauncher",
#     deps=[
#         "//org/kiji/schema:kiji-schema",
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.1-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//org/kiji/schema:kiji-cdh4.2",
#     main_class="org.kiji.schema.tools.KijiToolLauncher",
#     deps=[
#         "//org/kiji/schema:kiji-schema",
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.2-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//org/kiji/schema:kiji-cdh4.3",
#     main_class="org.kiji.schema.tools.KijiToolLauncher",
#     deps=[
#         "//org/kiji/schema:kiji-schema",
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.3-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//org/kiji/schema:kiji-cdh4.4",
#     main_class="org.kiji.schema.tools.KijiToolLauncher",
#     deps=[
#         "//org/kiji/schema:kiji-schema",
#         dynamic(kiji_platform="//org/kiji/platforms:cdh4.4-platform"),
#     ],
# )

java_binary(
    name="//org/kiji/schema:kiji-cdh5.0",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.0-platform"),
    ],
)

java_binary(
    name="//org/kiji/schema:kiji-cdh5.1",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.1-platform"),
    ],
)

java_binary(
    name="//org/kiji/schema:kiji-cdh5.2",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.2-platform"),
    ],
)

java_binary(
    name="//org/kiji/schema:kiji-cdh5.3",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.3-platform"),
    ],
)

java_binary(
    name="//org/kiji/schema:kiji-dynamic",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        dynamic(kiji_platform=None),
        "//org/kiji/schema:kiji-schema",
    ],
)

avro_java_library(
    name="//org/kiji/schema:kiji-schema-test-avro",
    sources=[
        "//kiji-schema/kiji-schema/src/test/avro/*.avdl",
        "//kiji-schema/kiji-schema/src/test/avro/*.avsc",
    ],
)

java_test(
    name="//org/kiji/schema:kiji-schema-test",
    sources=["//kiji-schema/kiji-schema/src/test/java"],
    resources=["//kiji-schema/kiji-schema/src/test/resources/"],
    deps=[
        maven(easymock),
        maven(hamcrest),
        maven(junit),

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
        "//org/kiji/testing:fake-hbase",

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-test-avro",
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_kiji,
        suppressions="//kiji-schema/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_kiji,
    ),
)

java_test(
    name="//org/kiji/schema:kiji-schema-cassandra-test",
    sources=["//kiji-schema/kiji-schema-cassandra/src/test/java"],
    resources=["//kiji-schema/kiji-schema-cassandra/src/test/resources/"],
    deps=[
        maven(easymock),
        maven(hamcrest),
        maven(junit),

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-cassandra",

        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/schema:kiji-schema-test-avro",

        dynamic(kiji_platform="//org/kiji/platforms:cassandra-test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

scala_library(
    name="//org/kiji/schema:kiji-schema-extras",
    sources=[
        "//kiji-schema/kiji-schema-extras/src/main/java",
        "//kiji-schema/kiji-schema-extras/src/main/scala",
    ],
    deps=[
        "//org/kiji/schema:kiji-schema",
    ],
)

scala_test(
    name="//org/kiji/schema:kiji-schema-extras-test",
    sources=["//kiji-schema/kiji-schema-extras/src/test/scala"],
    deps=[
        "//org/kiji/testing:fake-hbase",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-extras",

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//org/kiji/schema:schema-platform-api-pom",
    pom_name="//org/kiji/schema:schema-platform-api",
    pom_file="//kiji-schema/platform-api/pom.xml",
    main_deps=["//org/kiji/schema:schema-platform-api"],
)

generated_pom(
    name="//org/kiji/schema:cdh5-bridge-pom",
    pom_name="//org/kiji/schema:cdh5-bridge",
    pom_file="//kiji-schema/cdh5-bridge/pom.xml",
    main_deps=["//org/kiji/schema:cdh5-bridge"],
)

generated_pom(
    name="//org/kiji/schema:kiji-schema-pom",
    pom_name="//org/kiji/schema:kiji-schema",
    pom_file="//kiji-schema/kiji-schema/pom.xml",
    main_deps=["//org/kiji/schema:kiji-schema"],
    test_deps=["//org/kiji/schema:kiji-schema-test"],
)

generated_pom(
    name="//org/kiji/schema:kiji-schema-cassandra-pom",
    pom_name="//org/kiji/schema:kiji-schema-cassandra",
    pom_file="//kiji-schema/kiji-schema-cassandra/pom.xml",
    main_deps=["//org/kiji/schema:kiji-schema-cassandra"],
    test_deps=["//org/kiji/schema:kiji-schema-cassandra-test"],
)

generated_pom(
    name="//org/kiji/schema:kiji-schema-extras-pom",
    pom_name="//org/kiji/schema:kiji-schema-extras",
    pom_file="//kiji-schema/kiji-schema-extras/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/schema:kiji-schema-extras"],
    test_deps=["//org/kiji/schema:kiji-schema-extras-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiPhonebook

avro_java_library(
    name="//org/kiji/examples/phonebook:kiji-phonebook-avro-lib",
    sources=["//kiji-phonebook/src/main/avro/*.avsc"],
)

java_library(
    name="//org/kiji/examples/phonebook:kiji-phonebook",
    sources=["//kiji-phonebook/src/main/java"],
    resources=["//kiji-phonebook/src/main/resources"],
    deps=[
        "//org/kiji/examples/phonebook:kiji-phonebook-avro-lib",

        "//org/kiji/common:kiji-common-flags",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-cassandra",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/mapreduce:kiji-mapreduce-cassandra",

        maven(commons_io),
    ],
    checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/examples/phonebook:kiji-phonebook-test",
    sources=["//kiji-phonebook/src/test/java"],
    deps=[
        "//org/kiji/examples/phonebook:kiji-phonebook",

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/schema:kiji-schema-shell-lib",

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/examples/phonebook:kiji-phonebook-pom",
    pom_name="//org/kiji/examples/phonebook:kiji-phonebook",
    pom_file="//kiji-phonebook/pom.xml",
    main_deps=["//org/kiji/examples/phonebook:kiji-phonebook"],
    test_deps=["//org/kiji/examples/phonebook:kiji-phonebook-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiSchema DDL Shell

scala_library(
    name="//org/kiji/schema:kiji-schema-shell-lib",
    sources=["//kiji-schema-shell/src/main/scala"],
    resources=["//kiji-schema-shell/src/main/resources"],
    deps=[
        maven(commons_lang3),
        maven(scala_jline),

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-extras",
    ],
)

java_binary(
    name="//org/kiji/schema:kiji-schema-shell",
    main_class="org.kiji.schema.shell.ShellMain",
    deps=[
        "//org/kiji/schema:kiji-schema-shell-lib",
    ],
)

avro_java_library(
    name="//org/kiji/schema:kiji-schema-shell-test-avro-lib",
    sources=["//kiji-schema-shell/src/test/avro/*.avdl"],
)

scala_test(
    name="//org/kiji/schema:kiji-schema-shell-test",
    sources=["//kiji-schema-shell/src/test/scala"],
    resources=["//kiji-schema-shell/src/test/resources"],
    deps=[
        maven(scalatest),
        maven(specs2),

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/schema:kiji-schema-shell-test-avro-lib",
        "//org/kiji/schema:kiji-schema-shell-lib",

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//org/kiji/schema:kiji-schema-shell-pom",
    pom_name="//org/kiji/schema:kiji-schema-shell-lib",
    pom_file="//kiji-schema-shell/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/schema:kiji-schema-shell-lib"],
    test_deps=["//org/kiji/schema:kiji-schema-shell-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiMR

avro_java_library(
    name="//org/kiji/mapreduce:kiji-mapreduce-avro-lib",
    sources=["//kiji-mapreduce/kiji-mapreduce/src/main/avro/*.avdl"],
)

java_library(
    name="//org/kiji/mapreduce:platform-api",
    sources=["//kiji-mapreduce/platform-api/src/main/java"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/delegation:kiji-delegation",

        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/mapreduce:cdh5-mrbridge",
    sources=["//kiji-mapreduce/cdh5-bridge/src/main/java"],
    resources=["//kiji-mapreduce/cdh5-bridge/src/main/resources/"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/delegation:kiji-delegation",
        "//org/kiji/mapreduce:platform-api",

        dynamic(kiji_platform="//org/kiji/platforms:cdh5.1-platform"),
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/mapreduce:kiji-mapreduce",
    sources=["//kiji-mapreduce/kiji-mapreduce/src/main/java"],
    resources=["//kiji-mapreduce/kiji-mapreduce/src/main/resources"],
    deps=[
        maven(slf4j_api),
        avro_mapred,

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-extras",
        "//org/kiji/mapreduce:platform-api",
        "//org/kiji/mapreduce:kiji-mapreduce-avro-lib",

        "//org/kiji/mapreduce:cdh5-mrbridge",          # brings cdh5.1-platform

        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_kiji,
        suppressions="//kiji-mapreduce/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_kiji,
    ),
)

java_library(
    name="//org/kiji/mapreduce:kiji-mapreduce-cassandra",
    sources=["//kiji-mapreduce/kiji-mapreduce-cassandra/src/main/java"],
    resources=["//kiji-mapreduce/kiji-mapreduce-cassandra/src/main/resources"],
    deps=[
        maven(guava),
        maven(slf4j_api),
        maven(commons_codec),
        maven(commons_io),
        maven(commons_lang),

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-cassandra",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/mapreduce:platform-api",
        "//org/kiji/mapreduce:kiji-mapreduce-avro-lib",

        dynamic(kiji_platform="//org/kiji/platforms:cassandra-platform"),
    ],
    maven_exclusions=[
        # Something is bringing in junit in compile scope.
        "junit:junit:*:*:*",
    ],
    checkstyle=checkstyle_kiji,
)

avro_java_library(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-lib-avro-lib",
    sources=[
        "//kiji-mapreduce-lib/kiji-mapreduce-lib/src/main/avro/*.avdl",
        "//kiji-mapreduce-lib/kiji-mapreduce-lib/src/main/avro/*.avsc",
    ],
)

java_library(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-lib",
    sources=["//kiji-mapreduce-lib/kiji-mapreduce-lib/src/main/java"],
    resources=["//kiji-mapreduce-lib/kiji-mapreduce-lib/src/main/resources"],
    deps=[
        maven(commons_codec),
        maven(commons_io),
        maven(commons_lang),
        maven(gson),
        maven(guava),
        maven(jsr305),

        "//org/kiji/mapreduce/lib:kiji-mapreduce-lib-avro-lib",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/mapreduce:kiji-mapreduce",

        "//org/kiji/hadoop:hadoop-configurator",
    ],
    checkstyle=checkstyle_kiji,
)

scala_library(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext",
    sources=["//kiji-mapreduce-lib/schema-shell-ext/src/main/scala"],
    resources=["//kiji-mapreduce-lib/schema-shell-ext/src/main/resources"],
    deps=[
        maven(gson),
        "//org/kiji/annotations:annotations",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-shell-lib",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/mapreduce/lib:kiji-mapreduce-lib",
    ],
)

scala_test(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext-test",
    sources=[
        "//kiji-mapreduce-lib/schema-shell-ext/src/test/scala",
        "//kiji-mapreduce-lib/schema-shell-ext/src/test/java",
    ],
    resources=["//kiji-mapreduce-lib/schema-shell-ext/src/test/resources"],
    deps=[
        maven(easymock),
        maven(scalatest),
        maven(specs2),
        "//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext",

        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/schema:kiji-schema-shell-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/mapreduce:kiji-mapreduce-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/mapreduce/lib:kiji-mapreduce-lib-test)",  # FIXME: extract kiji test framework
    ],
)

java_binary(
    name="//org/kiji/mapreduce:kijimr",
    main_class="org.kiji.schema.tools.KijiToolLauncher",
    deps=[
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/mapreduce:kiji-mapreduce",
    ],
)

# TODO: This produces test failures right now.
java_test(
    name="//org/kiji/mapreduce:kiji-mapreduce-test",
    sources=["//kiji-mapreduce/kiji-mapreduce/src/test/java"],
    resources=["//kiji-mapreduce/kiji-mapreduce/src/test/resources"],
    deps=[
        maven(easymock),
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/mapreduce:kiji-mapreduce",

        "//org/kiji/mapreduce:cdh5-mrbridge",          # brings cdh5.1-platform

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

java_test(
    name="//org/kiji/mapreduce:kiji-mapreduce-cassandra-test",
    sources=["//kiji-mapreduce/kiji-mapreduce-cassandra/src/test/java"],
    resources=["//kiji-mapreduce/kiji-mapreduce-cassandra/src/test/resources"],
    deps=[
        "java_library(//org/kiji/mapreduce:kiji-mapreduce-test)",  # FIXME: extract kiji test framework
        "//org/kiji/mapreduce:kiji-mapreduce-cassandra",

        dynamic(kiji_platform="//org/kiji/platforms:cassandra-test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

java_test(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-lib-test",
    sources=["//kiji-mapreduce-lib/kiji-mapreduce-lib/src/test/java"],
    resources=["//kiji-mapreduce-lib/kiji-mapreduce-lib/src/test/resources"],
    deps=[
        maven("org.apache.mrunit:mrunit:jar:hadoop2:1.1.0"),
        "//org/kiji/mapreduce/lib:kiji-mapreduce-lib",

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/mapreduce:kiji-mapreduce-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/mapreduce:platform-api-pom",
    pom_name="//org/kiji/mapreduce:platform-api",
    pom_file="//kiji-mapreduce/platform-api/pom.xml",
    main_deps=["//org/kiji/mapreduce:platform-api"],
)

generated_pom(
    name="//org/kiji/mapreduce:cdh5-bridge-pom",
    pom_name="//org/kiji/mapreduce:cdh5-bridge",
    pom_file="//kiji-mapreduce/cdh5-bridge/pom.xml",
    main_deps=["//org/kiji/mapreduce:cdh5-mrbridge"],
)

generated_pom(
    name="//org/kiji/mapreduce:kiji-mapreduce-pom",
    pom_name="//org/kiji/mapreduce:kiji-mapreduce",
    pom_file="//kiji-mapreduce/kiji-mapreduce/pom.xml",
    main_deps=["//org/kiji/mapreduce:kiji-mapreduce"],
    test_deps=["//org/kiji/mapreduce:kiji-mapreduce-test"],
)

generated_pom(
    name="//org/kiji/mapreduce:kiji-mapreduce-cassandra-pom",
    pom_name="//org/kiji/mapreduce:kiji-mapreduce-cassandra",
    pom_file="//kiji-mapreduce/kiji-mapreduce-cassandra/pom.xml",
    main_deps=["//org/kiji/mapreduce:kiji-mapreduce-cassandra"],
    test_deps=["//org/kiji/mapreduce:kiji-mapreduce-cassandra-test"],
)

generated_pom(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-lib-pom",
    pom_name="//org/kiji/mapreduce/lib:kiji-mapreduce-lib",
    pom_file="//kiji-mapreduce-lib/kiji-mapreduce-lib/pom.xml",
    main_deps=["//org/kiji/mapreduce/lib:kiji-mapreduce-lib"],
    test_deps=["//org/kiji/mapreduce/lib:kiji-mapreduce-lib-test"],
)

generated_pom(
    name="//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext-pom",
    pom_name="//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext",
    pom_file="//kiji-mapreduce-lib/schema-shell-ext/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext"],
    test_deps=["//org/kiji/mapreduce/lib:kiji-mapreduce-schema-shell-ext-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiMusic

avro_java_library(
    name="//org/kiji/examples/music:kiji-music-avro-lib",
    sources=["//kiji-music/src/main/avro/*.avdl"],
)

java_library(
    name="//org/kiji/examples/music:kiji-music",
    sources=["//kiji-music/src/main/java"],
    deps=[
        maven(commons_io),
        maven(json_simple),
        "//org/kiji/examples/music:kiji-music-avro-lib",

        "//org/kiji/schema:kiji-schema",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/mapreduce/lib:kiji-mapreduce-lib",
    ],
    checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/examples/music:kiji-music-test",
    sources=["//kiji-music/src/test/java"],
    resources=["//kiji-music/src/test/resources"],
    deps=[
        "//org/kiji/examples/music:kiji-music",

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/examples/music:kiji-music-pom",
    pom_name="//org/kiji/examples/music:kiji-music",
    pom_file="//kiji-music/pom.xml",
    main_deps=["//org/kiji/examples/music:kiji-music"],
    test_deps=["//org/kiji/examples/music:kiji-music-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiHive

java_library(
    name="//org/kiji/hive:kiji-hive-lib",
    sources=["//kiji-hive-adapter/kiji-hive-adapter/src/main/java"],
    deps=[
        # TODO: Move this to a platform perhaps?
        maven("org.apache.hive:hive-exec:0.12.0-cdh5.0.3"),
        maven("org.apache.hive:hive-serde:0.12.0-cdh5.0.3"),

        "//org/kiji/schema:kiji-schema",
    ],
    maven_exclusions=[
        "junit:junit:*:*:*",
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/hive:kiji-hive-tools",
    sources=["//kiji-hive-adapter/kiji-hive-tools/src/main/java"],
    deps=[
        "//org/kiji/schema:kiji-schema",
    ],
    checkstyle=checkstyle_kiji,
)

java_test(
    name="//org/kiji/hive:kiji-hive-test",
    sources=["//kiji-hive-adapter/kiji-hive-adapter/src/test/java"],
    deps=[
        # Don't use the avro-1.7.5-cdh5.0.3 dependency since it causes the kiji-schema test avro jar
        # to fail to load.
        avro,

        "//org/kiji/hive:kiji-hive-lib",
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_kiji,
        suppressions="//kiji-hive-adapter/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_kiji,
    ),
)

java_test(
    name="//org/kiji/hive:kiji-hive-tools-test",
    sources=["//kiji-hive-adapter/kiji-hive-tools/src/test/java"],
    deps=[
        "//org/kiji/hive:kiji-hive-tools",
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

generated_pom(
    name="//org/kiji/hive:kiji-hive-pom",
    pom_name="//org/kiji/hive:kiji-hive-lib",
    pom_file="//kiji-hive-adapter/kiji-hive-adapter/pom.xml",
    main_deps=["//org/kiji/hive:kiji-hive-lib"],
    test_deps=["//org/kiji/hive:kiji-hive-test"],
)

generated_pom(
    name="//org/kiji/hive:kiji-hive-tools-pom",
    pom_name="//org/kiji/hive:kiji-hive-tools",
    pom_file="//kiji-hive-adapter/kiji-hive-tools/pom.xml",
    main_deps=["//org/kiji/hive:kiji-hive-tools"],
    test_deps=["//org/kiji/hive:kiji-hive-tools-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiExpress

avro_java_library(
    name="//org/kiji/express:kiji-express-avro-lib",
    sources=[
        "//kiji-express/kiji-express/src/main/avro/*.avdl",
    ],
)

scala_library(
    name="//org/kiji/express:kiji-express-lib",
    sources=[
        "//kiji-express/kiji-express/src/main/java",
        "//kiji-express/kiji-express/src/main/scala",
    ],
    resources=["//kiji-express/kiji-express/src/main/resources"],
    deps=[
        "//org/kiji/annotations:annotations",
        "//org/kiji/deps:riemann-java-client",
        "//org/kiji/express:kiji-express-avro-lib",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-shell-lib",
        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
        maven("com.google.protobuf:protobuf-java:2.5.0"),
        maven("com.twitter.elephantbird:elephant-bird-core:4.4"),
        maven("com.twitter.elephantbird:elephant-bird-hadoop-compat:4.4"),
        maven(cascading_core),
        maven(cascading_hadoop),
        maven(cascading_kryo),
        maven(cascading_local),
        maven(fasterxml_jackson_module_scala),
        maven(kryo),
        maven(scalding_args),
        maven(scalding_core),
    ],
)

java_binary(
    name="//org/kiji/express:kiji-express",
    main_class="org.kiji.express.flow.ExpressTool",
    deps=[
        "//org/kiji/express:kiji-express-lib",
    ],
)

avro_java_library(
    name="//org/kiji/express:kiji-express-test-avro-lib",
    sources=[
        "//kiji-express/kiji-express/src/test/avro/*.avdl",
    ],
)

# TODO: These tests take forever. Add support for native-libs (jvm properties).
scala_test(
    name="//org/kiji/express:kiji-express-test",
    # Exclude KijiSuite and SerDeSuite since these are traits.
    test_name_pattern=".*(?<!^Kiji)(?<!SerDe)Suite$",
    sources=["//kiji-express/kiji-express/src/test/scala"],
    resources=["//kiji-express/kiji-express/src/test/resources"],
    deps=[
        maven(scalatest),
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/schema:kiji-schema-shell-lib",
        "//org/kiji/express:kiji-express-test-avro-lib",
        "//org/kiji/express:kiji-express-lib",
        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

scala_library(
    name="//org/kiji/express:kiji-express-examples",
    sources=["//kiji-express/kiji-express-examples/src/main/scala"],
    resources=["//kiji-express/kiji-express-examples/src/main/resources"],
    deps=[
        avro,
        maven(scalding_core),
        maven(scalding_args),
        maven(cascading_core),
        "//org/kiji/express:kiji-express-lib",
        "//org/kiji/schema:kiji-schema",
        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
)

scala_test(
    name="//org/kiji/express:kiji-express-examples-test",
    test_name_pattern=".*Suite$",
    sources=["//kiji-express/kiji-express-examples/src/test/scala"],
    resources=["//kiji-express/kiji-express-examples/src/test/resources"],
    deps=[
        maven(scalatest),
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "//org/kiji/express:kiji-express-examples",
        "java_library(//org/kiji/express:kiji-express-test)",  # FIXME: extract kiji test framework
        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

scala_library(
    name="//org/kiji/express:kiji-express-tools",
    sources=["//kiji-express/kiji-express-tools/src/main/scala"],
    deps=[
        maven(scala_compiler),
        maven(scala_jline),
        maven(scala_reflect),
        avro,
        maven(guava),
        maven(scalding_core),
        maven(scalding_args),
        maven(cascading_core),
        "//org/kiji/annotations:annotations",
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-shell",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/express:kiji-express-lib",
    ],
)

scala_test(
    name="//org/kiji/express:kiji-express-tools-test",
    test_name_pattern=".*Suite$",
    sources=["//kiji-express/kiji-express-tools/src/test/scala"],
    deps=[
        maven(scalatest),
        "//org/kiji/testing:fake-hbase",
        "//org/kiji/express:kiji-express-tools",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/express:kiji-express-test)",  # FIXME: extract kiji test framework
        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

avro_java_library(
    name="//org/kiji/express:kiji-express-music-avro-lib",
    sources=["//kiji-express-music/src/main/avro/*.avdl"],
)

scala_library(
    name="//org/kiji/express:kiji-express-music",
    sources=["//kiji-express-music/src/main/scala"],
    resources=["//kiji-express-music/src/main/resources"],
    deps=[
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-cassandra",
        "//org/kiji/schema:kiji-schema-shell-lib",
        "//org/kiji/mapreduce:kiji-mapreduce",
        "//org/kiji/mapreduce:kiji-mapreduce-cassandra",
        "//org/kiji/express:kiji-express-lib",
        "//org/kiji/express:kiji-express-music-avro-lib",

        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
)

scala_test(
    name="//org/kiji/express:kiji-express-music-test",
    test_name_pattern=".*Suite$",
    sources=["//kiji-express-music/src/test/scala"],
    resources=["//kiji-express-music/src/test/resources"],
    deps=[
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/express:kiji-express-test)",  # FIXME: extract kiji test framework
        "//org/kiji/express:kiji-express-music",

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),

        maven(scalatest),
    ],
)

generated_pom(
    name="//org/kiji/express:kiji-express-pom",
    pom_name="//org/kiji/express:kiji-express-lib",
    pom_file="//kiji-express/kiji-express/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/express:kiji-express-lib"],
    test_deps=["//org/kiji/express:kiji-express-test"],
)

generated_pom(
    name="//org/kiji/express:kiji-express-examples-pom",
    pom_name="//org/kiji/express:kiji-express-examples",
    pom_file="//kiji-express/kiji-express-examples/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/express:kiji-express-examples"],
    test_deps=["//org/kiji/express:kiji-express-examples-test"],
)

generated_pom(
    name="//org/kiji/express:kiji-express-tools-pom",
    pom_name="//org/kiji/express:kiji-express-tools",
    pom_file="//kiji-express/kiji-express-tools/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/express:kiji-express-tools"],
    test_deps=["//org/kiji/express:kiji-express-tools-test"],
)

generated_pom(
    name="//org/kiji/express:kiji-express-music-pom",
    pom_name="//org/kiji/express:kiji-express-music",
    pom_file="//kiji-express-music/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/express:kiji-express-music"],
    test_deps=["//org/kiji/express:kiji-express-music-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiSpark

scala_library(
    name="//org/kiji/deps:spark-core",
    deps=[
        maven(spark_core),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        # This is a temporary hack.
        "joda-time:joda-time:*:*:[2.2,)",

    ],
    provides=["spark_core"]
)

scala_library(
    name="//org/kiji/deps:spark-mllib",
    deps=[
        maven(spark_mllib),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        # This is a temporary hack.
        "joda-time:joda-time:*:*:[2.2,)",

    ],
    provides=["spark_mllib"]
)

scala_library(
    name="//org/kiji/spark:kiji-spark",
    sources=["//kiji-spark/src/main/scala"],
    deps=[
        "//org/kiji/schema:kiji-schema",
        "//org/kiji/schema:kiji-schema-cassandra",
        dynamic(spark_core="//org/kiji/deps:spark-core"),
        maven("joda-time:joda-time:2.6"),
        # CDH 5.3 platform will be the first version to support KijiSpark
        # as its the first release to include Spark 1.2.x
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.3-platform"),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        "joda-time:joda-time:*:*:[2.2,)",
    ]
)

scala_test(
    name="//org/kiji/spark:kiji-spark-test",
    test_name_pattern=".*Suite$",
    sources=["//kiji-spark/src/test/scala"],
    deps=[
        "//org/kiji/spark:kiji-spark",
        "//org/kiji/schema:kiji-schema",
        "java_library(//org/kiji/schema:kiji-schema-test)",
        "//org/kiji/schema:kiji-schema-cassandra",
        "//org/kiji/commons:kiji-commons-scala",
        maven(spark_core),
        maven("joda-time:joda-time:2.6"),
        maven(scalatest),
        # CDH 5.3 platform will be the first version to support KijiSpark
        # as its the first release to include Spark 1.2.x
        dynamic(kiji_platform="//org/kiji/platforms:cdh5.3-platform"),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        "joda-time:joda-time:*:*:[2.2,)",
    ]
)

generated_pom(
    name="//org/kiji/spark:kiji-spark-pom",
    pom_name="//org/kiji/spark:kiji-spark",
    pom_file="//kiji-spark/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/spark:kiji-spark"],
    test_deps=["//org/kiji/spark:kiji-spark-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiModeling

scala_library(
    name="//org/kiji/modeling:kiji-modeling",
    sources=["//kiji-modeling/kiji-modeling/src/main/scala"],
    deps=["//org/kiji/express:kiji-express-lib"],
)

avro_java_library(
    name="//org/kiji/modeling:kiji-modeling-examples-avro-lib",
    sources=["//kiji-modeling/kiji-modeling-examples/src/main/avro/*.avdl"],
)

scala_library(
    name="//org/kiji/modeling:kiji-modeling-examples",
    sources=["//kiji-modeling/kiji-modeling-examples/src/main/scala"],
    deps=[
        "//org/kiji/express:kiji-express-lib",
        "//org/kiji/modeling:kiji-modeling",
        "//org/kiji/modeling:kiji-modeling-examples-avro-lib",

        dynamic(kiji_platform="//org/kiji/platforms:compile-platform"),
    ],
)

scala_test(
    name="//org/kiji/modeling:kiji-modeling-test",
    test_name_pattern=".*Suite$",
    sources=["//kiji-modeling/kiji-modeling/src/test/scala"],
    deps=[
        maven(easymock),
        maven(scalatest),
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/schema:kiji-schema-shell-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/express:kiji-express-test)",  # FIXME: extract kiji test framework
        "//org/kiji/modeling:kiji-modeling",

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

scala_test(
    name="//org/kiji/modeling:kiji-modeling-examples-test",
    sources=["//kiji-modeling/kiji-modeling-examples/src/test/scala"],
    resources=["//kiji-modeling/kiji-modeling-examples/src/test/resources"],
    deps=[
        maven(scalatest),
        "//org/kiji/modeling:kiji-modeling-examples",

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/schema:kiji-schema-shell-test)",  # FIXME: extract kiji test framework
        "java_library(//org/kiji/express:kiji-express-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//org/kiji/modeling:kiji-modeling-pom",
    pom_name="//org/kiji/modeling:kiji-modeling",
    pom_file="//kiji-modeling/kiji-modeling/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/modeling:kiji-modeling"],
    test_deps=["//org/kiji/modeling:kiji-modeling-test"],
)

generated_pom(
    name="//org/kiji/modeling:kiji-modeling-examples-pom",
    pom_name="//org/kiji/modeling:kiji-modeling-examples",
    pom_file="//kiji-modeling/kiji-modeling-examples/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//org/kiji/modeling:kiji-modeling-examples"],
    test_deps=["//org/kiji/modeling:kiji-modeling-examples-test"],
)

# --------------------------------------------------------------------------------------------------
# KijiREST

java_library(
    name="//org/kiji/rest:kiji-rest-lib",
    sources=["//kiji-rest/kiji-rest/src/main/java"],
    deps=[
        # Something pulls in jersey 1.9.
        maven("com.sun.jersey:jersey-core:1.18.1"),
        maven("com.sun.jersey:jersey-json:1.18.1"),
        maven("com.sun.jersey.contribs:jersey-guice:1.18.1"),
        maven("javax.servlet:javax.servlet-api:3.0.1"),

        maven(dropwizard_core),
        maven("org.eclipse.jetty:jetty-servlets:9.0.7.v20131107"),

        "//org/kiji/schema:kiji-schema",
    ],
    maven_exclusions=[
        "org.eclipse.jetty.orbit:javax.servlet:*:*:*",
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/rest:kiji-rest-monitoring-lib",
    sources=["//kiji-rest/monitoring/src/main/java"],
    resources=["//kiji-rest/monitoring/src/main/resources"],
    deps=[
        maven("io.dropwizard:dropwizard-metrics:0.7.1"),
        "//org/kiji/commons:kiji-commons-monitoring",
    ],
    checkstyle=checkstyle_kiji,
)

java_library(
    name="//org/kiji/rest:kiji-rest-standard-plugin",
    sources=["//kiji-rest/standard-plugin/src/main/java"],
    resources=["//kiji-rest/standard-plugin/src/main/resources"],
    deps=["//org/kiji/rest:kiji-rest-lib"],
)

avro_java_library(
    name="//org/kiji/rest:kiji-rest-standard-plugin-test-avro-lib",
    sources=["//kiji-rest/standard-plugin/src/test/avro/*.avdl"],
)

java_test(
    name="//org/kiji/rest:kiji-rest-test",
    sources=["//kiji-rest/kiji-rest/src/test/java"],
    resources=["//kiji-rest/kiji-rest/src/test/resources"],
    deps=[
        maven(dropwizard_testing),

        "//org/kiji/rest:kiji-rest-lib",
        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_kiji_test,
)

java_test(
    name="//org/kiji/rest:kiji-rest-monitoring-test",
    sources=["//kiji-rest/monitoring/src/test/java"],
    resources=["//kiji-rest/monitoring/src/test/resources"],
    deps=[
        "//org/kiji/rest:kiji-rest-monitoring-lib",
    ],
    checkstyle=checkstyle_kiji_test,
)

java_test(
    name="//org/kiji/rest:kiji-rest-standard-plugin-test",
    sources=["//kiji-rest/standard-plugin/src/test/java"],
    deps=[
        maven(guava),
        maven(dropwizard_core),
        maven(dropwizard_testing),

        "//org/kiji/deps:jackson",
        "java_library(//org/kiji/rest:kiji-rest-test)",
        "//org/kiji/rest:kiji-rest-standard-plugin",
        "//org/kiji/rest:kiji-rest-standard-plugin-test-avro-lib",

        "//org/kiji/testing:fake-hbase",
        "java_library(//org/kiji/schema:kiji-schema-test)",  # FIXME: extract kiji test framework

        dynamic(kiji_platform="//org/kiji/platforms:test-platform"),
    ],
)

java_binary(
    name="//org/kiji/rest:kiji-rest",
    main_class="org.kiji.rest.KijiRESTService",
    deps=["//org/kiji/rest:kiji-rest-lib"],
    maven_exclusions=[
        "org.slf4j:slf4j-log4j12:*:*:*",
        "org.slf4j:log4j-over-slf4j:*:*:*",
    ],
)

generated_pom(
    name="//org/kiji/rest:kiji-rest-pom",
    pom_name="//org/kiji/rest:kiji-rest-lib",
    pom_file="//kiji-rest/kiji-rest/pom.xml",
    main_deps=["//org/kiji/rest:kiji-rest-lib"],
    test_deps=["//org/kiji/rest:kiji-rest-test"],
)

generated_pom(
    name="//org/kiji/rest:kiji-rest-standard-plugin-pom",
    pom_name="//org/kiji/rest:kiji-rest-standard-plugin",
    pom_file="//kiji-rest/standard-plugin/pom.xml",
    main_deps=["//org/kiji/rest:kiji-rest-standard-plugin"],
    test_deps=["//org/kiji/rest:kiji-rest-standard-plugin-test"],
)

generated_pom(
    name="//org/kiji/rest:kiji-rest-monitoring-pom",
    pom_name="//org/kiji/rest:kiji-rest-monitoring",
    pom_file="//kiji-rest/monitoring/pom.xml",
    main_deps=["//org/kiji/rest:kiji-rest-monitoring-lib"],
    test_deps=["//org/kiji/rest:kiji-rest-monitoring-test"],
)

# TODO: custom versions of kiji-rest for different platforms
