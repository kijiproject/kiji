/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.framework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraKijiInstaller;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/** Tests running a table map/reducer. */
public class IntegrationTestCassandraTableMapReducer {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestCassandraTableMapReducer.class);

  @Rule
  public TestName mTestName = new TestName();


  private static KijiURI mUri;
  private static KijiURI mTableUri;

  @BeforeClass
  public static void populateTable() throws Exception {
    mUri = CassandraKijiMapReduceIntegrationTestUtil.getGloballyUniqueCassandraKijiUri();
    LOG.info("Installing to URI " + mUri);
    try {
      CassandraKijiInstaller.get().install(mUri, HBaseConfiguration.create());
      LOG.info("Created Kiji instance at " + mUri);
    } catch (IOException ioe) {
      LOG.warn("Could not create Kiji instance.");
      assertTrue("Did not start.", false);
    }

    // Create a table with a simple table layout.
    final Kiji kiji = Kiji.Factory.open(mUri);
    final KijiTableLayout layout =
        KijiTableLayouts.getTableLayout("org/kiji/mapreduce/layout/foo-test-rkf2.json");
    final long timestamp = System.currentTimeMillis();

    // Insert some data into the table.
    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
        .withRow("amy@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "amy@foo.com")
        .withQualifier("name").withValue(timestamp, "Amy")
        .withRow("bob@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "bob@foo.com")
        .withQualifier("name").withValue(timestamp, "Bob")
        .withRow("christine@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "christine@foo.com")
        .withQualifier("name").withValue(timestamp, "Christine")
        .withRow("dan@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "dan@foo.com")
        .withQualifier("name").withValue(timestamp, "Dan")
        .withRow("erin@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "erin@foo.com")
        .withQualifier("name").withValue(timestamp, "Erin")
        .withRow("frank@foo.com")
        .withFamily("info")
        .withQualifier("email").withValue(timestamp, "frank@foo.com")
        .withQualifier("name").withValue(timestamp, "Frank")
        .build();

    mTableUri = KijiURI.newBuilder(mUri.toString()).withTableName("foo").build();
    LOG.info("Table URI = " + mTableUri);
    kiji.release();
  }


  private Configuration createConfiguration() {
    return HBaseConfiguration.create();
  }

  private Path createOutputFile() {
    return new Path(String.format("/%s-%s-%d/part-r-00000",
        getClass().getName(), mTestName.getMethodName(), System.currentTimeMillis()));
  }

  @Test
  public void testGatherer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);

    final Path output = createOutputFile();

    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(conf)
        .withGatherer(TableMapper.class)
        .withReducer(TableReducer.class)
        .withInputTable(mTableUri)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(output.getParent(), 1))
        .build();

    if (!mrjob.run()) {
      Assert.fail("MapReduce job failed");
    }
    // Verify that the output matches what's expected.
    // Should just see email address and name.
    assertTrue(fs.exists(output.getParent()));
    final FSDataInputStream in = fs.open(output);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet(
        "amy@foo.com\tAmy",
        "bob@foo.com\tBob",
        "christine@foo.com\tChristine",
        "dan@foo.com\tDan",
        "erin@foo.com\tErin",
        "frank@foo.com\tFrank"
    );
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(output.getParent(), true);

    IOUtils.closeQuietly(in);
  }

  @Test
  public void testKvs() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);

    final Path output = createOutputFile();

    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(conf)
        .withGatherer(MapperWithKeyValueStore.class)
        .withReducer(TableReducer.class)
        .withInputTable(mTableUri)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(output.getParent(), 1))
        .build();

    if (!mrjob.run()) {
      Assert.fail("MapReduce job failed");
    }
    // Verify that the output matches what's expected.
    // Should just see email address and name.
    assertTrue(fs.exists(output.getParent()));
    final FSDataInputStream in = fs.open(output);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet(
        "amy@foo.com\tAmy",
        "bob@foo.com\tBob",
        "christine@foo.com\tChristine",
        "dan@foo.com\tDan",
        "erin@foo.com\tErin",
        "frank@foo.com\tFrank"
    );
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(output.getParent(), true);

    IOUtils.closeQuietly(in);
  }

  @Test
  public void testProducer() throws Exception {
    final Configuration conf = createConfiguration();
    final FileSystem fs = FileSystem.get(conf);




    final Path output = new Path(fs.getUri().toString(),
        String.format("/%s-%d/table-mr-output", getClass().getName(), System.currentTimeMillis()));

    final KijiMapReduceJob mrjob = KijiProduceJobBuilder.create()
        .withConf(conf)
        .withProducer(DomainProducer.class)
        .withInputTable(mTableUri)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTableUri))
        .build();
    if (!mrjob.run()) {
      Assert.fail("MapReduce job failed");
    }

    // Validate that the results are correct.
    final Kiji kiji = Kiji.Factory.open(mTableUri);
    KijiTable table = kiji.openTable(mTableUri.getTable());
    try {
      KijiTableReader reader = table.openTableReader();
      try {
        KijiDataRequest dataRequest = KijiDataRequest.create("derived", "domain");
        List<String> names = Lists.newArrayList("Amy", "Bob", "Christine", "Dan", "Erin", "Frank");
        for (String name : names) {
          EntityId entityId = table.getEntityId(name.toLowerCase() + "@foo.com");
          KijiRowData rowData = reader.get(entityId, dataRequest);
          assertNotNull(rowData.getMostRecentCell("derived", "domain"));
          assertEquals("foo.com",
              rowData.getMostRecentCell("derived", "domain").getData().toString());
        }
      } finally {
        reader.close();
      }
    } finally {
      table.release();
      kiji.release();
    }
  }

  /** Table mapper that processes Kiji rows and emits (key, value) pairs. */
  public static class TableMapper extends KijiGatherer<Text, Text> {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    // Writes out email as the key and name as the value.
    @Override
    public void gather(KijiRowData input, GathererContext<Text, Text> context)
        throws IOException {
      try {
        LOG.info("Row key = " + input.getEntityId());
        final String email = input.getMostRecentValue("info", "email").toString();
        final String name = input.getMostRecentValue("info", "name").toString();
        LOG.info("Writing key, value = " + email + ", " + name);
        context.write(new Text(email), new Text(name));
      } catch (NullPointerException npe) {
        LOG.info("Problem getting email and name from row data " + input);
      }
    }

  }

  /** Table reduces that processes (key, value) pairs and emits Kiji puts. */
  public static class TableReducer extends KijiReducer<Text, Text, Text, Text> {
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    // This reducer is just the identity function.
    @Override
    protected void reduce(
        Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
      for (Text text : values) {
        LOG.info("Writing key, value = " + key.toString() + ", " + text.toString());
        context.write(key, text);
      }
    }
  }

  /** Producer that gets the domain from an e-mail address. */
  public static class DomainProducer extends KijiProducer {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    @Override
    public String getOutputColumn() {
      return "derived:domain";
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      try {
        LOG.info("Row key = " + input.getEntityId());
        final String email = input.getMostRecentValue("info", "email").toString();
        final String name = input.getMostRecentValue("info", "name").toString();

        // Split off the domain name from the e-mail address.
        if (email.contains("@")) {
          List<String> userAndDomain = Lists.newArrayList(Splitter.on("@").split(email));
          if (userAndDomain.size() == 2) {
            final String domain = userAndDomain.get(1);
            context.put(domain);
          }
        }
      } catch (NullPointerException npe) {
        LOG.info("Problem getting email and name from row data " + input);
      }
    }
  }

  /** Mapper with a Kiji table KVS. */
  public static class MapperWithKeyValueStore extends KijiGatherer<Text, Text> {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("kiji-table", KijiTableKeyValueStore.builder()
      .withTable(mTableUri)
      .withColumn("info", "name")
      .build()
      );
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    // Writes out email as the key and name as the value.
    @Override
    public void gather(KijiRowData input, GathererContext<Text, Text> context)
        throws IOException {

      LOG.info("Row key = " + input.getEntityId());
      final String email = input.getMostRecentValue("info", "email").toString();
      final String name = input.getMostRecentValue("info", "name").toString();
      LOG.info("Got name " + name + " and e-mail " + email);

      KeyValueStoreReader<KijiRowKeyComponents, Utf8> nameStore = context.getStore("kiji-table");
      Utf8 nameFromStore = nameStore.get(KijiRowKeyComponents.fromComponents(email));
      if (nameFromStore == null || !nameFromStore.toString().equals(name)) {
        LOG.info("Name from KVS is " + nameFromStore + ". NO MATCH!");
      } else {
        LOG.info("Name from KVS matches name from table.");
        LOG.info("Writing key, value = " + email + ", " + name);
        context.write(new Text(email), new Text(name));
      }
    }

  }
}
