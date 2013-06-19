/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.framework.KijiTableInputFormat;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.mapreduce.produce.impl.ProduceMapper;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

public class TestKijiProduceJobBuilder extends KijiClientTest {

  /** A producer to use in the test job.  It writes "stuff" to family:column. */
  public static class MyProducer extends KijiProducer {
    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "info:email";
    }

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "email");
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context)
        throws IOException {
      context.put("stuff");
    }
  }

  /** Producer that requires a KeyValueStore but doesn't specify a good default. */
  public static class UnconfiguredKVProducer extends MyProducer {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.with("foostore", UnconfiguredKeyValueStore.get());
    }
  }

  /** Test table, owned by this test. */
  private KijiTable mTable;

  @Before
  public final void setupTestKijiProducer() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    getKiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", "file://" + getLocalTempDir() + "/workdir");

    mTable = getKiji().openTable("test");
  }

  @After
  public void teardownTestKijiProducer() throws Exception {
    ResourceUtils.releaseOrLog(mTable);
    mTable = null;
  }

  @Test
  public void testBuildWithHFileOutput() throws ClassNotFoundException, IOException {

    final KijiMapReduceJob produceJob = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(MyProducer.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path("foo/bar"), 10))
        .build();

    // Verify that the MR Job was configured correctly.
    final Job job = produceJob.getHadoopJob();
    assertEquals(KijiTableInputFormat.class, job.getInputFormatClass());
    assertEquals(ProduceMapper.class, job.getMapperClass());
    assertEquals(MyProducer.class,
        job.getConfiguration().getClass(KijiConfKeys.KIJI_PRODUCER_CLASS, null));
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
  }

  @Test
  public void testUnconfiguredKeyValueStore() throws ClassNotFoundException, IOException {
    // Should explode as we don't define a KVStore for 'foostore', but the class requires one

    // This should throw an exception because we didn't provide a better KVStore
    // than the UnconfiguredKeyValueStore in the default.
    try {
      KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(UnconfiguredKVProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
      fail("Should have thrown an IOException.");
    } catch (IOException ioe) {
      assertEquals("Cannot use an UnconfiguredKeyValueStore. "
          + "You must override this on the command line or in a JobBuilder.",
          ioe.getMessage());
    }
  }

  @Test
  public void testEmptyKeyValueStore() throws ClassNotFoundException, IOException {
    // We override UnconfiguredKeyValueStore with EmptyKeyValueStore; this should succeed.
    KijiMapReduceJob produceJob = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withProducer(UnconfiguredKVProducer.class)
        .withStore("foostore", EmptyKeyValueStore.get())
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();

    // Verify that the MR Job was configured correctly.
    Job job = produceJob.getHadoopJob();
    Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test
  public void testBuildWithDifferentTableOutput() throws ClassNotFoundException, IOException {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout("other"));
    getKiji().createTable(layout.getName(), layout);

    final KijiTable otherTable = getKiji().openTable("other");
    try {
      // This should throw a job configuration exception because the output table does not
      // match the input table.
      KijiProduceJobBuilder.create()
          .withConf(getConf())
          .withInputTable(mTable.getURI())
          .withProducer(MyProducer.class)
          .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(otherTable.getURI()))
          .build();
      fail("Should have thrown a JobConfigurationException.");
    } catch (JobConfigurationException jce) {
      assertEquals("Output table must be the same as the input table.", jce.getMessage());
    } finally {
      ResourceUtils.releaseOrLog(otherTable);
    }
  }
}
