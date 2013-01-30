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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiHFileOutputFormat;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.TestFileUtils;

public class TestKijiGatherJobBuilder extends KijiClientTest {
  // -----------------------------------------------------------------------------------------------

  /** Regular gatherer that emits (Text, Text) pairs. */
  public static class SimpleGatherer extends KijiGatherer<Text, Text> {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("info", "email"));
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData input, MapReduceContext<Text, Text> context)
        throws IOException {
      // Gathering logic, unused in these tests.
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Gatherer that emits Kiji puts to HFiles. */
  public static class GatherToHFile extends KijiGatherer<HFileKeyValue, NullWritable> {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("info", "email"));
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData input, MapReduceContext<HFileKeyValue, NullWritable> context)
        throws IOException {
      // Gathering logic, unused in these tests.
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return HFileKeyValue.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Gatherer that requires a KV store. */
  public static class UnconfiguredKVGatherer extends SimpleGatherer {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("foostore", UnconfiguredKeyValueStore.builder().build());
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Combiner to use in the test job. */
  public static class MyCombiner extends KijiBaseReducer<Text, Text, Text, Text> {
    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Reducer to use in the test job. */
  public static class MyReducer extends KijiBaseReducer<Text, Text, Text, Text> {
    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Dummy table reducer. */
  public static class ReducerToHFile extends KijiTableReducer<Text, Text> {

    /** {@inheritDoc} */
    @Override
    protected void reduce(Text key, Iterable<Text> values, KijiTableContext context)
        throws IOException {
      // Reducing logic, unused here.
    }
  }

  // -----------------------------------------------------------------------------------------------

  private File mTempDir;
  private Path mTempPath;
  private KijiTable mTable;

  @Before
  public void setUp() throws Exception {
    try {
      // Make doSetUp() errors visible:
      doSetUp();
    } catch (Exception thr) {
      thr.printStackTrace();
      throw thr;
    }
  }

  private void doSetUp() throws Exception {
    mTempDir = TestFileUtils.createTempDir("test", "dir");
    mTempPath = new Path("file://" + mTempDir.getAbsolutePath());

    final Configuration conf = getConf();
    conf.set("fs.defaultFS", mTempPath.toString());
    conf.set("fs.default.name", mTempPath.toString());
    final KijiTableLayout layout = new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);
    getKiji().getAdmin().createTable("test", layout, false);

    // Set the working directory so that it gets cleaned up after the test:
    conf.set("mapred.working.dir", new Path(mTempPath, "workdir").toString());

    mTable = getKiji().openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
    mTable.close();
    mTempDir = null;
    mTempPath = null;
    mTable = null;
  }

  @Test
  public void testBuildValid() throws Exception {
    final MapReduceJob gatherJob = KijiGatherJobBuilder.create()
        .withInputTable(mTable)
        .withGatherer(SimpleGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
        .build();

    // TODO: Verify that the MR Job was configured correctly.
    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(SimpleGatherer.class.getName(), conf.get(KijiConfKeys.KIJI_GATHERER_CLASS));
    assertEquals(MyCombiner.class, job.getCombinerClass());
    assertEquals(MyReducer.class, job.getReducerClass());
  }

  @Test
  public void testGatherToHFile() throws Exception {
    final MapReduceJob gatherJob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable)
        .withGatherer(GatherToHFile.class)
        .withOutput(new HFileMapReduceJobOutput(mTable, new Path(mTempPath, "/hfile"), 10))
        .build();

    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(GatherToHFile.class.getName(), conf.get(KijiConfKeys.KIJI_GATHERER_CLASS));
    assertEquals(null, job.getCombinerClass());
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(HFileKeyValue.class, job.getOutputKeyClass());
    assertEquals(NullWritable.class, job.getOutputValueClass());
  }

  @Test
  public void testGatherReducerToHFile() throws Exception {
    final MapReduceJob gatherJob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable)
        .withGatherer(SimpleGatherer.class)
        .withReducer(ReducerToHFile.class)
        .withOutput(new HFileMapReduceJobOutput(mTable, new Path(mTempPath, "/hfile"), 10))
        .build();

    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(SimpleGatherer.class.getName(), conf.get(KijiConfKeys.KIJI_GATHERER_CLASS));
    assertEquals(null, job.getCombinerClass());
    assertEquals(ReducerToHFile.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(SequenceFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(HFileKeyValue.class, job.getOutputKeyClass());
    assertEquals(NullWritable.class, job.getOutputValueClass());
  }

  @Test(expected=IOException.class)
  public void testUnconfiguredKeyValueStore() throws Exception {
    // Should explode as we don't define a KVStore for 'foostore', but the class requires one:
    KijiGatherJobBuilder.create()
        .withInputTable(mTable)
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
        .build();
  }

  @Test
  public void testEmptyKeyValueStore() throws Exception {
    // We override UnconfiguredKeyValueStore with EmptyKeyValueStore; this should succeed.
    final MapReduceJob gatherJob = KijiGatherJobBuilder.create()
        .withInputTable(mTable)
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
        .withStore("foostore", EmptyKeyValueStore.builder().build())
        .build();

    // Verify that the MR Job was configured correctly.
    final Job job = gatherJob.getHadoopJob();
    final Configuration conf = job.getConfiguration();
    assertEquals(1, conf.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        conf.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        conf.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }
}
