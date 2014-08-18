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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.bulkimport.impl.BulkImportMapper;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.TestingFileUtils;

public class TestKijiBulkImportJobBuilder extends KijiClientTest {
  /** A KijiBulkImporter for testing that the job-builder propagates this class correctly. */
  public static class NoopBulkImporter extends KijiBulkImporter<Void, Void> {
    /** {@inheritDoc} */
    @Override
    public void produce(Void key, Void value, KijiTableContext context)
        throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /** A BulkImporter implementation that uses a KeyValueStore. */
  public static class KVStoreBulkImporter extends NoopBulkImporter {
    /** {@inheritDoc} */
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.just("foostore", EmptyKeyValueStore.get());
    }
  }

  private File mTempDir;
  private Path mTempPath;
  private KijiTable mTable;

  @Before
  public void setUp() throws Exception {
    mTempDir = TestingFileUtils.createTempDir("test", "dir");
    mTempPath = new Path("file://" + mTempDir);

    getConf().set("fs.defaultFS", mTempPath.toString());
    getConf().set("fs.default.name", mTempPath.toString());
    final KijiTableLayout layout = KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    getKiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", new Path(mTempPath, "workdir").toString());

    mTable = getKiji().openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
    ResourceUtils.releaseOrLog(mTable);
    mTempDir = null;
    mTempPath = null;
    mTable = null;
  }

  @Test
  public void testBuildWithHFileOutput() throws Exception {
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(mTempPath, "input")))
        .withBulkImporter(NoopBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path(mTempPath, "output"), 10))
        .build();

    final Job job = mrjob.getHadoopJob();
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(NoopBulkImporter.class,
        job.getConfiguration().getClass(KijiConfKeys.KIJI_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());
  }

  @Test
  public void testBuildWithKeyValueStore() throws Exception {
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(mTempPath, "input")))
        .withBulkImporter(KVStoreBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mTable.getURI(), new Path(mTempPath, "output"), 10))
        .build();

    final Job job = mrjob.getHadoopJob();
    // Verify that everything else is what we expected as in the previous test
    // (except the bulk importer class name)...
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(KVStoreBulkImporter.class,
        job.getConfiguration().getClass(KijiConfKeys.KIJI_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());

    // KeyValueStore-specific checks here.
    final Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }
}
