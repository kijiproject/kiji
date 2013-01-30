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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.mapper.BulkImportMapper;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiHFileOutputFormat;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiBulkImportJobBuilder extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiBulkImportJobBuilder.class);

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temp directory to use for test resources. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /** A KijiBulkImporter for testing that the job-builder propagates this class correctly. */
  protected static class NoopBulkImporter extends KijiBulkImporter<Void, Void> {
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

  @Before
  public void setupLayout() throws Exception {
    getKiji().getAdmin()
        .createTable("table", KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE), false);
  }

  @Test
  public void testBuildWithHFileOutput() throws ClassNotFoundException, IOException {
    Kiji kiji = createMock(Kiji.class);
    // HFileMapReduceJobOutput is dependant on the HBase implementation of KijiTable
    HBaseKijiTable myTable = createMock(HBaseKijiTable.class);
    HTable htable = createMock(HTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set the working directory to the temp dir, so that it gets cleaned up
    // at the end of this test.
    conf.set("mapred.working.dir", mTempDir.getRoot().getAbsolutePath());

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();
    expect(myTable.getHTable()).andReturn(htable).anyTimes();

    replay(kiji);
    replay(myTable);
    replay(htable);

    LOG.info("Configuring job...");
    KijiBulkImportJobBuilder builder = KijiBulkImportJobBuilder.create()
        .withInput(new TextMapReduceJobInput(new Path("/path/to/input")))
        .withBulkImporter(NoopBulkImporter.class)
        .withOutput(new HFileMapReduceJobOutput(myTable, new Path("/path/to/output"), 10));

    LOG.info("Building job...");
    MapReduceJob bulkImportJob = builder.build();

    verify(kiji);
    verify(myTable);
    verify(htable);

    LOG.info("Verifying job configuration...");
    Job job = bulkImportJob.getHadoopJob();
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(NoopBulkImporter.class,
        job.getConfiguration().getClass(KijiBulkImporter.CONF_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());
  }

  @Test
  public void testBuildWithKeyValueStore() throws ClassNotFoundException, IOException {
    Kiji kiji = createMock(Kiji.class);
    // HFileMapReduceJobOutput is dependant on the HBase implementation of KijiTable
    HBaseKijiTable myTable = createMock(HBaseKijiTable.class);
    HTable htable = createMock(HTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set the mapred.working.dir so the partition files get written somewhere reasonable.
    conf.set("mapred.working.dir", mTempDir.getRoot().getAbsolutePath());

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();
    expect(myTable.getHTable()).andReturn(htable).anyTimes();

    replay(kiji);
    replay(myTable);
    replay(htable);

    LOG.info("Configuring job...");
    KijiBulkImportJobBuilder builder = KijiBulkImportJobBuilder.create()
        .withInput(new TextMapReduceJobInput(new Path("/path/to/input")))
        .withBulkImporter(KVStoreBulkImporter.class)
        .withOutput(new HFileMapReduceJobOutput(myTable, new Path("/path/to/output"), 10));

    LOG.info("Building job...");
    MapReduceJob bulkImportJob = builder.build();

    verify(kiji);
    verify(myTable);
    verify(htable);

    LOG.info("Verifying job configuration...");
    Job job = bulkImportJob.getHadoopJob();
    // Verify that everything else is what we expected as in the previous test
    // (except the bulk importer class name)...
    assertEquals(TextInputFormat.class, job.getInputFormatClass());
    assertEquals(BulkImportMapper.class, job.getMapperClass());
    assertEquals(KVStoreBulkImporter.class,
        job.getConfiguration().getClass(KijiBulkImporter.CONF_BULK_IMPORTER_CLASS, null));
    assertEquals(IdentityReducer.class, job.getReducerClass());
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
    assertEquals(TotalOrderPartitioner.class, job.getPartitionerClass());

    // KeyValueStore-specific checks here.
    Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("foostore",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }
}
