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
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.kiji.mapreduce.kvstore.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.mapper.ProduceMapper;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiHFileOutputFormat;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiProduceJobBuilder extends KijiClientTest {
  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temp directory to write partition files to so that they get cleaned up after test. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /** A producer to use in the test job.  It writes "stuff" to family:column. */
  public static class MyProducer extends KijiProducer {
    @Override
    public String getOutputColumn() {
      return "family:column";
    }

    @Override
    public KijiDataRequest getDataRequest() {
      KijiDataRequest dataRequest = new KijiDataRequest();
      dataRequest.addColumn(new KijiDataRequest.Column("family", "column"));
      return dataRequest;
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context)
        throws IOException {
      context.put("stuff");
    }
  }

  /** Producer that requires a KeyValueStore but doesn't specify a good default. */
  public static class UnconfiguredKVProducer extends MyProducer {
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return RequiredStores.with("foostore",
          new UnconfiguredKeyValueStore<String, Object>());
    }
  }

  @Before
  public void setupLayout() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
  }

  @Test
  public void testBuildWithHFileOutput() throws ClassNotFoundException, IOException {
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

    MapReduceJob produceJob = KijiProduceJobBuilder.create()
        .withInputTable(myTable)
        .withProducer(MyProducer.class)
        .withOutput(new HFileMapReduceJobOutput(myTable, new Path("foo/bar"), 10))
        .build();

    verify(kiji);
    verify(myTable);

    // Verify that the MR Job was configured correctly.
    Job job = produceJob.getHadoopJob();
    assertEquals(KijiTableInputFormat.class, job.getInputFormatClass());
    assertEquals(ProduceMapper.class, job.getMapperClass());
    assertEquals(MyProducer.class,
        job.getConfiguration().getClass(KijiProducer.CONF_PRODUCER_CLASS, null));
    assertEquals(10, job.getNumReduceTasks());
    assertEquals(KijiHFileOutputFormat.class, job.getOutputFormatClass());
  }

  @Test(expected=IOException.class)
  public void testUnconfiguredKeyValueStore() throws ClassNotFoundException, IOException {
    // This should explode because we don't define a KVStore for 'foostore', but
    // the class requires one.
    Kiji kiji = createMock(Kiji.class);
    KijiTable myTable = createMock(HBaseKijiTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);

    // This should throw an exception because we didn't provide a better KVStore
    // than the UnconfiguredKeyValueStore in the default.
    KijiProduceJobBuilder.create()
        .withInputTable(myTable)
        .withProducer(UnconfiguredKVProducer.class)
        .withOutput(new KijiTableMapReduceJobOutput(myTable))
        .build();
  }

  @Test
  public void testEmptyKeyValueStore() throws ClassNotFoundException, IOException {
    // We override UnconfiguredKeyValueStore with EmptyKeyValueStore; this should succeed.
    Kiji kiji = createMock(Kiji.class);
    KijiTable myTable = createMock(HBaseKijiTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);

    MapReduceJob produceJob = KijiProduceJobBuilder.create()
        .withInputTable(myTable)
        .withProducer(UnconfiguredKVProducer.class)
        .withStore("foostore", new EmptyKeyValueStore<String, Object>())
        .withOutput(new KijiTableMapReduceJobOutput(myTable))
        .build();

    verify(kiji);
    verify(myTable);

    // Verify that the MR Job was configured correctly.
    Job job = produceJob.getHadoopJob();
    Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStore.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.class"));
    assertEquals("foostore",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.name"));
  }

  @Test(expected=JobConfigurationException.class)
  public void testBuildWithDifferentTableOutput() throws ClassNotFoundException, IOException {
    Kiji kiji = createMock(Kiji.class);
    KijiTable myTable = createMock(KijiTable.class);
    KijiTable otherTable = createMock(KijiTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);
    replay(otherTable);

    // This should throw a job configuration exception because the output table does not
    // match the input table.
    KijiProduceJobBuilder.create()
        .withInputTable(myTable)
        .withProducer(MyProducer.class)
        .withOutput(new KijiTableMapReduceJobOutput(otherTable))
        .build();
  }
}
