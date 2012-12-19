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
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestKijiGatherJobBuilder extends KijiClientTest {

  /** A gatherer to use in the test job. */
  public static class MyGatherer extends KijiGatherer<Text, Text> {
    @Override
    public KijiDataRequest getDataRequest() {
      KijiDataRequest dataRequest = new KijiDataRequest();
      dataRequest.addColumn(new KijiDataRequest.Column("family", "column"));
      return dataRequest;
    }

    @Override
    public void gather(KijiRowData input, MapReduceContext<Text, Text> context)
        throws IOException {
      context.write(new Text("family"), new Text("column"));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  /** A gatherer that requires a KV store. */
  public static class UnconfiguredKVGatherer extends MyGatherer {
    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.<String, KeyValueStore<?, ?>>singletonMap("foostore",
          new UnconfiguredKeyValueStore<String, Object>());
    }
  }

  /** A combiner to use in the test job. */
  public static class MyCombiner extends KijiBaseReducer<Text, Text, Text, Text> {
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  /** A reducer to use in the test job. */
  public static class MyReducer extends KijiBaseReducer<Text, Text, Text, Text> {
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  @Before
  public void setupLayout() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
  }

  @Test
  public void testBuildValid() throws IOException {
    Kiji kiji = createMock(Kiji.class);
    KijiTable myTable = createMock(HBaseKijiTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    Configuration conf = new Configuration();

    // Set expected method calls.
    expect(kiji.getConf()).andReturn(conf).anyTimes();
    expect(kiji.getName()).andReturn("mykiji").anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);

    MapReduceJob gatherJob = new KijiGatherJobBuilder()
        .withInputTable(myTable)
        .withGatherer(MyGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
        .build();

    verify(kiji);
    verify(myTable);

    // TODO: Verify that the MR Job was configured correctly.
    gatherJob.getHadoopJob();
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
    expect(kiji.getName()).andReturn("mykiji").anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);

    new KijiGatherJobBuilder()
        .withInputTable(myTable)
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
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
    expect(kiji.getName()).andReturn("mykiji").anyTimes();
    expect(myTable.getKiji()).andReturn(kiji).anyTimes();
    expect(myTable.getName()).andReturn("table").anyTimes();
    expect(myTable.getLayout()).andReturn(tableLayout).anyTimes();

    replay(kiji);
    replay(myTable);

    MapReduceJob gatherJob = new KijiGatherJobBuilder()
        .withInputTable(myTable)
        .withGatherer(UnconfiguredKVGatherer.class)
        .withCombiner(MyCombiner.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("mypath"), 10))
        .withStore("foostore", new EmptyKeyValueStore<String, Object>())
        .build();

    verify(kiji);
    verify(myTable);

    // Verify that the MR Job was configured correctly.
    Job job = gatherJob.getHadoopJob();
    Configuration confOut = job.getConfiguration();
    assertEquals(1, confOut.getInt(KeyValueStore.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.class"));
    assertEquals("foostore",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.name"));
  }
}
