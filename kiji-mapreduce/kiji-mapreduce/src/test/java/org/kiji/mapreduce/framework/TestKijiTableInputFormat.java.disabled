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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.impl.KijiTableSplit;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a producer job in-process against a fake HBase instance. */
public class TestKijiTableInputFormat extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiTableInputFormat.class);

  /** Test table, owned by this test. */
  private KijiTable mTable;

  /** Table reader, owned by this test. */
  private KijiTableReader mReader;

  @Before
  public final void setupTest() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    new InstanceBuilder(getKiji())
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
        .build();

    // Fill local variables.
    mTable = getKiji().openTable("test");
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTest() throws Exception {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testSimpleProducer() throws Exception {
    final Configuration conf = new Configuration();

    final Job job = new Job(conf);
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("info"))
        .build();
    KijiTableInputFormat.configureJob(
        job,
        mTable.getURI(),
        dataRequest,
        HBaseEntityId.fromHBaseRowKey(new byte[]{}),
        HBaseEntityId.fromHBaseRowKey(new byte[]{(byte) 0xff}),
        null
    );

    final TaskAttemptContext context =
        new TaskAttemptContextImpl(
            job.getConfiguration(),
            new TaskAttemptID("jobtracker", 1, true, 2, 3));
    final InputSplit split = new KijiTableSplit(new TableSplit());

    final KijiTableInputFormat input = new KijiTableInputFormat();
    input.setConf(job.getConfiguration());
    final RecordReader<EntityId, KijiRowData> reader = input.createRecordReader(split, context);
    reader.initialize(split, context);

    // Progress values depend on the hash-prefixes actual values.
    // A better unit-test would generate exact row keys and check progress values.

    Assert.assertEquals(0.0f, reader.getProgress());
    Assert.assertTrue(reader.nextKeyValue());
    final float progress1 = reader.getProgress();
    LOG.info("New progress after 1 row: {}", progress1);
    Assert.assertTrue(progress1 > 0.0f);
    Assert.assertTrue(progress1 < 1.0f);
    Assert.assertEquals(progress1, reader.getProgress());

    Assert.assertTrue(reader.nextKeyValue());
    final float progress2 = reader.getProgress();
    LOG.info("New progress after 2 row: {}", progress2);
    Assert.assertTrue(progress2 > progress1);
    Assert.assertTrue(progress2 < 1.0f);
    Assert.assertEquals(progress2, reader.getProgress());

    Assert.assertFalse(reader.nextKeyValue());

    reader.close();
  }
}
