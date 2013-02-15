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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a producer job in-process against a fake HBase instance. */
public class TestProducer extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestProducer.class);

  /** Test table, owned by this test. */
  private KijiTable mTable;

  /** Table reader, owned by this test. */
  private KijiTableReader mReader;

  @Before
  public final void setupTestProducer() throws Exception {
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
  public final void teardownTestProducer() throws Exception {
    mReader.close();
    mTable.close();
  }

  /**
   * Producer intended to run on the generic KijiMR test layout. Uses resource
   * org/kiji/mapreduce/layout/test.json.
   */
  public static class SimpleProducer extends KijiProducer {

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "map_family";
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      final String userId = Bytes.toString((byte[]) input.getEntityId().getComponentByIndex(0));
      final String firstName = input.getMostRecentValue("info", "first_name").toString();
      context.put("produced qualifier",
          String.format("produced content for row '%s': %s", userId, firstName));
    }
  }

  @Test
  public void testSimpleProducer() throws Exception {
    // Run producer:
    final MapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(SimpleProducer.class)
        .withInputTable(mTable.getURI())
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate produced output:
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info").addFamily("map_family");
    final KijiRowScanner scanner = mReader.getScanner(builder.build());
    for (KijiRowData row : scanner) {
      final EntityId eid = row.getEntityId();
      final String userId = Bytes.toString((byte[]) eid.getComponentByIndex(0));
      LOG.info("Row: {}", userId);
      assertEquals(userId, String.format("%s %s",
          row.getMostRecentValue("info", "first_name"),
          row.getMostRecentValue("info", "last_name")));
      assertEquals(1, row.getMostRecentValues("map_family").size());
      final Map.Entry<String, Utf8> entry =
          row.<Utf8>getMostRecentValues("map_family").entrySet().iterator().next();
      assertEquals("produced qualifier", entry.getKey().toString());
      assertTrue(entry.getValue().toString()
          .startsWith(String.format("produced content for row '%s': ", userId)));
    }
    scanner.close();
  }

  /** Producer to test the setup/produce/cleanup workflow. */
  public static class ProducerWorkflow extends KijiProducer {
    private boolean mSetupFlag = false;
    private boolean mCleanupFlag = false;
    private int mProduceCounter = 0;

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "primitives:string";
    }

    /** {@inheritDoc} */
    @Override
    public void setup(KijiContext context) throws IOException {
      super.setup(context);
      assertFalse(mSetupFlag);
      assertEquals(0, mProduceCounter);
      mSetupFlag = true;
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      mProduceCounter += 1;
      final String rowKey = Bytes.toString((byte[]) input.getEntityId().getComponentByIndex(0));
      context.put(rowKey);
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup(KijiContext context) throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      assertEquals(2, mProduceCounter);  // testProducerWorkflow sets up exactly 2 rows
      mCleanupFlag = true;
      super.cleanup(context);
    }
  }

  /** Tests the producer workflow (setup/produce/cleanup) and counters. */
  @Test
  public void testProducerWorkflow() throws Exception {
    // Run producer:
    final MapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(ProducerWorkflow.class)
        .withInputTable(mTable.getURI())
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate produced output:
    final KijiRowScanner scanner = mReader.getScanner(
        KijiDataRequest.create("primitives", "string"));
    final Set<String> produced = Sets.newHashSet();
    for (KijiRowData row : scanner) {
      produced.add(row.getMostRecentValue("primitives", "string").toString());
    }
    scanner.close();

    assertTrue(produced.contains("Marsellus Wallace"));
    assertTrue(produced.contains("Vincent Vega"));

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(2, counters.findCounter(JobHistoryCounters.PRODUCER_ROWS_PROCESSED).getValue());
  }

  // TODO(KIJI-359): Missing tests :
  //  - Outputting to wrong column qualifier
  //  - producing an HFile and bulk-loading
  //  - multi-threaded producer/mapper
  //  - key/value stores
}
