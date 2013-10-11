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

package org.kiji.scoring.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReaderBuilder;
import org.kiji.scoring.FreshKijiTableReaderBuilder.StatisticGatheringMode;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.lib.AlwaysFreshen;
import org.kiji.scoring.lib.NeverFreshen;
import org.kiji.scoring.lib.NewerThan;
import org.kiji.scoring.statistics.FreshKijiTableReaderStatistics;
import org.kiji.scoring.statistics.FreshenerStatistics;

/** Tests InternalFreshKijiTableReader. */
public class TestInternalFreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestInternalFreshKijiTableReader.class);

  private static final String TABLE_NAME = "row_data_test_table";
  private static final KijiColumnName FAMILY_QUAL0 = new KijiColumnName("family", "qual0");
  private static final KijiColumnName FAMILY_QUAL1 = new KijiColumnName("family", "qual1");
  private static final KijiColumnName FAMILY_QUAL2 = new KijiColumnName("family", "qual2");
  private static final KijiColumnName MAP_QUALIFIER = new KijiColumnName("map", "qualifier");
  private static final KijiDataRequest FAMILY_QUAL0_R = KijiDataRequest.create("family", "qual0");
  private static final KijiDataRequest FAMILY_QUAL1_R = KijiDataRequest.create("family", "qual1");
  private static final KijiDataRequest FAMILY_QUAL2_R = KijiDataRequest.create("family", "qual2");
  private static final KijiDataRequest MAP_QUALIFIER_R = KijiDataRequest.create("map", "qualifier");

  private static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();
  private static final AlwaysFreshen ALWAYS = new AlwaysFreshen();
  private static final NeverFreshen NEVER = new NeverFreshen();
  private static final ScoreFunction TEST_SCORE_FN = new TestScoreFunction();
  private static final ScoreFunction TEST_SCORE_FN2 = new TestScoreFunctionTwo();
  private static final ScoreFunction TEST_TIMEOUT_SCORE_FN = new TestTimeoutScoreFunction();

  public static final class TestScoreFunction extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return FAMILY_QUAL0_R;
    }
    public String score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      return "new-val";
    }
  }

  public static final class TestScoreFunctionTwo extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return FAMILY_QUAL0_R;
    }
    public String score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      return "two-val";
    }
  }

  public static final class TestTimeoutScoreFunction extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return FAMILY_QUAL0_R;
    }
    public String score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return "new-val";
    }
  }

  public static final class TestMapScoreFunction extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return MAP_QUALIFIER_R;
    }
    public Integer score(
        final KijiRowData dataToScore,
        final FreshenerContext context
    ) throws IOException {
      return dataToScore.<Integer>getMostRecentValue("map", "qualifier") + 1;
    }
  }

  public static final class TestNeverFreshen extends KijiFreshnessPolicy {

    @Override
    public boolean shouldUseClientDataRequest(FreshenerContext context) {
      return false;
    }

    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) {
      return FAMILY_QUAL0_R;
    }

    @Override
    public boolean isFresh(
        final KijiRowData rowData, final FreshenerContext context
    ) {
      return true;
    }
  }

  private static final class TestUsesOwnRequestPolicy extends KijiFreshnessPolicy {

    @Override
    public boolean shouldUseClientDataRequest(FreshenerContext context) {
      return false;
    }

    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) {
      return FAMILY_QUAL2_R;
    }

    @Override
    public boolean isFresh(
        final KijiRowData rowData, final FreshenerContext context
    ) {
      boolean retVal = false;
      try {
        retVal = rowData.getMostRecentValue("family", "qual2").equals("new@val.com");
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      return retVal;
    }
  }

  private static final class TestTimeoutPolicy extends KijiFreshnessPolicy {

    @Override
    public boolean isFresh(
        final KijiRowData rowData, final FreshenerContext context
    ) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return false;
    }
  }

  public static final String TEST_PARAMETER_KEY = "test.kiji.parameter.key";

  public static final class TestConfigurableScoreFunction extends ScoreFunction {

    @Override
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return FAMILY_QUAL0_R;
    }

    @Override
    public String score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      return context.getParameter(TEST_PARAMETER_KEY);
    }
  }

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupTestInternalFreshKijiTableReader() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("row_data_test_table", layout)
            .withRow("foo")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "foo-val")
                    .withQualifier("qual1").withValue(5L, "foo-val")
                    .withQualifier("qual2").withValue(5L, "foo@val.com")
                .withFamily("map")
                    .withQualifier("qualifier").withValue(5L, 1)
                    .withQualifier("qualifier1").withValue(5L, 2)
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "bar-val")
                    .withQualifier("qual2").withValue(5L, "bar@val.com")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable(TABLE_NAME);
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupTestInternalFreshKijiTableReader() throws Exception {
    mReader.close();
    mTable.release();
    mKiji.release();
  }

  @Test
  public void testMetaTable() throws Exception {
    final KijiMetaTable metaTable = mKiji.getMetaTable();
    final byte[] original = Bytes.toBytes("original");
    metaTable.putValue(TABLE_NAME, "test.kiji.metatable.key", original);

    // Ensure that the InstanceBuilder metatable behaves as expected.
    assertEquals("original",
        Bytes.toString(metaTable.getValue(TABLE_NAME, "test.kiji.metatable.key")));
  }

  @Test
  public void testGetWithNoFreshener() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    final FreshKijiTableReader reader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .build();

    // Getting a column with no Freshener attached should behave the same as a normal get.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("family", "qual0"),
        reader.get(eid, request).getMostRecentValue("family", "qual0"));
  }

  @Test
  public void testGetFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual2");

    // Create a KijiFreshnessManager and register some Fresheners.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL2,
          NEVER,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }
    // Open a new reader to pull in the new freshness policies. Allow 10 seconds so it is very
    // unlikely to timeout.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(10000)
        .build();
    try {
      // freshReader should return the same as regular reader because the data is fresh.
      assertEquals(
          mReader.get(eid, request).getMostRecentValue("family", "qual2"),
          freshReader.get(eid, request).getMostRecentValue("family", "qual2"));
      // Value should be unchanged.
      assertEquals("foo@val.com",
          mReader.get(eid, request).getMostRecentValue("family", "qual2").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetStale() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }
    // Open a new reader to pull in the new freshness policies.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(10000)
        .build();
    try {
      // freshReader should return different from regular reader because the data is stale.
      assertFalse(
          mReader.get(eid, request).getMostRecentValue("family", "qual0").equals(
          freshReader.get(eid, request).getMostRecentValue("family", "qual0")));
      // The new value should have been written.
      assertEquals(
          "new-val", mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testBulkGet() throws Exception {
    final EntityId eidFoo = mTable.getEntityId("foo");
    final EntityId eidBar = mTable.getEntityId("bar");
    final KijiDataRequest freshRequest = KijiDataRequest.create("family", "qual2");
    final KijiDataRequest staleRequest = KijiDataRequest.create("family", "qual0");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual2").add("family", "qual0");
    final KijiDataRequest completeRequest = builder.build();

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL2,
          NEVER,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }
    // Open a new reader to pull in the new Fresheners.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(10000)
        .build();
    try {
      // Get the old data for comparison
      final List<KijiRowData> oldData =
          mReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), completeRequest);

      // Run a request which should return fresh.  nothing should be written.
      final List<KijiRowData> newData =
          freshReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), freshRequest);
      assertEquals(
          oldData.get(0).getMostRecentValue("family", "qual2"),
          newData.get(0).getMostRecentValue("family", "qual2"));
      assertEquals(
          oldData.get(1).getMostRecentValue("family", "qual2"),
          newData.get(1).getMostRecentValue("family", "qual2"));

      // Run a request which should return stale.  data should be written.
      final List<KijiRowData> newData2 =
          freshReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), staleRequest);
      assertFalse(
          oldData.get(0).getMostRecentValue("family", "qual0").equals(
              newData2.get(0).getMostRecentValue("family", "qual0")));
      assertEquals("new-val", newData2.get(0).getMostRecentValue("family", "qual0").toString());
      assertFalse(
          oldData.get(1).getMostRecentValue("family", "qual0").equals(
              newData2.get(1).getMostRecentValue("family", "qual0")));
      assertEquals("new-val", newData2.get(1).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetStaleTimeout() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader reader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .build();

    // The fresh reader should return stale data after a timeout.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("family", "qual0"),
        reader.get(eid, request).getMostRecentValue("family", "qual0"));

    // Wait for the score function to finish then try again.
    Thread.sleep(1000L);
    assertEquals("new-val",
        mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
  }

  @Test
  public void testAutomaticReload() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a Freshener.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);

      FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
          .withTable(mTable)
          .withTimeout(1000)
          .withAutomaticReread(1000)
          .build();
      try {
        // Register a new Freshener
        manager.removeFreshener(TABLE_NAME, FAMILY_QUAL0);
        manager.registerFreshener(
            TABLE_NAME,
            FAMILY_QUAL0,
            new NewerThan(Long.MAX_VALUE),
            TEST_SCORE_FN2,
            EMPTY_PARAMS,
            false,
            false);

        // Assert that data is written according to the old Freshener.
        assertEquals("new-val",
            freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

        // Wait until an automatic reload has happened then assert that data is written according to
        // the new Freshener.
        Thread.sleep(1500);
        assertEquals("two-val",
            freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      } finally {
        freshReader.close();
      }
    } finally {
      manager.close();
    }
  }

  @Test
  public void testFullPool() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request0 = KijiDataRequest.create("family", "qual0");
    final KijiDataRequest request1 = KijiDataRequest.create("family", "qual1");

    // Create a KijiFreshnessManager and register some Fresheners.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL1,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }
    // Set the pool size to 2.
    FreshenerThreadPool.getInstance(2);

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(100)
        .build();
    try {
      freshReader.get(eid, request0);
      final long beforeTime = System.currentTimeMillis();
      freshReader.get(eid, request1);
      final long afterTime = System.currentTimeMillis();
      LOG.info("get request sent at: {} result received at: {} total delay: {}",
          beforeTime, afterTime, afterTime - beforeTime);
      // Assert that we return quickly.
      assertTrue(afterTime - beforeTime < 150);
      // Assert that the producer has not finished.
      assertEquals("foo-val",
          mReader.get(eid, request0).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testSpecifyTimeout() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(100)
        .build();
    try {
      // Read should return stale data.
      assertEquals("foo-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

      // Wait for the freshener to finish, assert that it wrote, then reset.
      Thread.sleep(1000);
      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      final KijiTableWriter writer = mTable.openTableWriter();
      writer.put(eid, "family", "qual0", "foo-val");
      writer.close();

      // Read should return fresh data given a longer timeout.
      assertEquals("new-val",
          freshReader.get(eid, request, 1200).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testNewCache() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);

      final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
          .withTable(mTable)
          .withTimeout(100)
          .build();

      try {
        // Read should return stale data.
        assertEquals("foo-val",
            freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

        Thread.sleep(100);

        manager.removeFreshener(TABLE_NAME, FAMILY_QUAL0);
        freshReader.rereadFreshenerRecords();

        final long startTime = System.currentTimeMillis();
        assertEquals("foo-val",
            freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
        // A read with the old Freshener should time out
        // With the new Freshener it should return immediately.
        assertTrue(System.currentTimeMillis() - startTime < 50L);

        Thread.sleep(1000);

        // The old score function should still finish.
        assertEquals("new-val",
            mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      } finally {
        freshReader.close();
      }
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRequestFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map", "qualifier");
    final KijiDataRequest mapRequest = KijiDataRequest.create("map");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          MAP_QUALIFIER,
          ALWAYS,
          new TestMapScoreFunction(),
          EMPTY_PARAMS,
          false,
          false);

      final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
          .withTable(mTable).withTimeout(1000).build();
      try {
        assertEquals(2, freshReader.get(eid, request).getMostRecentValue("map", "qualifier"));

        // Get the family and check that the Freshener ran.
        assertEquals(3, freshReader.get(eid, mapRequest).getMostRecentValue("map", "qualifier"));
      } finally {
        freshReader.close();
      }
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRepeatedUse() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map", "qualifier");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          MAP_QUALIFIER,
          ALWAYS,
          new TestMapScoreFunction(),
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(1000)
        .build();
    try {
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testReplaceRecord() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a Freshener.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          NEVER,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);

      final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
          .withTable(mTable).withTimeout(1000).build();
      try {
        freshReader.get(eid, request);
        freshReader.get(eid, request);
        freshReader.get(eid, request);

        manager.registerFreshener(
            TABLE_NAME,
            FAMILY_QUAL0,
            NEVER,
            TEST_TIMEOUT_SCORE_FN,
            EMPTY_PARAMS,
            true,
            false);
        freshReader.rereadFreshenerRecords();

        freshReader.get(eid, request);
        freshReader.get(eid, request);
        freshReader.get(eid, request);

        manager.registerFreshener(
            TABLE_NAME,
            FAMILY_QUAL0,
            NEVER,
            TEST_SCORE_FN,
            EMPTY_PARAMS,
            true,
            false);
        freshReader.rereadFreshenerRecords();

        freshReader.get(eid, request);
        freshReader.get(eid, request);
        freshReader.get(eid, request);
      } finally {
        freshReader.close();
      }
    } finally {
      manager.close();
    }
  }

  @Test
  public void testUsesOwnRequest() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          new TestUsesOwnRequestPolicy(),
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(1000)
        .build();

    try {
      assertEquals("new-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testPartialFreshening() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual1").add("family", "qual0");
    final KijiDataRequest request = builder.build();

    // Create a KijiFreshnessManager and register two Fresheners.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL1,
          ALWAYS,
          TEST_TIMEOUT_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      writer.put(eid, "family", "qual0", "foo-val");
      writer.put(eid, "family", "qual1", "foo-val");
    } finally {
      writer.close();
    }


    // Default partial freshening is false.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .build();
    try {
      assertEquals("foo-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      assertEquals("foo-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual1").toString());

      Thread.sleep(1000);

      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual1").toString());
    } finally {
      freshReader.close();
    }

    final KijiTableWriter writer2 = mTable.openTableWriter();
    try {
      writer2.put(eid, "family", "qual0", "foo-val");
      writer2.put(eid, "family", "qual1", "foo-val");
    } finally {
      writer2.close();
    }

    // Reset and try again with partial freshness allowed.
    final FreshKijiTableReader freshReader2 = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(900)
        .withPartialFreshening(true)
        .build();

    try {
      assertEquals("new-val",
          freshReader2.get(eid, request).getMostRecentValue("family", "qual0").toString());
      assertEquals("foo-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual1").toString());

      Thread.sleep(1000);

      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual1").toString());
    } finally {
      freshReader2.close();
    }
  }

  @Test
  public void testBrokenFreshnessPolicy() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          new TestTimeoutPolicy(),
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(500).withPartialFreshening(true).build();

    try {
      // Nothing should have been written because the producer was blocked behind a slow policy.
      assertEquals("foo-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

      Thread.sleep(1000);

      // The policy will finish eventually and the write will proceed.
      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testColumnsToFreshenGroupFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual1").add("family", "qual0");
    final KijiDataRequest request = builder.build();

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL1,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withColumnsToFreshen(Lists.newArrayList(FAMILY_QUAL0))
        .build();

    try {
      // Because only family:qual0 is in columnsToFreshen, only it will be refreshed.
      final KijiRowData freshData = freshReader.get(eid, request);
      assertEquals("new-val",
          freshData.getMostRecentValue("family", "qual0").toString());
      assertEquals("foo-val",
          freshData.getMostRecentValue("family", "qual1").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testColumnsToFreshenWholeGroupFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual1").add("family", "qual0");
    final KijiDataRequest request = builder.build();

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL1,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withColumnsToFreshen(Lists.newArrayList(new KijiColumnName("family")))
        .build();

    try {
      // Because the entire family is in columnsToFreshen, all qualified columns within the family
      // will be refreshed.
      final KijiRowData freshData = freshReader.get(eid, request);
      assertEquals("new-val",
          freshData.getMostRecentValue("family", "qual0").toString());
      assertEquals("new-val",
          freshData.getMostRecentValue("family", "qual1").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testChangeColumnsToFreshen() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual0").add("family", "qual1");
    final KijiDataRequest request = builder.build();

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL0,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          FAMILY_QUAL1,
          ALWAYS,
          TEST_SCORE_FN,
          EMPTY_PARAMS,
          false,
          false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withColumnsToFreshen(Lists.newArrayList(FAMILY_QUAL0))
        .build();

    try {
      // Because family:qual0 is in columnsToFreshen only it will be refreshed.
      final KijiRowData freshData = freshReader.get(eid, request);
      assertEquals("new-val",
          freshData.getMostRecentValue("family", "qual0").toString());
      assertEquals("foo-val",
          freshData.getMostRecentValue("family", "qual1").toString());

      freshReader.rereadFreshenerRecords(Lists.newArrayList(FAMILY_QUAL1));

      // Now family:qual1 should be refreshed because it is in columnsToFreshen.
      assertEquals("new-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual1").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testStatistics() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final KijiFreshenerRecord record;
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME, FAMILY_QUAL0, ALWAYS, TEST_SCORE_FN, EMPTY_PARAMS, false, false);
      record = manager.retrieveFreshenerRecord(TABLE_NAME, FAMILY_QUAL0);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withStatisticsGathering(StatisticGatheringMode.ALL, 0)
        .build();

    try {
      freshReader.get(eid, request);
      // Sleep to give the statistics gatherer time to gather.
      Thread.sleep(100);

      final FreshKijiTableReaderStatistics stats = freshReader.getStatistics();
      assertTrue(1 == stats.getRawFreshenerRunStatistics().size());
      final FreshenerStatistics freshenerStatistics =
          stats.getAggregatedFreshenerStatistics().get(record);

      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getMean());
      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getCount());
      assertTrue(0 == freshenerStatistics.getTimedOutPercent().getMean());
      assertTrue(1 == freshenerStatistics.getTimedOutPercent().getCount());
      assertTrue(600000000 > freshenerStatistics.getMeanFresheningDuration().getMean());
      assertTrue(1 == freshenerStatistics.getMeanFresheningDuration().getCount());

      freshReader.get(eid, request);
      // Sleep to give the statistics gatherer time to gather.
      Thread.sleep(100);

      assertTrue(2 == stats.getRawFreshenerRunStatistics().size());
      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getMean());
      assertTrue(2 == freshenerStatistics.getScoreFunctionRanPercent().getCount());
      assertTrue(0 == freshenerStatistics.getTimedOutPercent().getMean());
      assertTrue(2 == freshenerStatistics.getTimedOutPercent().getCount());
      assertTrue(600000000 > freshenerStatistics.getMeanFresheningDuration().getMean());
      assertTrue(2 == freshenerStatistics.getMeanFresheningDuration().getCount());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testTimedOutStatistics() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final KijiFreshenerRecord record;
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME, FAMILY_QUAL0, ALWAYS, TEST_TIMEOUT_SCORE_FN, EMPTY_PARAMS, false, false);
      record = manager.retrieveFreshenerRecord(TABLE_NAME, FAMILY_QUAL0);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withStatisticsGathering(StatisticGatheringMode.ALL, 0)
        .build();
    try {
      freshReader.get(eid, request);
      // Sleep to give the Freshener time to finish and the statistics gatherer time to gather.
      Thread.sleep(1000);

      final FreshKijiTableReaderStatistics stats = freshReader.getStatistics();
      assertTrue(1 == stats.getRawFreshenerRunStatistics().size());
      final FreshenerStatistics freshenerStatistics =
          stats.getAggregatedFreshenerStatistics().get(record);

      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getMean());
      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getCount());
      assertTrue(1 == freshenerStatistics.getTimedOutPercent().getMean());
      assertTrue(1 == freshenerStatistics.getTimedOutPercent().getCount());
      assertTrue(1300000000 > freshenerStatistics.getMeanFresheningDuration().getMean());
      assertTrue(1 == freshenerStatistics.getMeanFresheningDuration().getCount());

      freshReader.get(eid, request);
      // Sleep to give the Freshener time to finish and the statistics gatherer time to gather.
      Thread.sleep(1000);

      assertTrue(2 == stats.getRawFreshenerRunStatistics().size());
      assertTrue(1 == freshenerStatistics.getScoreFunctionRanPercent().getMean());
      assertTrue(2 == freshenerStatistics.getScoreFunctionRanPercent().getCount());
      assertTrue(1 == freshenerStatistics.getTimedOutPercent().getMean());
      assertTrue(2 == freshenerStatistics.getTimedOutPercent().getCount());
      assertTrue(1300000000 > freshenerStatistics.getMeanFresheningDuration().getMean());
      assertTrue(2 == freshenerStatistics.getMeanFresheningDuration().getCount());
    } finally {
      freshReader.close();
    }
  }

  // This is a brief test of using the new statistics gathering to do benchmarking.
  //@Test
  public void benchmark() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    try {
      manager.registerFreshener(
          TABLE_NAME, FAMILY_QUAL0, ALWAYS, TEST_SCORE_FN, EMPTY_PARAMS, false, false);
    } finally {
      manager.close();
    }

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withStatisticsGathering(StatisticGatheringMode.ALL, 0)
        .build();

    try {
      for (int x = 0; x < 1000000; x++) {
        if (x % 10000 == 0) {
          LOG.info("{} records read", x);
        }
        freshReader.get(eid, request);
      }
      LOG.info(freshReader.getStatistics().toString());
    } finally {
      freshReader.close();
    }
  }
}
