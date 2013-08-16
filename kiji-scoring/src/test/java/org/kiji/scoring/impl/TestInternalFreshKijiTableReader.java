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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
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
import org.kiji.scoring.FreshKijiTableReaderBuilder.FreshReaderType;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.impl.InternalFreshKijiTableReader.FreshnessCapsule;
import org.kiji.scoring.lib.AlwaysFreshen;
import org.kiji.scoring.lib.NeverFreshen;
import org.kiji.scoring.lib.NewerThan;
import org.kiji.scoring.lib.ShelfLife;

/**
 * Tests InternalFreshKijiTableReader.
 */
public class TestInternalFreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestInternalFreshKijiTableReader.class);

  private static String mPreloadState = "unloaded";

  private static final String TABLE_NAME = "row_data_test_table";

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  public static final class TestProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "qual0");
    }
    public String getOutputColumn() {
      return "family:qual0";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      producerContext.put("new-val");
    }
  }

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  public static final class TestProducerTwo extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "qual0");
    }
    public String getOutputColumn() {
      return "family:qual0";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      producerContext.put("two-val");
    }
  }

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  public static final class TestTimeoutProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "qual0");
    }
    public String getOutputColumn() {
      return "family:qual0";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ie) {
        throw new RuntimeException("TestProducer thread interrupted during produce sleep.");
      }
      producerContext.put("new-val");
    }
  }

  public static final class TestFamilyProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("map", "qualifier");
    }
    public String getOutputColumn() {
      return null;
    }
    public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
      context.put("qualifier", input.<Integer>getMostRecentValue("map", "qualifier") + 1);
    }
  }

  public static final class TestColumnToFamilyProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("map", "qualifier");
    }
    public String getOutputColumn() {
      return null;
    }
    public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
      context.put(input.<Integer>getMostRecentValue("map", "qualifier") + 1);
    }
  }

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  private static final class TestNeverFreshen implements KijiFreshnessPolicy {
    public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
      return true;
    }
    public boolean shouldUseClientDataRequest() {
      return false;
    }
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "qual0");
    }
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.emptyMap();
    }
    public String serialize() {
      return "";
    }
    public void deserialize(final String policyState) {}
  }

  private static final class TestUsesOwnRequestPolicy implements KijiFreshnessPolicy {
    public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
      boolean retVal = false;
      try {
        retVal = rowData.getMostRecentValue("family", "qual2").equals("new@val.com");
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      return retVal;
    }
    public boolean shouldUseClientDataRequest() {
      return false;
    }
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("family", "qual2");
    }
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.emptyMap();
    }
    public String serialize() {
      return "";
    }
    public void deserialize(final String policyState) {
      Preconditions.checkArgument(policyState.isEmpty());
    }
  }

  /** Dummy freshness policy for testing FreshKijiTableReader.preload(). */
  private static final class TestPreloadPolicy implements KijiFreshnessPolicy {

    public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
      return true;
    }
    public boolean shouldUseClientDataRequest() {
      return true;
    }
    public KijiDataRequest getDataRequest() {
      return null;
    }
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.emptyMap();
    }
    public String serialize() {
      return "";
    }
    public void deserialize(final String policyState) {
      mPreloadState = "loaded";
    }
  }

  private static final class TestTimeoutPolicy implements KijiFreshnessPolicy {
    public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return false;
    }
    public boolean shouldUseClientDataRequest() {
      return true;
    }
    public KijiDataRequest getDataRequest() {
      return null;
    }
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.emptyMap();
    }
    public String serialize() {
      return "";
    }
    public void deserialize(final String policyState) {
    }
  }

  private static final class TestQualifiedMapProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("map");
    }
    public String getOutputColumn() {
      return null;
    }
    public void produce(final KijiRowData rowData, final ProducerContext producerContext)
        throws IOException {
      producerContext.put(10);
    }
  }

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;
  private InternalFreshKijiTableReader mFreshReader;

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
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "bar-val")
                    .withQualifier("qual2").withValue(5L, "bar@val.com")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable(TABLE_NAME);
    mReader = mTable.openTableReader();
    mFreshReader = (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(1000)
        .build();
  }

  @After
  public void cleanupTestInternalFreshKijiTableReader() throws Exception {
    mFreshReader.close();
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
  public void testPolicyForName() throws Exception {
    final Class<? extends KijiFreshnessPolicy> expected = ShelfLife.class;
    assertEquals(expected, mFreshReader.policyForName("org.kiji.scoring.lib.ShelfLife").getClass());
  }

  @Test
  public void testGetPolicies() throws Exception {
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("family", "qual2").add("family", "qual0");
    final KijiDataRequest completeRequest = builder.build();

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual2", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(1000)
        .build();
    try {
      assertEquals(2, freshReader.getCapsules(completeRequest).size());
      assertEquals(AlwaysFreshen.class,
          freshReader.getCapsules(completeRequest).get(new KijiColumnName("family", "qual0"))
              .getPolicy().getClass());
      assertEquals(TestProducer.class,
          freshReader.getCapsules(completeRequest).get(new KijiColumnName("family", "qual0"))
              .getProducer().getClass());
      assertEquals(NeverFreshen.class,
          freshReader.getCapsules(completeRequest).get(new KijiColumnName("family", "qual2"))
              .getPolicy().getClass());
      assertEquals(TestProducer.class,
          freshReader.getCapsules(completeRequest).get(new KijiColumnName("family", "qual2"))
              .getProducer().getClass());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetFamilyPolicy() throws IOException {
    final KijiDataRequest request = KijiDataRequest.create("map", "name");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.removePolicies(TABLE_NAME);
    manager.storePolicy(TABLE_NAME, "map", TestFamilyProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policy.
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(100)
        .build();
    try {
      assertEquals(1, freshReader.getCapsules(request).size());
      assertEquals(NeverFreshen.class,
          freshReader.getCapsules(request).get(new KijiColumnName("map")).getPolicy().getClass());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetClientData() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("family", "qual0"),
        mFreshReader.getClientData(eid, request).get().getMostRecentValue("family", "qual0")
    );
  }

  @Test
  public void testProducerForName() throws Exception {
    final Class<? extends KijiProducer> expected = TestProducer.class;
    assertEquals(expected, mFreshReader.producerForName(
        "org.kiji.scoring.impl.TestInternalFreshKijiTableReader$TestProducer").getClass());
  }

  @Test
  public void testGetFutures() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final Map<KijiColumnName, FreshnessCapsule> capsules =
        new HashMap<KijiColumnName, FreshnessCapsule>();

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new NeverFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual2", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
            .withTable(mTable)
            .withTimeout(1000)
            .build();
    try {
      final Future<KijiRowData> clientData = freshReader.getClientData(eid, request);

      // Get an empty list of futures.
      final List<Future<Boolean>> actual =
          freshReader.getFutures(capsules, clientData, eid, request, "0");
      assertEquals(0, actual.size());

      capsules.put(new KijiColumnName("family", "qual0"),
          freshReader.makeCapsule(new KijiColumnName("family", "qual0")));
      capsules.put(new KijiColumnName("family", "qual2"),
          freshReader.makeCapsule(new KijiColumnName("family", "qual2")));

      // Get a list of two futures, both are never freshen, so should return false to indicate no
      // reread.
      final List<Future<Boolean>> actual2 =
          freshReader.getFutures(capsules, clientData, eid, request, "1");
      assertEquals(2, actual2.size());
      for (Future<Boolean> future : actual2) {
        assertEquals(false, future.get());
      }
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetWithNoPolicy() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Getting a column with no freshness policy attached should behave the same as a normal get.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("family", "qual0"),
        mFreshReader.get(eid, request).getMostRecentValue("family", "qual0"));
  }

  @Test
  public void testGetFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual2");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual2", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies. Allow 10 seconds so it is very
    // unlikely to timeout.
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
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
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual2", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
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
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual2", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
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
    manager.storePolicy(TABLE_NAME, "family:qual0", TestTimeoutProducer.class, new AlwaysFreshen());

    mFreshReader.close();
    mFreshReader = (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(500)
        .build();

    // The fresh reader should return stale data after a timeout.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("family", "qual0"),
        mFreshReader.get(eid, request).getMostRecentValue("family", "qual0"));

    // Wait for the producer to finish then try again.
    Thread.sleep(1000L);
    assertEquals("new-val",
        mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
  }

  @Test
  public void testGetFreshFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map", "qualifier");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "map", TestFamilyProducer.class, new AlwaysFreshen());

    // Open a new reader to pull in the new freshness policy.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(1000)
        .build();
    try {
      assertEquals(
          2, freshReader.get(eid, request).getMostRecentValue("map", "qualifier"));
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testGetMultipleQualifiersFromFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest firstRequest = KijiDataRequest.create("map", "qualifier0");
    final KijiDataRequest secondRequest = KijiDataRequest.create("map", "qualifier");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("map", "qualifier").add("map", "qualifier0");
    final KijiDataRequest completeRequest = builder.build();

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "map", TestFamilyProducer.class, new AlwaysFreshen());


    // Open a new reader to pull in the new freshness policy.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(1000)
        .build();

    try {
      // Check that each request freshens.
      // Get the firstRequest which will update the visits column.
      freshReader.get(eid, firstRequest);
      // Get the firstRequest and expect the visits column has been incremented twice.
      assertEquals(
          3, freshReader.get(eid, secondRequest).getMostRecentValue("map", "qualifier"));

      // Expect that the producer will only run one additional time despite two columns in the same
      // request.
      assertEquals(
          4, freshReader.get(eid, completeRequest).getMostRecentValue("map", "qualifier"));
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testReread() throws IOException {
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new ShelfLife(10L));
    manager.storePolicy(TABLE_NAME, "family:qual1", TestProducer.class, new AlwaysFreshen());
    final InternalFreshKijiTableReader freshReader =
        (InternalFreshKijiTableReader) FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(100)
        .build();
    try {
      final KijiDataRequestBuilder builder = KijiDataRequest.builder();
      builder.newColumnsDef().add("family", "qual0").add("family", "qual1");
      final KijiDataRequest request = builder.build();

      final Map<KijiColumnName, FreshnessCapsule> capsules =
          freshReader.getCapsules(request);
      assertTrue(capsules.containsKey(new KijiColumnName("family", "qual0")));
      assertTrue(capsules.containsKey(new KijiColumnName("family", "qual1")));
      manager.removePolicy(TABLE_NAME, "family:qual0");
      freshReader.rereadPolicies(false);
      final Map<KijiColumnName, FreshnessCapsule> capsules2 =
          freshReader.getCapsules(request);
      assertFalse(capsules2.containsKey(new KijiColumnName("family", "qual0")));
      assertTrue(capsules2.containsKey(new KijiColumnName("family", "qual1")));
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testAutomaticReload() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());

    FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withReaderType(FreshReaderType.LOCAL)
        .withTable(mTable)
        .withTimeout(1000)
        .withAutomaticReread(1000)
        .build();
    try {
      // Register a new freshness policy
      manager.storePolicy(
          TABLE_NAME, "family:qual0", TestProducerTwo.class, new NewerThan(Long.MAX_VALUE));

      // Assert that data is written according to the old freshness policy.
      assertEquals("new-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

      // Wait until an automatic reload has happened then assert that data is written according to
      // the new freshness policy.
      Thread.sleep(1500);
      assertEquals("two-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testFullPool() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request0 = KijiDataRequest.create("family", "qual0");
    final KijiDataRequest request1 = KijiDataRequest.create("family", "qual1");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestTimeoutProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual1", TestTimeoutProducer.class, new AlwaysFreshen());

    // Set the pool size to 2.
    FreshenerThreadPool.getInstance(2);

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(100).build();
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
    manager.storePolicy(TABLE_NAME, "family:qual0", TestTimeoutProducer.class, new AlwaysFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(100).build();
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

      // Read should return stale data given a longer timeout.
      assertEquals("new-val",
          freshReader.get(eid, request, 1200).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testPreload() throws IOException {
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new TestPreloadPolicy());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).build();
    try {
      assertEquals("unloaded", mPreloadState);
      freshReader.preload(request);
      assertEquals("loaded", mPreloadState);
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testNewCache() throws IOException, InterruptedException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    final KijiDataRequest request1 = KijiDataRequest.create("family", "qual1");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestTimeoutProducer.class, new AlwaysFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(100).build();

    try {
      // Read should return stale data.
      assertEquals("foo-val",
          freshReader.get(eid, request).getMostRecentValue("family", "qual0").toString());

      Thread.sleep(100);

      manager.removePolicy(TABLE_NAME, "family:qual0");
      freshReader.rereadPolicies(false);

      final long startTime = System.currentTimeMillis();
      assertEquals("foo-val",
          freshReader.get(eid, request1).getMostRecentValue("family", "qual1").toString());
      // A read with the old policy should time out
      // With the new policy it should return immediately.
      assertTrue(System.currentTimeMillis() - startTime < 50L);

      Thread.sleep(1000);

      // The old producer should still finish.
      assertEquals("new-val",
          mReader.get(eid, request).getMostRecentValue("family", "qual0").toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testColumnToFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map", "qualifier");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(
        TABLE_NAME, "map:qualifier", TestColumnToFamilyProducer.class, new AlwaysFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(1000).build();
    try {
      // Get map:qualifier which populates the cache and ensure the producer ran.
      assertEquals(2, freshReader.get(eid, request).getMostRecentValue("map", "qualifier"));

      manager.removePolicy(TABLE_NAME, "map:qualifier");
      manager.storePolicy(TABLE_NAME, "map", TestFamilyProducer.class, new AlwaysFreshen());
      freshReader.rereadPolicies(false);

      // Get against with the new family wide freshness policy.
      assertEquals(3, freshReader.get(eid, request).getMostRecentValue("map", "qualifier"));
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testRepeatedUse() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map", "qualifier");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(
        TABLE_NAME, "map:qualifier", TestColumnToFamilyProducer.class, new AlwaysFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(1000).build();
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

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new NeverFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(1000).build();
    try {
      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);

      manager.removePolicy(TABLE_NAME, "family:qual0");
      manager.storePolicy(
          TABLE_NAME, "family:qual0", TestTimeoutProducer.class, new NeverFreshen());
      freshReader.rereadPolicies(false);

      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);

      manager.removePolicy(TABLE_NAME, "family:qual0");
      manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new NeverFreshen());
      freshReader.rereadPolicies(false);

      freshReader.get(eid, request);
      freshReader.get(eid, request);
      freshReader.get(eid, request);
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testRequestFamily() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("map");

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(
        TABLE_NAME, "map:qualifier", TestQualifiedMapProducer.class, new AlwaysFreshen());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(1000).build();

    try {
      assertEquals(10, freshReader.get(eid, request).getMostRecentValue("map", "qualifier"));
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testUsesOwnRequest() throws IOException {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(
        TABLE_NAME, "family:qual0", TestProducer.class, new TestUsesOwnRequestPolicy());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(1000).build();

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

    // Create a KijiFreshnessManager and register a freshness policy.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy(TABLE_NAME, "family:qual1", TestTimeoutProducer.class, new AlwaysFreshen());

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      writer.put(eid, "family", "qual0", "foo-val");
      writer.put(eid, "family", "qual1", "foo-val");
    } finally {
      writer.close();
    }


    // Default partial freshening is false.
    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(500).build();
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
        .withTable(mTable).withTimeout(500).returnPartiallyFreshData(true).build();

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
    manager.storePolicy(TABLE_NAME, "family:qual0", TestProducer.class, new TestTimeoutPolicy());

    final FreshKijiTableReader freshReader = FreshKijiTableReaderBuilder.create()
        .withTable(mTable).withTimeout(500).returnPartiallyFreshData(true).build();

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
}
