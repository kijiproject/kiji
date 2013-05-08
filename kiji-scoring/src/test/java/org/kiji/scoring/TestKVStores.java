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

package org.kiji.scoring;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.lib.AlwaysFreshen;

/**
 * Tests that KVStores are available to both producers and policies and that policies can mask the
 * kvstores of producers.
 */
public class TestKVStores extends KijiClientTest {
  /** The name of a file to back a text file key value store. */
  private static final String KV_FILENAME = "cats.txt";

  /**
   * A producer which registers an unconfigured key value store, to be replaced with a key value
   * store from the freshness policy.
   */
  private static final class UnconfiguredProducer extends KijiProducer {
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.builder().build();
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      Map<String, KeyValueStore<?, ?>> storeMap =
          new HashMap<String, KeyValueStore<?, ?>>();
      storeMap.put("cats", UnconfiguredKeyValueStore.get());
      return storeMap;
    }

    @Override
    public String getOutputColumn() {
      return "info:name";
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      KeyValueStoreReader<String, String> reader = context.getStore("cats");
      String newName = reader.get("Jennyanydots");
      reader.close();
      assertEquals("Old Gumbie Cat", newName);
      context.put(newName);
    }
  }

  /**
   * This is a simple Freshness policy designed to provide a key value store, parameterized by
   * a string path to the state.
   */
  private static final class ShadowingFreshening implements KijiFreshnessPolicy {
    private String mPathString;

    /** Empty constructor for reflection. */
    public ShadowingFreshening() { }

    public ShadowingFreshening(String path) {
      super();
      mPathString = path;
    }

    @Override
    public boolean isFresh(KijiRowData rowData, PolicyContext policyContext) {
      return false;
    }

    @Override
    public boolean shouldUseClientDataRequest() {
      return true;
    }

    @Override
    public KijiDataRequest getDataRequest() {
      // Never called so it doesn't matter.
      return null;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      Map<String, KeyValueStore<?, ?>> storeMap = new HashMap<String, KeyValueStore<?, ?>>();
      Path path = new Path(mPathString);
      storeMap.put("cats", TextFileKeyValueStore.builder().withInputPath(path).build());
      return storeMap;
    }

    @Override
    public String serialize() {
      return mPathString;
    }

    @Override
    public void deserialize(String policyState) {
      mPathString = policyState;
    }
  }

  /**
   * A producer that sets up a kvstore and then reads from it for its value.
   */
  private static final class SimpleKVProducer extends KijiProducer {
    /** A reader for our key value store. */
    private KeyValueStoreReader<String, String> mReader;

    /**
     * This is a bit of a hack: We're going to use this static member to permit proper creation
     * of the kvstore from a static context, where we don't have access to getLocalTempDir.
     * This class must be static due to reflection utils.
     *
     * This member will be set by the test so this class must NOT be used by multiple tests or
     * they will fail in a multithreaded environment.
     */
    private static Path mInputPath;

    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.builder().build();
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      Map<String, KeyValueStore<?, ?>> storeMap =
          new HashMap<String, KeyValueStore<?, ?>>();
      storeMap.put("cats", TextFileKeyValueStore.builder().withInputPath(mInputPath).build());
      return storeMap;
    }

    @Override
    public String getOutputColumn() {
      return "info:name";
    }

    @Override
    public void setup(KijiContext context) throws IOException {
      mReader = context.getStore("cats");
    }

    @Override
    public void cleanup(KijiContext context) throws IOException {
      mReader.close();
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      String newName = mReader.get("Skimbleshanks");
      assertEquals("Railway Cat", newName);
      context.put(newName);
    }
  }

  /**
   * Sets up one table to freshen and creates a file for a key value store.
   * */
  @Before
  public void setup() throws IOException {
    TableLayoutDesc userLayout = KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST);
    TableLayoutDesc tableLayout = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    new InstanceBuilder(getKiji())
        .withTable(userLayout)
            .withRow("felix")
                .withFamily("info")
                    .withQualifier("name").withValue("Felis")
        .build();
    // Set up a file for a key value store as well.
    final File file = new File(getLocalTempDir(), KV_FILENAME);
    final BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
    try {
      writer.append("Jennyanydots\tOld Gumbie Cat\n");
      writer.append("Skimbleshanks\tRailway Cat\n");
    } finally {
      writer.close();
    }
  }

  /** A test to make sure that producers run inside of freshening can access key value stores. */
  @Test
  public void testSimpleKVStore() throws IOException {
    SimpleKVProducer.mInputPath = new Path("file:" + new File(getLocalTempDir(), KV_FILENAME));
    // Install a freshness policy.
    KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    manager.storePolicy("user", "info:name", SimpleKVProducer.class, new AlwaysFreshen());
    KijiTable userTable = getKiji().openTable("user");
    FreshKijiTableReader reader =
        FreshKijiTableReaderFactory.getDefaultFactory().openReader(userTable, 10000);
    // Read from the table to ensure that the user name is updated.
    KijiRowData data =
        reader.get(userTable.getEntityId("felix"), KijiDataRequest.create("info", "name"));
    assertEquals("Railway Cat", data.getMostRecentValue("info", "name").toString());
  }

  /** A test to ensure that policies can mask the key value stores of their producers. */
  @Test
  public void testKVMasking() throws IOException {
    // Create a freshness policy that knows where to find the text file backed kv-store.
    KijiFreshnessPolicy policy =
        new ShadowingFreshening("file:" + new File(getLocalTempDir(), KV_FILENAME));
    // Install a freshness policy.
    KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    manager.storePolicy("user", "info:name", UnconfiguredProducer.class, policy);
    KijiTable userTable = getKiji().openTable("user");
    FreshKijiTableReader reader =
        FreshKijiTableReaderFactory.getDefaultFactory().openReader(userTable, 10000);
    // Read from the table to ensure that the user name is updated.
    KijiRowData data =
        reader.get(userTable.getEntityId("felix"), KijiDataRequest.create("info", "name"));
    assertEquals("Old Gumbie Cat", data.getMostRecentValue("info", "name").toString());
  }

  // TODO(SCORE-27) Add a unit test to ensure that policies can use KV stores in isFresh.
}
