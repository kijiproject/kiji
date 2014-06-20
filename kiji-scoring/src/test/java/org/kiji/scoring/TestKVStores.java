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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.avro.ParameterDescription;
import org.kiji.scoring.lib.AlwaysFreshen;

/**
 * Tests that KVStores are available to both score functions and policies and that policies can mask
 * the kvstores of score functions.
 */
public class TestKVStores extends KijiClientTest {
  /** The name of a file to back a text file key value store. */
  private static final String KV_FILENAME = "cats.txt";

  /**
   * A producer which registers an unconfigured key value store, to be replaced with a key value
   * store from the freshness policy.
   */
  private static final class UnconfiguredScoreFunction extends ScoreFunction {
    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) {
      return KijiDataRequest.builder().build();
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
      Map<String, KeyValueStore<?, ?>> storeMap =
          new HashMap<String, KeyValueStore<?, ?>>();
      storeMap.put("cats", UnconfiguredKeyValueStore.get());
      return storeMap;
    }

    @Override
    public TimestampedValue<String> score(
        KijiRowData input,
        FreshenerContext context
    ) throws IOException {
      KeyValueStoreReader<String, String> reader = context.getStore("cats");
      String newName = reader.get("Jennyanydots");
      assertEquals("Old Gumbie Cat", newName);
      return TimestampedValue.create(newName);
    }
  }

  /**
   * This is a simple Freshness policy designed to provide a key value store, parameterized by
   * a string path to the state.
   */
  private static final class ShadowingFreshening extends KijiFreshnessPolicy {
    public static final String PARAMETER_KEY =
        "org.kiji.scoring.TestKVStores$ShadowingFreshening.path_string";

    private String mPathString;

    /** Empty constructor for reflection. */
    public ShadowingFreshening() { }

    public ShadowingFreshening(String path) {
      super();
      mPathString = path;
    }

    @Override
    public Map<String, String> serializeToParameters() {
      final Map<String, String> serialized = Maps.newHashMap();
      serialized.put(PARAMETER_KEY, mPathString);
      return serialized;
    }

    @Override
    public boolean isFresh(KijiRowData rowData, FreshenerContext context) {
      return STALE;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
      Map<String, KeyValueStore<?, ?>> storeMap = new HashMap<String, KeyValueStore<?, ?>>();
      Path path = new Path(context.getParameter(PARAMETER_KEY));
      storeMap.put("cats", TextFileKeyValueStore.builder().withInputPath(path).build());
      return storeMap;
    }

    @Override
    public void setup(FreshenerSetupContext context) {
      mPathString = context.getParameter(PARAMETER_KEY);
    }
  }

  /**
   * This is a simple Freshness policy designed to provide a key value store, parameterized by
   * a string path to the state.
   */
  private static final class KVStoreInIsFreshPolicy extends KijiFreshnessPolicy {
    public static final String PARAMETER_KEY =
        "org.kiji.scoring.TestKVStores$KVStoreInIsFreshPolicy.path_string";

    private String mPathString;

    /** Empty constructor for reflection. */
    public KVStoreInIsFreshPolicy() { }

    public KVStoreInIsFreshPolicy(String path) {
      super();
      mPathString = path;
    }

    @Override
    public Map<String, String> serializeToParameters() {
      final Map<String, String> serialized = Maps.newHashMap();
      serialized.put(PARAMETER_KEY, mPathString);
      return serialized;
    }

    @Override
    public boolean isFresh(KijiRowData rowData, FreshenerContext context) {
      final KeyValueStoreReader<String, String> cats;
      try {
        cats = context.getStore("cats");
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
      if (cats != null) {
        try {
          return cats.get("Skimbleshanks").equals("Railway Cat");
        } catch (IOException ioe) {
          throw new KijiIOException(ioe);
        }
      } else {
        throw new RuntimeException("Could not retrieve KVStoreReader \"cats\" test is broken.");
      }
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
      Map<String, KeyValueStore<?, ?>> storeMap = new HashMap<String, KeyValueStore<?, ?>>();
      Path path = new Path(context.getParameter(PARAMETER_KEY));
      storeMap.put("cats", TextFileKeyValueStore.builder().withInputPath(path).build());
      return storeMap;
    }

    @Override
    public void setup(FreshenerSetupContext context) {
      mPathString = context.getParameter(PARAMETER_KEY);
    }
  }

  private static final class SimpleKVScoreFunction extends ScoreFunction {

    private static final String PARAMETER_KEY =
        "org.kiji.scoring.TestKVStores$SimpleKVScoreFunction.input_path";
    private KeyValueStoreReader<String, String> mReader;

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores(FreshenerGetStoresContext context) {
      Map<String, KeyValueStore<?, ?>> storeMap =
          new HashMap<String, KeyValueStore<?, ?>>();
      storeMap.put("cats", TextFileKeyValueStore.builder().withInputPath(
          new Path(context.getParameter(PARAMETER_KEY))).build());
      return storeMap;
    }

    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) {
      return KijiDataRequest.builder().build();
    }

    @Override
    public void setup(FreshenerSetupContext context) throws IOException {
      mReader = context.getStore("cats");
    }

    @Override
    public TimestampedValue<String> score(
        KijiRowData input,
        FreshenerContext context
    ) throws IOException {
      final String newName = mReader.get("Skimbleshanks");
      assertEquals("Railway Cat", newName);
      return TimestampedValue.create(newName);
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
    final String path = new Path("file:" + new File(getLocalTempDir(), KV_FILENAME)).toString();
    final Map<String, String> params = Maps.newHashMap();
    params.put(SimpleKVScoreFunction.PARAMETER_KEY, path);
    final Map<String, ParameterDescription> emptyDescriptions = Collections.emptyMap();

    // Install a freshness policy.
    KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          "user",
          new KijiColumnName("info", "name"),
          AlwaysFreshen.class.getName(),
          SimpleKVScoreFunction.class.getName(),
          params,
          emptyDescriptions,
          true,
          false,
          false);
    } finally {
      manager.close();
    }
    final KijiTable userTable = getKiji().openTable("user");
    try {
      final FreshKijiTableReader reader = FreshKijiTableReader.Builder.create()
          .withTable(userTable)
          .withTimeout(10000)
          .build();
      try {
        // Read from the table to ensure that the user name is updated.
        KijiRowData data =
            reader.get(userTable.getEntityId("felix"), KijiDataRequest.create("info", "name"));
        assertEquals("Railway Cat", data.getMostRecentValue("info", "name").toString());
      } finally {
        reader.close();
      }
    } finally {
      userTable.release();
    }
  }

  /** A test to ensure that policies can mask the key value stores of their producers. */
  @Test
  public void testKVMasking() throws IOException {
    // Create a freshness policy that knows where to find the text file backed kv-store.
    KijiFreshnessPolicy policy =
        new ShadowingFreshening("file:" + new File(getLocalTempDir(), KV_FILENAME));
    // Install a freshness policy.
    KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          "user",
          new KijiColumnName("info", "name"),
          policy,
          new UnconfiguredScoreFunction(),
          Collections.<String, String>emptyMap(),
          Collections.<String, ParameterDescription>emptyMap(),
          true,
          false);
    } finally {
      manager.close();
    }
    final KijiTable userTable = getKiji().openTable("user");
    try {
      final FreshKijiTableReader reader = FreshKijiTableReader.Builder.create()
          .withTable(userTable)
          .withTimeout(10000)
          .build();
      try {
        // Read from the table to ensure that the user name is updated.
        KijiRowData data =
            reader.get(userTable.getEntityId("felix"), KijiDataRequest.create("info", "name"));
        assertEquals("Old Gumbie Cat", data.getMostRecentValue("info", "name").toString());
      } finally {
        reader.close();
      }
    } finally {
      userTable.release();
    }
  }

  @Test
  public void testKVStoreInIsFresh() throws IOException {
    // Create a freshness policy that knows where to find the text file backed kv-store.
    KijiFreshnessPolicy policy =
        new KVStoreInIsFreshPolicy("file:" + new File(getLocalTempDir(), KV_FILENAME));
    // Install a freshness policy.
    KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          "user",
          new KijiColumnName("info", "name"),
          policy,
          new UnconfiguredScoreFunction(),
          Collections.<String, String>emptyMap(),
          Collections.<String, ParameterDescription>emptyMap(),
          true,
          false);
    } finally {
      manager.close();
    }
    KijiTable userTable = null;
    FreshKijiTableReader freshReader = null;
    try {
      userTable = getKiji().openTable("user");
      freshReader = FreshKijiTableReader.Builder.create()
          .withTable(userTable)
          .withTimeout(10000)
          .build();

      // Read from the table to ensure that the user name is updated.
      KijiRowData data =
          freshReader.get(userTable.getEntityId("felix"), KijiDataRequest.create("info", "name"));
      // IsFresh should have returned true, so nothing should be written.
      assertEquals("Felis", data.getMostRecentValue("info", "name").toString());
    } finally {
      ResourceUtils.closeOrLog(freshReader);
      ResourceUtils.releaseOrLog(userTable);
    }
  }
}
