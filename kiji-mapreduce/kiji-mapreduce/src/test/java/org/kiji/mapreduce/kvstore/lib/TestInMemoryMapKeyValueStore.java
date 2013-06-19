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

package org.kiji.mapreduce.kvstore.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

public class TestInMemoryMapKeyValueStore extends KijiClientTest {
  /** Number of entries in our large KV Store. */
  private static final int LARGE_KV_SIZE = 1024;

  /**
   * Producer designed to test kvstores. It doesn't actually read any data from the table.
   */
  public static class TestingProducer extends KijiProducer {

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      // We'll use UnconfiguredKeyValueStores, the real ones being
      // set by the job builder.
      return RequiredStores.with("small", UnconfiguredKeyValueStore.get())
          .with("large", UnconfiguredKeyValueStore.get());
    }

    @Override
    public KijiDataRequest getDataRequest() {
      // We won't actually use this so it's moot.
      return KijiDataRequest.create("info");
    }

    @Override
    public String getOutputColumn() {
      return "info:first_name";
    }

    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      // Ignore the input. Just retrieve our kv stores and confirm their contents.
      KeyValueStoreReader<String, String> smallStore = context.getStore("small");
      assertEquals("Small store contains incorrect.", "ipsum", smallStore.get("lorem"));
      KeyValueStoreReader<String, String> largeStore = context.getStore("large");
      for (int i = 0; i < LARGE_KV_SIZE; i++) {
        assertEquals("Large store contains incorrect value.",
            Integer.toString(i), largeStore.get(Integer.toString(i)));
      }
      smallStore.close();
      largeStore.close();
      // Write some data back to the table so we can be sure the producer ran.
      context.put("lorem");
    }
  }

  @Before
  public final void setupTestProducer() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());

    // Populate the environment. A small table with one row.
    new InstanceBuilder(getKiji())
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
                    .withQualifier("zip_code").withValue(94110)
        .build();
  }

  /**
   * A test that makes sure a producer can use this kvstore when it's
   * set-up by a job builder.
   */
  @Test
  public void testProducer() throws Exception {
    // Create some maps for KV stores for this test.
    // A small map whose value will be checked.
    final Map<String, String> smallMap = new HashMap<String, String>();
    smallMap.put("lorem", "ipsum");
    // A larger map to ensure that we can safely encode non-trivial amounts of data.
    final Map<String, String> largeMap = new HashMap<String, String>(LARGE_KV_SIZE);
    for (int i = 0; i < LARGE_KV_SIZE; i++) {
      largeMap.put(Integer.toString(i), Integer.toString(i));
    }
    final KijiURI tableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName("test").build();
    final KijiMapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(TestingProducer.class)
        .withInputTable(tableURI)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(tableURI))
        .withStore("small", InMemoryMapKeyValueStore.fromMap(smallMap))
        .withStore("large", InMemoryMapKeyValueStore.fromMap(largeMap))
        .build();
    // Be sure the job runs successfully and to completion.
    // If the producer finishes, the first_name of the main row will be changed to "lorem".
    assertTrue(job.run());
    final KijiTable table = getKiji().openTable("test");
    final KijiTableReader reader = table.openTableReader();
    String value = reader
        .get(table.getEntityId("Marsellus Wallace"), KijiDataRequest.create("info", "first_name"))
        .getMostRecentValue("info", "first_name").toString();
    assertEquals("Expected producer output not present. Did producer run successfully?",
        "lorem", value);
    reader.close();
    table.release();
  }

  @Test
  public void testNonSerializableTypes() throws Exception {
    /** A simple class which is not serializable. */
    final class NotSerialized { }
    final Map<String, NotSerialized> map = new HashMap<String, NotSerialized>();
    map.put("lorem", new NotSerialized());
    final InMemoryMapKeyValueStore<String, NotSerialized> kvStore =
        InMemoryMapKeyValueStore.fromMap(map);
    final Configuration conf = new Configuration(false);
    final KeyValueStoreConfiguration kvConf = KeyValueStoreConfiguration.fromConf(conf);
    try {
      kvStore.storeToConf(kvConf);
      fail("Should have thrown a SerializationException");
    } catch (SerializationException se) {
      assertEquals(
          "InMemoryKeyValueStore requires that its keys and values are Serializable",
          se.getMessage());
    }
  }

  @Test
  public void simpleKVStoreTest() throws Exception {
    // Make a store and serialize it.
    final Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("one", 1);
    final InMemoryMapKeyValueStore<String, Integer> kvStore =
        InMemoryMapKeyValueStore.fromMap(map);
    final Configuration conf = new Configuration(false);
    final KeyValueStoreConfiguration kvConf = KeyValueStoreConfiguration.fromConf(conf);
    kvStore.storeToConf(kvConf);

    // Deserialize the store and read the value back.
    final InMemoryMapKeyValueStore<String, Integer> outKvStore =
        new InMemoryMapKeyValueStore<String, Integer>();
    outKvStore.initFromConf(kvConf);
    final KeyValueStoreReader<String, Integer> reader = outKvStore.open();
    assertEquals("Couldn't deserialize correct value!", new Integer(1), reader.get("one"));
  }
}

