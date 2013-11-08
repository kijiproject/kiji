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

package org.kiji.mapreduce.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.lib.InMemoryMapKeyValueStore;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class TestKeyValueStoreReaderFactory {
  final int mNumThreads = 30;
  CyclicBarrier mBarrier;
  Collection<ReaderFetcher> mReaderFetchers;
  List<Future<KeyValueStoreReader>> mKVStoreReaderFutures;
  KeyValueStoreReaderFactory mFactory;

  @Before
  public void setup() throws Exception {
    mBarrier = new CyclicBarrier(mNumThreads);
    mReaderFetchers = Lists.newArrayList();
    Map<String, KeyValueStore<?, ?>> keyValueStores = Maps.newHashMap();
    keyValueStores.put("myStore", createKVStore());
    mFactory = KeyValueStoreReaderFactory.create(keyValueStores);
  }

  @After
  public void tearDown() {
    mFactory.close();
  }

  /**
   * A test for thread safety on reads/opens in KeyValueStoreReaderFactory's openStore method.
   * A single pass doesn't guarantee thread safety, but the test will likely fail within a few
   * runs if thread safety is broken.
   */
  @Test
  public void testOpenStoreThreadSafety() throws Exception {
    for (int i = 0; i < mNumThreads; i++) {
      ReaderFetcher rf = new ReaderFetcher(mFactory);
      mReaderFetchers.add(rf);
    }

    ExecutorService pool = Executors.newFixedThreadPool(mNumThreads);
    mKVStoreReaderFutures = pool.invokeAll(mReaderFetchers);

    KeyValueStoreReader curReader, prevReader = mKVStoreReaderFutures.get(0).get();

    // All opened readers should be the same object
    for (int i = 1; i < mKVStoreReaderFutures.size(); i++) {
      curReader = mKVStoreReaderFutures.get(i).get();
      assertNotNull(curReader);
      assertEquals("Two different readers opened", prevReader, curReader);
      prevReader = curReader;
    }
  }

  // Copied from TestInMemoryMapKeyValueStore.
  private KeyValueStore<String, Integer> createKVStore() throws Exception {
    final Map<String, Integer> map = Maps.newHashMap();
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
    return outKvStore;
  }

  private class ReaderFetcher implements Callable<KeyValueStoreReader> {
    KeyValueStoreReaderFactory factory;

    ReaderFetcher(KeyValueStoreReaderFactory factory) {
      this.factory = factory;
    }

    public KeyValueStoreReader call() throws Exception {

      // Waits for all threads to hit this point before having them all openStore() at once.
      mBarrier.await();
      return factory.openStore("myStore");
    }
  }
}
