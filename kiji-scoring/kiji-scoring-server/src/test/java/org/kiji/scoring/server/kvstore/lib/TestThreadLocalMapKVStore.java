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

package org.kiji.scoring.server.kvstore.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiClientTest;

public class TestThreadLocalMapKVStore extends KijiClientTest {
  // A ThreadLocalMapKVStore that will be used with these tests.
  private ThreadLocalMapKVStore<String, String> mKVStore =
      new ThreadLocalMapKVStore<String, String>();

  /**
   * Tests basic functionality, similar to the javadoc for the ThreadLocalMapKVStore class.
   */
  @Test
  public void testBasicFunctionality() throws IOException {
    final KeyValueStoreReader<String, String> myReader = mKVStore.open();

    final Map<String, String> myMap = new HashMap<String, String>();
    myMap.put("mungojerrie", "rumpelteazer");

    // All values should return null before registering.
    assertNull(myReader.get("mungojerrie"));

    // Check for basic lookups.
    mKVStore.registerThreadLocalMap(myMap);
    String name = myReader.get("mungojerrie");
    assertEquals("rumpelteazer", name);
    myMap.put("mungojerrie", "macavity");
    String name2 = myReader.get("mungojerrie");
    assertEquals("macavity", name2);
    // Keys that don't exist should return null.
    assertNull(myReader.get("skimbleshanks"));
    mKVStore.unregisterThreadLocalMap(); // Clean up after yourself

    // All values should return null after unregistering.
    assertNull(myReader.get("mungojerrie"));

    myReader.close();
  }

  @Test
  public void testPerThreadViews() throws IOException, InterruptedException, ExecutionException {
    final KeyValueStoreReader<String, String> myReader = mKVStore.open();

    final ExecutorService es = Executors.newFixedThreadPool(2);
    final CountDownLatch readLatch = new CountDownLatch(2);
    final CountDownLatch cleanupLatch = new CountDownLatch(2);
    final List<Callable<String>> callables = new LinkedList();

    // Submit two threads that register and expect different values.
    callables.add(new Callable<String>() {
      public String call() {
        // Add our map and wait.
        final Map<String, String> myMap = new HashMap<String, String>();
        myMap.put("mungojerrie", "rumpelteazer");
        mKVStore.registerThreadLocalMap(myMap);
        readLatch.countDown();

        // Retrieve our value.
        String res = null;
        try {
          res = myReader.get("mungojerrie");
        } catch (IOException ioe) {
          fail("Couldn't retrieve value.");
        }
        cleanupLatch.countDown();

        // Cleanup.
        mKVStore.unregisterThreadLocalMap();
        return res;
      }
    });

    callables.add(new Callable<String>() {
      public String call() {
        // Add our map and wait.
        final Map<String, String> myMap = new HashMap<String, String>();
        myMap.put("mungojerrie", "skimbleshanks");
        mKVStore.registerThreadLocalMap(myMap);
        readLatch.countDown();

        // Retrieve our value.
        String res = null;
        try {
          res = myReader.get("mungojerrie");
        } catch (IOException ioe) {
          fail("Couldn't retrieve value.");
        }
        cleanupLatch.countDown();

        // Cleanup.
        mKVStore.unregisterThreadLocalMap();
        return res;
      }
    });

    // Invoke the futures and assert that they retrieve the correct values.
    List<Future<String>> futures = es.invokeAll(callables);
    assertEquals("rumpelteazer", futures.get(0).get());
    assertEquals("skimbleshanks", futures.get(1).get());
  }
}
