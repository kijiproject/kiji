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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * A key value store implementation that provides different mappings of keys to values for
 * different threads. This class is used to expose request parameters to an express model running
 * in the scoring server and is not intended for general use.
 *
 * <p>This class permits threads to register and deregister their own per-thread id maps using
 * calls to {@link #registerThreadLocalMap(Map<K, V>)} and {@link #unregisterThreadLocalMap()}
 * respectively.</p>
 *
 * <p>Maps are not shallow or deep copied when registered, so modifications to the map passed into
 * {@link #registerThreadLocalMap(Map<K, V>)} will be reflected in the kvstore.  A thread may
 * also register new versions of its maps as necessary to overwrite old ones. Below is an example
 * of a single thread using this class. Multiple threads could execute this code in parallel and
 * receive different values:</p>
 *
 * <pre><code>
 *   // Set up a personal map for this thread.
 *   Map&lt;String, String&gt; myMap = new HashMap&lt;String,String&gt();
 *   myMap.put("mungojerrie", "rumpelteazer");
 *
 *   KeyValueStoreReader&lt;String, String&gt; myReader = myThreadLocalMapKVStore.open();
 *   myThreadLocalMapKVStore.registerThreadLocalMap(myMap);
 *   String name = myReader.get("mungojerrie"); // Returns "rumpelteazer".
 *   myMap.put("mungojerrie", "macavity");
 *   String name2 = myReader.get("mungojerrie"); // Now returns "macavity".
 *   myThreadLocalMapKVStore.unregisterThreadLocalMap(); // Clean up after yourself.
 * </code></pre>
 *
 * <p>Note that because threads are often pooled and ids reused internally by the JVM, it is
 * important that a thread call {@link #unregisterThreadLocalMap()} to remove their bindings.</p>
 *
 * <p>Because its per-thread id mappings are only valid on the local node, this kvstore is not
 * suitable for serialization. Attempting to serialize it to a configuration will result in an
 * exception.</p>
 *
 * <p>If a map is not registered for a thread, all calls from that thread to
 * {@link ThreadLocalMapReader#get(Object)} will return null.</p>
 *
 * @param <K> The type of the keys for this kvstore.
 * @param <V> The type of the values for this kvstore.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class ThreadLocalMapKVStore<K, V> implements KeyValueStore<K, V> {
  // Must be concurrent to due to multiple thread updates.
  private final Map<Long, Map<K, V>> mThreadMap = new ConcurrentHashMap();
  private final ThreadLocalMapReader mReader = new ThreadLocalMapReader();

  /** {@inheritDoc} */
  @Override
  public void storeToConf(
      KeyValueStoreConfiguration conf
  ) throws IOException {
    throw new UnsupportedOperationException(
        "This key value store is not suitable for serialization.");
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(
      KeyValueStoreConfiguration conf
  ) throws IOException {
    throw new IllegalStateException(
        "ThreadLocalMapKVStore should not be serialized in a configuration.");
  }

  /**
   * Registers a new map for this thread, replacing any existing registered ones. This stores the
   * map itself, rather than a copy, so future mutations to the map will be reflected by the
   * kvstore reader.
   *
   * <p>Because thread ids are often reused, it's important that callers to this function also
   * call {@link #unregisterThreadLocalMap()} to clean-up.</p>
   *
   * @param map The map to store for the calling thread.
   */
  public void registerThreadLocalMap(
      Map<K, V> map
  ) {
    final Long threadId = Thread.currentThread().getId();
    mThreadMap.put(threadId, map);
  }

  /**
   * Unregisters any map for this thread previously passed into
   * {@link #registerThreadLocalMap(java.util.Map)}. If no map has been passed in, this function is
   * a noop.
   *
   * <p>Because thread ids are often reused, it's important that this method is called to cleanup.
   * </p>
   */
  public void unregisterThreadLocalMap() {
    final Long threadId = Thread.currentThread().getId();
    mThreadMap.remove(threadId);
  }

  /**
   * A simple implementation of a KeyValueStoreReader for this kvstore.  It exposes access to the
   * kvstore's internal map.
   *
   * As it's just a wrapper around the per-thread map, this class is essentially stateless and its
   * close() method is a no-op.
   */
  private final class ThreadLocalMapReader implements KeyValueStoreReader<K, V> {
    @Override
    public V get(
        K key
    ) throws IOException {
      final Long threadId = Thread.currentThread().getId();
      if (!mThreadMap.containsKey(threadId)) {
        return null;
      }

      return mThreadMap.get(threadId).get(key);
    }

    @Override
    public boolean containsKey(
        K key
    ) throws IOException {
      final Long threadId = Thread.currentThread().getId();
      return mThreadMap.containsKey(threadId) && mThreadMap.get(threadId).containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException { }
  }

  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    return mReader;
  }

}
