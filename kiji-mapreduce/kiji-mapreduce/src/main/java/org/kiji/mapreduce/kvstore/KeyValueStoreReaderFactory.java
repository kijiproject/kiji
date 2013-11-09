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

package org.kiji.mapreduce.kvstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;

/**
 * Class that manages the creation of KeyValueStoreReaders associated
 * with a set of bound KeyValueStore name--instance pairs.
 *
 * <p>This also manages a cache of opened readers, which will be returned
 * if available, rather than creating a new store reader for a given named
 * store.</p>
 *
 * <p>The {@link #close()} method of this object will close all KeyValueStoreReaders
 * associated with it. You should call this when you are done with the
 * readers, or close them all individually.</p>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KeyValueStoreReaderFactory implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(
      KeyValueStoreReaderFactory.class.getName());

  /** The set of Key-value stores to provide to the KeyValueStoreClient. */
  private final Map<String, KeyValueStore<?, ?>> mKeyValueStores;

  /** A cache of opened key-value stores we may use and modify. */
  private final Map<String, KeyValueStoreReader<?, ?>> mKVStoreReaderCache;

  /** Creates an empty KeyValueStoreReaderFactory. */
  private KeyValueStoreReaderFactory() {
    this(Collections.<String, KeyValueStore<?, ?>>emptyMap());
  }

  /**
   * Creates a KeyValueStoreReaderFactory backed by a map of specific store bindings.
   *
   * @param storeBindings defines the set of KeyValueStores available, and the
   *     names by which they are registered.
   */
  private KeyValueStoreReaderFactory(Map<String, KeyValueStore<?, ?>> storeBindings) {
    mKeyValueStores = Collections.unmodifiableMap(
        new HashMap<String, KeyValueStore<?, ?>>(storeBindings));
    mKVStoreReaderCache = Maps.newConcurrentMap();
  }

  /**
   * Creates a KeyValueStoreReaderFactory backed by store bindings specified in a Configuration.
   *
   * @param conf the Configuration from which a set of KeyValueStore bindings should
   *     be deserialized and initialized.
   * @throws IOException if there is an error deserializing or initializing a
   *     KeyValueStore instance.
   */
  private KeyValueStoreReaderFactory(Configuration conf) throws IOException {
    Map<String, KeyValueStore<?, ?>> keyValueStores = new HashMap<String, KeyValueStore<?, ?>>();
    int numKvStores = conf.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT,
        KeyValueStoreConfigSerializer.DEFAULT_KEY_VALUE_STORE_COUNT);
    for (int i = 0; i < numKvStores; i++) {
      KeyValueStoreConfiguration kvStoreConf =
          KeyValueStoreConfiguration.createInConfiguration(conf, i);

      Class<? extends KeyValueStore> kvStoreClass = kvStoreConf
          .<KeyValueStore>getClass(KeyValueStoreConfigSerializer.CONF_CLASS,
          null, KeyValueStore.class);

      String kvStoreName = kvStoreConf.get(KeyValueStoreConfigSerializer.CONF_NAME, "");

      if (null != kvStoreClass) {
        KeyValueStore<?, ?> kvStore = ReflectionUtils.newInstance(kvStoreClass, conf);
        if (null != kvStore) {
          kvStore.initFromConf(kvStoreConf);
          if (kvStoreName.isEmpty()) {
            LOG.warn("Deserialized KeyValueStore not bound to a name; ignoring.");
            continue;
          }
          keyValueStores.put(kvStoreName, kvStore);
        }
      }
    }

    mKeyValueStores = Collections.unmodifiableMap(keyValueStores);
    mKVStoreReaderCache = Maps.newConcurrentMap();
  }

  /**
   * Creates an empty KeyValueStoreReaderFactory.
   *
   * @return a new, empty KeyValueStoreReaderFactory.
   */
  public static KeyValueStoreReaderFactory createEmpty() {
    return new KeyValueStoreReaderFactory();
  }

  /**
   * Creates a KeyValueStoreReaderFactory backed by a map of specific store bindings.
   *
   * @param storeBindings defines the set of KeyValueStores available, and the
   *     names by which they are registered.
   * @return a new KeyValueStoreReaderFactory backed by a copy of the specified storeBindings.
   */
  public static KeyValueStoreReaderFactory create(Map<String, KeyValueStore<?, ?>> storeBindings) {
    return new KeyValueStoreReaderFactory(storeBindings);
  }

  /**
   * Creates a KeyValueStoreReaderFactory backed by store bindings specified in a Configuration.
   *
   * @param conf the Configuration from which a set of KeyValueStore bindings should
   *     be deserialized and initialized.
   * @throws IOException if there is an error deserializing or initializing a
   *     KeyValueStore instance.
   * @return a new KeyValueStoreReaderFactory backed by the storeBindings specified in conf.
   */
  public static KeyValueStoreReaderFactory create(Configuration conf) throws IOException {
    return new KeyValueStoreReaderFactory(conf);
  }

  /**
   * Closes all KeyValueStoreReaders opened by this factory.
   */
  @Override
  public void close() {
    for (KeyValueStoreReader<?, ?> reader : mKVStoreReaderCache.values()) {
      if (null != reader && reader.isOpen()) {
        IOUtils.closeQuietly(reader);
      }
    }
  }

  /**
   * Opens a KeyValueStore associated with storeName for read-access.
   *
   * <p>Calling openStore() multiple times on the same name will reuse the same
   * reader unless it is closed.</p>
   *
   * <p>Note: this method is thread safe as long as readers are only being created
   * and opened - two threads requesting the same reader will receive the same one.
   * In a multi-threaded environment, a closed reader may still be returned. You
   * can prevent this by avoiding calling close() on individual readers returned
   * from this method. Instead, call close() on the KeyValueStoreReaderFactory when
   * you are done with all stores, and all readers will be closed.</p>
   *
   * @param <K> The key type for the KeyValueStore.
   * @param <V> The value type for the KeyValueStore.
   * @param storeName the name of the KeyValueStore to open.
   * @return A KeyValueStoreReader associated with this storeName, or null
   *     if there is no such KeyValueStore available.
   * @throws IOException if there is an error opening the underlying storage resource.
   */
  @SuppressWarnings("unchecked")
  public <K, V> KeyValueStoreReader<K, V> openStore(String storeName)
      throws IOException {
    KeyValueStore<K, V> store = (KeyValueStore<K, V>) mKeyValueStores.get(storeName);
    KeyValueStoreReader<?, ?> reader = mKVStoreReaderCache.get(storeName);
    if (null != store && (null == reader || !reader.isOpen())) {
      synchronized (store) {
        reader = mKVStoreReaderCache.get(storeName);
        if (null == reader || !reader.isOpen()) {
          reader = store.open();
          mKVStoreReaderCache.put(storeName, reader);
        }
      }
    }
    return (KeyValueStoreReader<K, V>) reader;
  }
}
