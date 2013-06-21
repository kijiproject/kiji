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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * KeyValueStore backed entirely by an in-memory map.
 *
 * <p>This key-value store provides an encapsulated way to pass information to
 * all tasks in a KijiMR job via the key-value store system. This can be
 * useful for things like passing contextual information to Producers that is
 * too ephemeral to store in a KijiTable or file in HDFS (time of day, for
 * example.) A typical usage case for this class is to register an
 * UnconfiguredKeyValueStore in the Producer and mask it with an instance of
 * InMemoryMapKeyValueStore registered with the job builder.</p>
 *
 * <p>Since the entire contents of the map will be stored in the job's
 * Configuration, copied to all tasks, and held in memory it is not
 * recommended that this class be used for very large amounts of data.</p>
 *
 * <p>The key and value types of this key value store must be
 * {@link java.io.Serializable}. Ideally, they should be Java primitive wrappers
 * (e.g. Integer), Strings, or other small objects.</p>
 *
 * <p>To create a InMemoryMapKeyValueStore you should use {@link #builder()} to
 * get a builder object.
 * This class has one method,
 * {@link InMemoryMapKeyValueStore.Builder#withMap(Map<K, V>)} which takes as input the
 * Map&lt;K, V&gt; to be used to back the InMemoryMapKeyValueStore.</p>
 *
 * <p>The contents of this Map are copied when
 * {@link InMemoryMapKeyValueStore.Builder#build()} is called; modifications to
 * the map between the calls to {@link Builder#withMap(Map<K, V>)} and
 * {@link InMemoryMapKeyValueStore.Builder#build()} will be reflected in the
 * KeyValueStore. Modifications made after the call to build() will not. Thus,
 * all data should be stored in the Map passed to {@link Builder#withMap(Map<K, V>)}
 * before the call to {@link InMemoryMapKeyValueStore.Builder#build()}, but it
 * can either be inserted before or between these calls.</p>
 *
 * <p>As a shortcut, you may use the static method
 * {@link InMemoryMapKeyValueStore#fromMap(Map<K, V>)} to immediately generate a key
 * value store with the contents of the Map argument. Note that the Map is copied
 * immediately and no modifications made to it after the call to get() will be
 * reflected in the InMemoryMapKeyValueStore.</p>
 *
 * <p>In either case, the copy is a <em>shallow</em> copy, i.e. references to the
 * Objects used as keys and values will be used rather than creating new
 * Objects. As a result, you should be cautious about mutating the Objects used
 * as keys or values, as their state is not locked down until the key value store is
 * serialized.</p>
 *
 * @param <K> The type of the key field. Should implement {@link java.io.Serializable}.
 * @param <V> The type of the value field. Should implement {@link java.io.Serializable}.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class InMemoryMapKeyValueStore<K, V> implements KeyValueStore<K, V> {
  private static final String CONF_MAP = "map";

  /**
   * The map written to or read from the Configuration object. This must be
   * HashMap so that it implements serializable.
   */
  private HashMap<K, V> mMap;

  /** true if the user has called open() on this object. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new InMemoryMapKeyValueStore
   * instances. Use this to specify the Map&lt;String, String&gt; for this KeyValueStore
   * and call build() to return a new instance.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  public static final class Builder<K, V> {
    private Map<K, V> mMap;

    /**
     * Private constructor. Use InMemoryMapKeyValueStore.builder() to get a builder instance.
     */
    private Builder() { }

    /**
     * Sets the map containing the keys and values. Its contents will be copied at the call
     * to build(). This is a shallow copy: Updates to the map will not be reflected after
     * build(), but mutations to keys and values will until the InMemoryMapKeyValueStore is
     * serialized into a Configuration.
     *
     * @param map the map containing the data backing this key value store.
     * @return this builder instance.
     */
    public Builder withMap(Map<K, V> map) {
      mMap = map;
      return this;
    }

    /**
     * Build a new InMemoryMapKeyValueStore instance.
     *
     * @return an initialized KeyValueStore.
     */
    public InMemoryMapKeyValueStore<K, V> build() {
      Preconditions.checkArgument(mMap != null, "Must specify a non-null map.");

      return new InMemoryMapKeyValueStore(this);
    }
  }

  /**
   * Creates a new InMemoryMapKeyValueStore.Builder instance that can be
   * used to make an InMemoryMapKeyValueStore.
   *
   * @return the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should
   * create InMemoryMapKeyValueStore instances by using a builder or
   * the factory method {@link InMemoryMapKeyValueStore#fromMap(Map<K, V>)}.
   */
  public InMemoryMapKeyValueStore() {
    mMap = new HashMap<K, V>();
  }

  /**
   * Constructor that up this KeyValueStore using a builder.
   *
   * @param builder the builder instance to read configuration from.
   */
  private InMemoryMapKeyValueStore(Builder builder) {
    mMap = new HashMap<K, V>(builder.mMap);
  }

  /**
   * Factory method that returns an InMemoryMapKeyValueStore instance.
   * This makes a shallow copy of the map. Future modifications to the map
   * passed into this call will <em>not</em> be reflected in the KeyValueStore,
   * but changes to the key or value Objects will be.
   *
   * @param <K> The type of the key field. Should implement {@link java.io.Serializable}.
   * @param <V> The type of the value field. Should implement {@link java.io.Serializable}.
   * @param map the map containing the data for the InMemoryMapKeyValueStore.
   * @return An InMemoryMapKeyValueStore instance.
   */
  public static <K, V> InMemoryMapKeyValueStore<K, V> fromMap(Map<K, V> map) {
    return builder().withMap(map).build();
  }

  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    try {
      conf.set(CONF_MAP, Base64.encodeBase64String(SerializationUtils.serialize(mMap)));
    } catch (SerializationException se) {
      // Throw a more helpful error message for the user.
      throw new SerializationException(
          "InMemoryKeyValueStore requires that its keys and values are Serializable",
          se);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    Preconditions.checkState(!mOpened, "Cannot reinitialize; already opened.");

    mMap = (HashMap<K, V>) SerializationUtils
        .deserialize(Base64.decodeBase64(conf.get(CONF_MAP)));
  }

  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    mOpened = true;
    return new Reader();
  }

  /**
   * A very simple KVStore Reader. It simply wraps access to the mMap of the
   * outer class's mMap. Because of this, its {@link #close()} and
   * {@link #isOpen()} methods are somewhat inane.
   */
  @ApiAudience.Private
  private final class Reader implements KeyValueStoreReader<K, V> {
    /** Private constructor. */
    private Reader() { }

    @Override
    public void close() throws IOException { }

    @Override
    public V get(K key) throws IOException {
      return mMap.get(key);
    }

    @Override
    public boolean containsKey(K key) throws IOException {
      return mMap.containsKey(key);
    }

    @Override
    public boolean isOpen() {
      return true;
    }
  }
}
