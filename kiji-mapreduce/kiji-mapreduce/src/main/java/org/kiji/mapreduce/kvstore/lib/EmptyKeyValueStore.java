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

package org.kiji.mapreduce.kvstore.lib;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * A KeyValueStore that contains no data. Perhaps useful for testing.
 * Get an instance by calling EmptyKeyValueStore.builder().build()
 * or get a singleton instance by calling EmptyKeyValueStore.get();
 *
 * @param <K> the key type for the store.
 * @param <V> the value type for the store.
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class EmptyKeyValueStore<K, V> implements KeyValueStore<K, V> {
  /** The single reader instance to return for all calls to open(). */
  private final EmptyKeyValueReader mReaderInstance;

  /**
   * A Builder-pattern class that configures and creates new EmptyKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new EmptyKeyValueStore instance.
   *
   * @param <K> The type of the key field for the store.
   * @param <V> The type of value field for the store.
   */
  @ApiAudience.Public
  @ApiStability.Evolving
  public static final class Builder<K, V> {
    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
    }

    /**
     * Build a new EmptyKeyValueStore instance.
     *
     * @return the initialized KeyValueStore.
     */
    public EmptyKeyValueStore<K, V> build() {
      return new EmptyKeyValueStore<K, V>();
    }
  }

  /**
   * Creates a new EmptyKeyValueStore.Builder instance that can be used
   * to create a new KeyValueStore.
   *
   * @param <KT> The type of the key field.
   * @param <VT> The type of the value field.
   * @return a new Builder instance.
   */
  public static <KT, VT> Builder<KT, VT> builder() {
    return new Builder<KT, VT>();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create
   * EmptyKeyValueStore instances by using a builder or factory method.
   * Call EmptyKeyValueStore.get() to get a singleton instance.
   */
  public EmptyKeyValueStore() {
    mReaderInstance = new EmptyKeyValueReader();
  }

  /** singleton instance. */
  private static final EmptyKeyValueStore<?, ?> INSTANCE = new EmptyKeyValueStore();

  /**
   * Returns a singleton EmptyKeyValueStore instance.
   *
   * @param <KT> The type of the key field.
   * @param <VT> The type of the value field.
   * @return a singleton EmptyKeyValueStore instance.
   */
  @SuppressWarnings("unchecked")
  public static <KT, VT> EmptyKeyValueStore<KT, VT> get() {
    return (EmptyKeyValueStore<KT, VT>) INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) {
    // Do nothing; this is empty after all.
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) {
    // Nothing to do here.
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() {
    return mReaderInstance;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    } else if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    // All empty instances are equal.
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return 0;
  }

  /**
   * KeyValueReader instance that returns no values from an "empty" key value store.
   *
   * @param K the key type to accept.
   * @param V the value type to return (in practice, all values will be null).
   */
  @ApiAudience.Private
  private final class EmptyKeyValueReader implements KeyValueStoreReader<K, V> {
    /** Construct the EmptyKeyValueReader. */
    private EmptyKeyValueReader() {
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
    }

    /** {@inheritDoc} */
    @Override
    public V get(K key) {
      return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) {
      return false;
    }
  }
}
