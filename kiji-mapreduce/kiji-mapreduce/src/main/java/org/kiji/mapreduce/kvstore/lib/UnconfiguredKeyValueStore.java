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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * A KeyValueStore that is not configured. This throws an exception when
 * calling storeToConf() to ensure that the user replaces this "default"
 * with a proper KeyValueStore on the command line.
 *
 * <p>Calls to open() will throw IllegalStateException</p>
 *
 * @param <K> the key type for the store.
 * @param <V> the value type for the store.
 */
@ApiAudience.Public
public final class UnconfiguredKeyValueStore<K, V> implements KeyValueStore<K, V> {

  /**
   * A Builder-pattern class that creates new UnconfiguredKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new UnconfiguredKeyValueStore instance.
   *
   * @param <K> The type of the key field for the store.
   * @param <V> The type of value field for the store.
   */
  @ApiAudience.Public
  public static final class Builder<K, V> {
    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
    }

    /**
     * Build a new UnconfiguredKeyValueStore instance.
     *
     * @return the initialized KeyValueStore.
     */
    public UnconfiguredKeyValueStore<K, V> build() {
      return new UnconfiguredKeyValueStore<K, V>();
    }
  }

  /**
   * Creates a new UnconfiguredKeyValueStore.Builder instance that can be used
   * to create a new KeyValueStore.
   *
   * @param <KT> The type of the key field.
   * @param <VT> The type of the value field.
   * @return a new Builder instance.
   */
  public static <KT, VT> Builder<KT, VT> builder() {
    return new Builder<KT, VT>();
  }

  /** Construct the definition of an unconfigured KeyValueStore. */
  private UnconfiguredKeyValueStore() {
  }

  /** singleton instance. */
  private static final UnconfiguredKeyValueStore<?, ?> INSTANCE =
      new UnconfiguredKeyValueStore();

  /**
   * Returns a singleton UnconfiguredKeyValueStore instance.
   *
   * @param <KT> The type of the key field.
   * @param <VT> The type of the value field.
   * @return a singleton UnconfiguredKeyValueStore instance.
   */
  @SuppressWarnings("unchecked")
  public static <KT, VT> UnconfiguredKeyValueStore<KT, VT> get() {
    return (UnconfiguredKeyValueStore<KT, VT>) INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    throw new IOException("Cannot use an UnconfiguredKeyValueStore. "
        + "You must override this on the command line or in a JobBuilder.");
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    throw new IOException("Cannot use an UnconfiguredKeyValueStore. "
        + "You must override this on the command line or in a JobBuilder.");
  }

  /**
   * Throws IllegalStateException; you cannot open an unconfigured store.
   *
   * @throws IllegalStateException because you cannot open an unconfigured store.
   * @return nothing, since it will always throw IllegalStateException.
   */
  @Override
  public KeyValueStoreReader<K, V> open() {
    throw new IllegalStateException("Cannot open an unconfigured store");
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

    // All unconfigured instances are equal.
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return 0;
  }
}
