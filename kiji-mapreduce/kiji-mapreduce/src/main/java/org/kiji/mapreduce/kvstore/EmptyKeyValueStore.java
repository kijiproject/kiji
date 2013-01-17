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

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * A KeyValueStore that contains no data. Perhaps useful for testing.
 *
 * @param <K> the key type for the store.
 * @param <V> the value type for the store.
 */
@ApiAudience.Public
public class EmptyKeyValueStore<K, V> extends KeyValueStore<K, V> {
  /** The single reader instance to return for all calls to open(). */
  private final EmptyKeyValueReader mReaderInstance;

  /** Construct the definition of an empty KeyValueStore. */
  public EmptyKeyValueStore() {
    mReaderInstance = new EmptyKeyValueReader();
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

    @SuppressWarnings("unchecked")
    EmptyKeyValueStore<K, V> other = (EmptyKeyValueStore<K, V>) otherObj;
    return other == this;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return 0;
  }

  /**
   * KeyValueReader instance that returns no values from an "empty" key value store.
   * @param K the key type to accept.
   * @param V the value type to return (in practice, all values will be null).
   */
  private final class EmptyKeyValueReader extends KeyValueStoreReader<K, V> {
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
