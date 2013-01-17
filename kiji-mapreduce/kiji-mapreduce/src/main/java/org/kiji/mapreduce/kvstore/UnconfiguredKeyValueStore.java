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

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * A KeyValueStore that is not configured. This throws an exception when
 * calling storeToConf() to ensure that the user replaces this "default"
 * with a proper KeyValueStore on the command line.
 *
 * @param <K> the key type for the store.
 * @param <V> the value type for the store.
 */
@ApiAudience.Public
public class UnconfiguredKeyValueStore<K, V> extends KeyValueStore<K, V> {

  /** Construct the definition of an unconfigured KeyValueStore. */
  public UnconfiguredKeyValueStore() {
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

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() {
    return null;
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
    UnconfiguredKeyValueStore<K, V> other = (UnconfiguredKeyValueStore<K, V>) otherObj;
    return other == this;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return 0;
  }
}
