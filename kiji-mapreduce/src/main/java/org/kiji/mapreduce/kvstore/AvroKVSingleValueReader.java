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
import java.util.List;

import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * Reads a file of (key, value) records into memory, indexed
 * by "key."
 *
 * <p>Lookups for a key <i>K</i> will return the "value" of the first record
 * in the file where the key field has value <i>K</i>.</p>
 *
 * @param <K> the key type expected to be implemented by the keys to this reader.
 * @param <V> the value type expected to be accessed by keys in the reader.
 */
public class AvroKVSingleValueReader<K, V> extends KeyValueStoreReader<K, V> {
  private KeyValueStoreReader<K, List<V>> mReader;
  /**
   * Constructs a key value reader over an Avro file.
   *
   * @param reader The reader used to read values from the file.
   * @throws IOException If the Avro file cannot be read, or the key field is not found.
   */
  public AvroKVSingleValueReader(KeyValueStoreReader<K, List<V>> reader)
      throws IOException {
    mReader = reader;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOpen() {
    return mReader.isOpen();
  }

  /** {@inheritDoc} */
  @Override
  public V get(K key) throws IOException, InterruptedException {
    List<V> values = mReader.get(key);
    if (null == values) {
      return null;
    }
    return values.get(0);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsKey(K key) throws IOException, InterruptedException {
    return mReader.containsKey(key);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mReader.close();
  }
}
