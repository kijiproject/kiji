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

package org.kiji.mapreduce;

import java.io.Closeable;
import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/**
 * Allows users to read from a KeyValueStore opened in a producer.
 *
 * <p>By default, this data is presented to you as a read-only, non-iterable map.
 * Only get() requests for an explicit key are supported by default, though some
 * implementations may offer iteration.
 * </p>
 *
 * <p>An opened KeyValueStore may contain state or connect to external resources;
 * you should call the close() method when you are finished using this KeyValueStoreReader.
 * </p>
 *
 * <p>KeyValueStoreReader implementations may throw IOException or InterruptedException
 * from their constructor or other initialization routines called by KeyValueStore.open()
 * when attempting to connect to underlying resources.</p>
 *
 * @param <K> the type associated with keys in this store.
 * @param <V> the type associated with values in this store.
 */
@ApiAudience.Public
public abstract class KeyValueStoreReader<K, V> implements Closeable {

  /**
   * Looks up the specified key in the KeyValueStore and returns the associated
   * value if available.
   *
   * @param key the non-null key that defines an entity in the KeyValueStore to
   *     retrieve.
   * @return the value associated with 'key', or null if no such value is available.
   * @throws IOException if there is an IO error communicating with the underlying
   *     storage medium for the KeyValueStore.
   * @throws InterruptedException if accessing the underlying storage medium was
   *     interrupted.
   */
  public abstract V get(K key) throws IOException, InterruptedException;

  /**
   * Determines if the specified key exists in the KeyValueStore.
   *
   * @param key the non-null key that may define an entity in the KeyValueStore.
   * @return true if the key is present in the KeyValueStore.
   * @throws IOException if there is an IO error communicating with the underlying
   *     storage medium for the KeyValueStore.
   * @throws InterruptedException if accessing the underlying storage medium was
   *     interrupted.
   */
  public abstract boolean containsKey(K key) throws IOException, InterruptedException;

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }

  /** @return true if the reader is open; false if close() has already been called. */
  public abstract boolean isOpen();

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
}
