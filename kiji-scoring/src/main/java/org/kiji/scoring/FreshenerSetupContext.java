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
package org.kiji.scoring;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;

/**
 * Interface defining operations available to {@link KijiFreshnessPolicy} and {@link ScoreFunction}
 * setup methods. This includes all the same operations as are available to
 * {@link FreshenerGetStoresContext} plus {@link #getStore(String)} which provides access to
 * KeyValueStores.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface FreshenerSetupContext extends FreshenerGetStoresContext {

  /**
   * Opens a KeyValueStore associated with storeName for read-access.
   *
   * <p>
   *   The user does not need to call <code>close()</code> on KeyValueStoreReaders returned by this
   *   method; any open KeyValueStoreReaders will be closed automatically by the
   *   FreshKijiTableReader associated with this context.
   * </p>
   *
   * <p>
   *   Calling getStore() multiple times on the same name will reuse the same reader unless it is
   *   closed.
   * </p>
   *
   * @param <K> The key type for the KeyValueStore.
   * @param <V> The value type for the KeyValueStore.
   * @param storeName the name of the KeyValueStore to open.
   * @return A KeyValueStoreReader associated with this storeName, or null
   *     if there is no such KeyValueStore available.
   * @throws java.io.IOException if there is an error opening the underlying storage resource.
   */
  <K, V> KeyValueStoreReader<K, V> getStore(String storeName) throws IOException;

  /**
   * Get the configured CounterManager for this Context.
   *
   * Allows incrementing, retrieving, and listing counters. Some of these operations are not
   * available in all contexts.
   *
   * @return the configured CounterManager for this Context.
   */
  CounterManager getCounterManager();
}
