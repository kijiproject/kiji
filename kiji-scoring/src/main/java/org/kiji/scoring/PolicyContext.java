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

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Context passed to KijiFreshnessPolicy instances to provide access to outside data.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface PolicyContext {

  /**
   * Get the KijiDataRequest which triggered this freshness check.
   *
   * @return The KijiDataRequest issued by the client for this.
   */
  KijiDataRequest getClientRequest();

  /**
   * Get the name of the column to which the freshness policy is attached.
   *
   * @return The name of the column to which the freshness policy is attached.
   */
  KijiColumnName getAttachedColumn();

  /**
   * Get the Configuration associated with the Kiji instance for this context.
   *
   * @return The Configuration associated with the Kiji instance for this context.
   */
  Configuration getConfiguration();

  /**
   * Opens a KeyValueStore associated with storeName for read-access.
   *
   * <p>The user does not need to call <code>close()</code> on KeyValueStoreReaders returned by
   * this method; any open KeyValueStoreReaders will be closed automatically by the
   * KijiProducer/Gatherer/BulkImporter associated with this Context.</p>
   *
   * <p>Calling getStore() multiple times on the same name will reuse the same
   * reader unless it is closed.</p>
   *
   * @param <K> The key type for the KeyValueStore.
   * @param <V> The value type for the KeyValueStore.
   * @param storeName the name of the KeyValueStore to open.
   * @return A KeyValueStoreReader associated with this storeName, or null
   *     if there is no such KeyValueStore available.
   * @throws IOException if there is an error opening the underlying storage resource.
   */
  <K, V> KeyValueStoreReader<K, V> getStore(String storeName) throws IOException;
}
