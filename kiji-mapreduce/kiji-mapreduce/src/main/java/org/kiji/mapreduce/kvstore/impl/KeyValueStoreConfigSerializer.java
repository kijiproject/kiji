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

package org.kiji.mapreduce.kvstore.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * Serialize multiple KeyValueStoreConfigurations into a Hadoop Configuration
 * or deserialize the reverse.
 */
@ApiAudience.Private
public final class KeyValueStoreConfigSerializer {

  /** KeyValueStoreConfiguration variable storing the class name to deserialize the store. */
  public static final String CONF_CLASS = "kiji$kvstore$class";

  /** KeyValueStoreConfiguration variable storing the name of this KeyValueStore. */
  public static final String CONF_NAME = "kiji$kvstore$name";

  /** How many KeyValueStore definitions were serialized in the configuration? */
  public static final String CONF_KEY_VALUE_STORE_COUNT = "kiji.job.kvstores.count";

  /** Default value for CONF_KEY_VALUE_STORE_COUNT. */
  public static final int DEFAULT_KEY_VALUE_STORE_COUNT = 0;

  /** Construct a new KeyValueStoreConfigSerializer. */
  private KeyValueStoreConfigSerializer() {
  }

  // Singleton instance.
  private static final KeyValueStoreConfigSerializer INSTANCE = new KeyValueStoreConfigSerializer();

  /**
   * This method returns a KeyValueStoreConfigSerializer instance.
   *
   * @return a KeyValueStoreConfigSerializer.
   */
  public static KeyValueStoreConfigSerializer get() {
    return INSTANCE;
  }

  /**
   * Given a set of KeyValueStores, add their configuration data to the provided Configuration.
   * This replaces any previous KeyValueStore configs in the Configuration.
   *
   * @param stores the map from names to KeyValueStores to serialize.
   * @param conf the Configuration to hold the stores.
   * @throws IOException if there is an error serializing the configuration of a store.
   */
  public void addStoreMapToConfiguration(Map<String, KeyValueStore<?, ?>> stores,
      Configuration conf) throws IOException {
    int i = 0;
    for (Map.Entry<String, KeyValueStore<?, ?>> entry : stores.entrySet()) {
      String storeName = entry.getKey();
      KeyValueStore<?, ?> store = entry.getValue();

      if (null != store) {
        KeyValueStoreConfiguration kvStoreConf =
            KeyValueStoreConfiguration.createInConfiguration(conf, i);

        // Set the default deserializer class to the same one that serialized it.
        // This is done first, so the store itself can override it.
        kvStoreConf.set(CONF_CLASS, store.getClass().getName());

        // Serialize the class' inner data itself.
        store.storeToConf(kvStoreConf);

        // Then store the name binding associated with this KeyValueStore.
        kvStoreConf.set(CONF_NAME, storeName);

        i++;
      }
    }

    // Record the number of KeyValueStore instances we wrote to the conf, so we can
    // deserialize the same number afterward.
    conf.setInt(CONF_KEY_VALUE_STORE_COUNT, i);
  }
}
