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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;

/**
 * Convenient methods for assembling maps from names to KeyValueStore
 * implementations in getRequiredStores() calls.
 *
 * <p>Assembling maps from store names to KeyValueStore implementations
 * in the getRequiredStores() method of KijiProducer, KijiGatherer, etc.
 * can be cumbersome. This utility class contains methods that allow you
 * to assemble these in a more concise fashion.</p>
 *
 * <p>Using the RequiredStores.with(name, store) method will return a
 * java.util.Map subclass augmented with a 'with(name, store)' method
 * that can be used to chain such calls together:
 * <tt>RequiredStores.with("a", A).with("b", B)...</tt>.</p>
 *
 * {@link KeyValueStore}
 * {@link org.kiji.mapreduce.KijiProducer}
 */
@ApiAudience.Public
public final class RequiredStores {

  /** Not a stateful class; cannot be instantiated. */
  private RequiredStores() {
  }

  /**
   * @return an immutable empty mapping from names to store implementations.
   */
  public static Map<String, KeyValueStore<?, ?>> none() {
    return Collections.emptyMap();
  }

  /**
   * Create an immutable required store map of a single entry.
   *
   * @param name the name of the single KeyValueStore required.
   * @param store the default implementation of the single KeyValueStore required.
   * @param <K> the inferred KeyValueStore's key type.
   * @param <V> the inferred KeyValueStore's value type.
   * @return a mapping from that name to the store.
   */
  public static <K, V> Map<String, KeyValueStore<?, ?>> just(String name,
      KeyValueStore<K, V> store) {
    return Collections.<String, KeyValueStore<?, ?>>singletonMap(name, store);
  }


  /**
   * A Map from names to KeyValueStore entries.
   *
   * <p>Includes a with() method that allows you to daisy-chain calls
   * to add multiple stores in a concise fashion.</p>
   */
  public static final class StoreMap extends HashMap<String, KeyValueStore<?, ?>> {
    private static final long serialVersionUID = 1L;

    /** Package-private constructor. Can only be created by RequiredStores.with(..). */
    StoreMap() { }

    /**
     * Add the associated name-to-store mapping.
     *
     * @param name the name of the single KeyValueStore required.
     * @param store the default implementation of the single KeyValueStore required.
     * @return this object.
     */
    public StoreMap with(String name, KeyValueStore<?, ?> store) {
      put(name, store);
      return this;
    }
  }

  /**
   * Creates a map from names to stores with an initial entry. Returns a map that
   * can be populated with additional mappings.
   *
   * @param name the name of the single KeyValueStore required.
   * @param store the default implementation of the single KeyValueStore required.
   * @return a mapping from that name to the store.
   */
  public static StoreMap with(String name, KeyValueStore<?, ?> store) {
    return new StoreMap().with(name, store);
  }
}
