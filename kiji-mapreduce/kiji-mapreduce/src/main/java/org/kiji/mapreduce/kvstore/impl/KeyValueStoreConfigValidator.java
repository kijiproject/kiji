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

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;

/**
 * Merges default required store configs with per-job store configuration overrides,
 * and validates that the required stores are present.
 */
@ApiAudience.Private
public final class KeyValueStoreConfigValidator {

  /** Construct a new KeyValueStoreConfigValidator. */
  private KeyValueStoreConfigValidator() {
  }

  // Singleton instance.
  private static final KeyValueStoreConfigValidator INSTANCE = new KeyValueStoreConfigValidator();

  /**
   * This method returns a KeyValueStoreConfigValidator instance.
   *
   * @return a KeyValueStoreConfigValidator.
   */
  public static KeyValueStoreConfigValidator get() {
    return INSTANCE;
  }

  /**
   * Fills any unbound stores in <code>storeBindings</code> with the default
   * implementations found in <code>requiredStores</code>.
   *
   * <p>Each store in <code>requiredStores</code> must have either:</p>
   * <ul>
   *   <li>A default implementation, or</li>
   *   <li>An implementation defined in <code>storeBindings</code>.</li>
   * </ul>
   *
   * <p>If not, an exception is thrown.</p>
   *
   * @param requiredStores A map of required store names to their default implementations
   *     (may be null).
   * @param storeBindings A map of bound stores, which will be modified to contain default
   *     implementations from <code>requiredStores</code> if not yet bound.
   * @throws IOException If any of the required stores do not validate.
   */
  public void bindAndValidateRequiredStores(
      Map<String, KeyValueStore<?, ?>> requiredStores,
      Map<String, KeyValueStore<?, ?>> storeBindings) throws IOException {
    if (null == requiredStores) {
      // No required stores. Nothing to validate.
      return;
    }

    // Check that each required store was either bound or had a default.
    for (Map.Entry<String, KeyValueStore<?, ?>> entry : requiredStores.entrySet()) {
      final String requiredName = entry.getKey();
      final KeyValueStore<?, ?> defaultImplementation = entry.getValue();

      if (null != storeBindings.get(requiredName)) {
        // There was an implementation bound to this required store, no whammy.
        continue;
      }
      if (null != defaultImplementation) {
        // There was a default implementation for this required store, no whammy.
        storeBindings.put(requiredName, defaultImplementation);
        continue;
      }

      // Whammy! No default implementation and no user-bound implementation.
      throw new IOException("Store named " + requiredName + " is required, but not defined.");
    }
  }
}
