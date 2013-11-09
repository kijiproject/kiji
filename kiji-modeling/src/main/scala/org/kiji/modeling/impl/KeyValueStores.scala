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

package org.kiji.modeling.impl

import org.kiji.annotations.{Inheritance, ApiAudience, ApiStability}
import org.kiji.modeling.KeyValueStore

/**
 * Provides access to key-value stores used by the modeling SPI workflow phases (like Extract and
 * Score). After a concrete instance of this trait has been constructed, its keyValueStoreSpecs
 * property must be initialized.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
trait KeyValueStores {
  /**
   * Container for the key-value stores accessible to this phase of the model workflow. This
   * property must be initialized by a model job runner.
   */
  private var _keyValueStores: Option[Map[String, KeyValueStore[_, _]]] = None

  /**
   * Gets the key-value stores accessible to this phase of the model workflow. Key-value stores can
   * be addressed by their logical name.
   *
   * @return the key-value stores accessible to this phase of the model workflow.
   */
  final private[kiji] def keyValueStores: Map[String, KeyValueStore[_, _]] = {
    _keyValueStores.getOrElse {
      throw new IllegalStateException("This model phase has not been initialized properly. "
          + "Its key-value stores haven't been loaded yet.")
    }
  }

  /**
   * Gets the [[org.kiji.modeling.KeyValueStore]] associated with the specified store name.
   *
   * @param name associated with the store.
   * @tparam K is the type of the keys.
   * @tparam V is the type of the values.
   * @return the key-value store associated with the specified name.
   */
  final protected[kiji] def keyValueStore[K, V](name: String): KeyValueStore[K, V] = {
    val stores: Map[String, KeyValueStore[_, _]] = _keyValueStores.getOrElse {
      throw new IllegalStateException("This model phase has not been initialized properly. "
        + "Its key-value stores haven't been loaded yet.")
    }
    stores(name).asInstanceOf[KeyValueStore[K,V]]
  }

  /**
   * Sets the key-value stores accessible to this phase of the model workflow. This should only be
   * used by KijiExpress model job runners.
   *
   * @param value to set this phase's key-value stores to.
   */
  private[kiji] def keyValueStores_=(value: Map[String, KeyValueStore[_, _]]) {
    _keyValueStores = Some(value)
  }
}
