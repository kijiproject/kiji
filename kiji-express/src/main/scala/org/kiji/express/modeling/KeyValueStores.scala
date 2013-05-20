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

package org.kiji.express.modeling

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * Provides access to key-value stores used by the modeling SPI workflow phases (like Extract and
 * Score). After a concrete instance of this trait has been constructed, its kvstores property must
 * be initialized.
 */
@ApiAudience.Framework
@ApiStability.Experimental
trait KeyValueStores {
  /**
   * Container for the key-value stores accessable to this phase of the model workflow. This
   * property must be initialized by a Model Pipeline Runner.
   */
  private[this] var _kvstores: Option[Map[String, KeyValueStore[_, _]]] = None

  /**
   * Gets the key-value stores accessable to this phase of the model workflow. Key-value stores can
   * be addressed by their logical name.
   *
   * @return the key-value stores accessable to this phase of the model workflow.
   */
  final protected[express] def kvstores: Map[String, KeyValueStore[_, _]] = {
    _kvstores.getOrElse {
      throw new IllegalStateException("This model phase has not been initialized properly. "
          + "Its key-value stores haven't been loaded yet.")
    }
  }

  /**
   * Sets the key-value stores accessable to this phase of the model workflow. This should only be
   * used by KijiExpress Pipeline Runners.
   *
   * @param value to set this phase's key-value stores to.
   */
  private[express] def kvstores_=(value: Map[String, KeyValueStore[_, _]]) {
    _kvstores = Some(value)
  }
}
