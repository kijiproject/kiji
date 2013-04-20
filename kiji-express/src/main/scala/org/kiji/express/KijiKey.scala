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

package org.kiji.express

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.{EntityId => JEntityId}

/**
 * A reusable container for [[org.kiji.schema.EntityId]]s.
 *
 * The MapReduce framework views a data set as a collection of key-value pairs,
 * and likes to read those pairs into a reusable instance of the key or value class. When a row
 * is read from a Kiji table, its [[org.kiji.schema.EntityId]] is emitted as the key, and
 * [[org.kiji.schema.KijiRowData]] is emitted as the value. Because instances of
 * [[org.kiji.schema.EntityId]] are not reusable, this class is provided to give the MapReduce
 * framework a reusable container.
 */
@ApiAudience.Private
@ApiStability.Experimental
final class KijiKey {
  /** The entity id contained by this instance. */
  // scalastyle:off null
  private var currentKey: JEntityId = null
  // scalastyle:on null

  /**
   *  Retrieves the [[org.kiji.schema.EntityId]] wrapped by this instance.
   *
   * @return the entity id contained in this instance.
   */
  def get(): JEntityId = currentKey

  /**
   * Sets the [[org.kiji.schema.EntityId]] contained in this instance.
   *
   * @param key that will be wrapped by this instance.
   */
  def set(key: JEntityId) {
    currentKey = key
  }
}
