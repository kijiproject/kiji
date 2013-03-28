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

package org.kiji.chopsticks

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.EntityId

/**
 * A reusable container for Kiji entity ids.
 *
 * The MapReduce framework views a data set as a collection of key-value pairs,
 * and likes to read those pairs into a reusable instance of the key or value class. When a row
 * is read from a Kiji table, its entity id (specifically class `EntityId`) is used as the key.
 * Because instances of `EntityId` are not reusable, this simple class is provided to give the
 * MapReduce framework a reusable container.
 */
@ApiAudience.Private
@ApiStability.Unstable
final class KijiKey {
  /** The entity id contained by this instance. */
  private var currentKey: EntityId = null

  /**
   * @return the entity id contained in this instance.
   */
  def get(): EntityId = currentKey

  /**
   * Sets the entity id contained in this instance.
   *
   * @param key that this container will hold.
   */
  def set(key: EntityId) {
    currentKey = key
  }
}
