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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiPutter;

/** Context for Kiji bulk-importers or reducers to output to a Kiji table. */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface KijiTableContext extends KijiContext, KijiPutter {
  /** @return a factory to create entity IDs to write to the output Kiji table. */
  EntityIdFactory getEntityIdFactory();

  /**
   * Creates an entity ID for the specified Kiji row key.
   *
   * @param components Kiji row key components.
   * @return the entity ID for the specified Kiji row key.
   */
  EntityId getEntityId(Object... components);
}
