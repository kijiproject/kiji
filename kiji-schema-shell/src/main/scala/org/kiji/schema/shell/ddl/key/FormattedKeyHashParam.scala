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

package org.kiji.schema.shell.ddl.key

import scala.collection.mutable.Set

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.RowKeyFormat2

/**
 * An argument to a KeyHashParams instance, as part of a list. Subclasses
 * are all interpreted in KeyHashParams.
 */
@ApiAudience.Private
abstract class FormattedKeyHashParam {
  /**
   * Validates that the key hash parameter is valid.
   *
   * @param names the names of the declared key components.
   * @throws DDLException if there's an invalid component.
   */
  def validate(names: Set[String]): Unit

  /**
   * Updates the hashing properties of the specified RowKeyFormat2 object through
   * the builder being used to construct it.
   *
   * @param format A builder for a RowKeyFormat2 to populate with updated hashing
   *   properties.
   * @return the same builder.
   */
  def updateHashProperties(format: RowKeyFormat2.Builder): RowKeyFormat2.Builder
}
