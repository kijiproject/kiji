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

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.ComponentType

/** Represents data types for elements of formatted row key elements. */
@ApiAudience.Private
object RowKeyElemType extends Enumeration {
  type RowKeyElemType = Value
  val STRING = Value("STRING")
  val INT = Value("INT")
  val LONG = Value("LONG")

  /**
   * Return an Avro ComponentType representing the specified enum value.
   */
  def toComponentType(c: RowKeyElemType): ComponentType = {
    c match {
      case STRING => ComponentType.STRING
      case INT => ComponentType.INTEGER
      case LONG => ComponentType.LONG
    }
  }
}
