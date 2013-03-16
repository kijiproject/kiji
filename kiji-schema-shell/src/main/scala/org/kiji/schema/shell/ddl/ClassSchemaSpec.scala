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

package org.kiji.schema.shell.ddl

import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaStorage
import org.kiji.schema.avro.SchemaType

/**
 * A schema specified by the classname of an Avro specific class.
 *
 * The class name is provided as a list of string "parts" that represent package
 * and class names; these should be concatenated with the string "." interleaved
 * between parts.
 */
class ClassSchemaSpec(private val parts: List[String]) extends SchemaSpec {
  /** The fully-qualified class name that represents the schema. */
  val className = {
    val sb = new StringBuilder
    var first = true
    parts.foreach { part =>
      if (!first) {
        sb.append(".")
      }
      sb.append(part)
      first = false
    }
    sb.toString()
  }

  override def toColumnSchema(): CellSchema = {
    return CellSchema.newBuilder()
        .setType(SchemaType.CLASS)
        .setStorage(SchemaStorage.UID)
        .setValue(className)
        .build()
  }

  override def toString(): String = { className }
}
