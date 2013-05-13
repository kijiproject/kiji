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

import scala.collection.mutable.Map

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.TableLayoutDesc

import org.kiji.schema.shell.Environment

/** Update one or more table-level properties. */
@ApiAudience.Private
final class AlterTableSetPropertyCommand(
    val env: Environment,
    val tableName: String,
    val tableProperties: Map[String, Object]) extends TableDDLCommand with TableProperties {

  override def validateArguments(): Unit = {
    checkTableExists()
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    applyTableProperties(tableProperties, layout)
  }
}
