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

import scala.collection.JavaConversions._

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.TableLayoutDesc

import org.kiji.schema.shell.AddToAllSchemaUsageFlags
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

/**
 * Set the column to use a particular schema definition.
 *
 * This is deprecated in layout-1.3; you should use ALTER TABLE.. ADD SCHEMA .. TO COLUMN
 * (AlterTableAddColumnSchemaCommand) to add a new reader/writer schema. On validating layouts,
 * this command will function like "ADD SCHEMA", adding a new approved reader/writer/default
 * schema, but not evicting old ones.
 */
@ApiAudience.Private
final class AlterTableSetColumnSchemaCommand(
    val env: Environment,
    val tableName: String,
    val columnName: ColumnName,
    val schema: SchemaSpec) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    checkColumnExists(layout, columnName.family, columnName.qualifier)
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout,
        AddToAllSchemaUsageFlags)
    if (cellSchemaContext.supportsLayoutValidation) {
      echo("Deprecation warning: ALTER TABLE.. SET SCHEMA.. FOR COLUMN is deprecated.")
      echo("New syntax: ALTER TABLE.. ADD SCHEMA.. FOR COLUMN")

      // This class is deprecated in layout-1.3; run the AddSchema command's logic.
      new AlterTableAddColumnSchemaCommand(env, tableName, AddToAllSchemaUsageFlags, columnName,
          schema).updateLayout(layout)
    } else {
      // layout-1.2 or below: replace the CellSchema for the column.
      getColumn(layout, columnName)
          .getOrElse(throw new DDLException("No such column: " + columnName))
          .setColumnSchema(schema.toNewCellSchema(cellSchemaContext))
    }
  }
}
