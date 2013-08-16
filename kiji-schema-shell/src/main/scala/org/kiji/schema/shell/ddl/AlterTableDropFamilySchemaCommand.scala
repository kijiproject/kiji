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
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.TableLayoutDesc

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.SchemaUsageFlags

/**
 * Drop a reader or writer schema from the list of approved schemas for a map-type family.
 */
@ApiAudience.Private
final class AlterTableDropFamilySchemaCommand(
    val env: Environment,
    val tableName: String,
    val schemaFlags: SchemaUsageFlags,
    val familyName: String,
    val schema: SchemaSpec) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout, schemaFlags)

    checkColFamilyExists(layout, familyName)
    checkColFamilyIsMapType(layout, familyName)

    if (!cellSchemaContext.supportsLayoutValidation()) {
      throw new DDLException("Cannot run ALTER TABLE.. DROP SCHEMA on a table layout "
          + "that does not support schema validation.")
    }
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout, schemaFlags)
    val curFamilyDesc: FamilyDesc = getFamily(layout, familyName)
        .getOrElse(throw new DDLException("No such family: " + familyName))
    val curMapSchema: CellSchema = curFamilyDesc.getMapSchema()
    schema.dropFromCellSchema(curMapSchema, cellSchemaContext)
  }
}
