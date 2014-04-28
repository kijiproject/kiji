/**
 * (c) Copyright 2014 WibiData, Inc.
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

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro._

import org.kiji.schema.shell.{EmptySchemaUsageFlags, DDLException, Environment}

/**
 * Set the column to use a particular schema validation mode.
 */
@ApiAudience.Private
final class AlterTableSetValidationModeCommand(
  val env: Environment,
  val tableName: String,
  val validationMode: AvroValidationPolicy,
  val target: Either[ColumnName, String]) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    val cellSchemaContext: CellSchemaContext =
      CellSchemaContext.create(env, layout, EmptySchemaUsageFlags)

    def validateColumn(columnName: ColumnName): Unit = {
      checkColumnExists(layout, columnName.family, columnName.qualifier)
    }

    def validateFamily(familyName: String): Unit = {
      checkColFamilyExists(layout, familyName)
    }

    // Execute the appropriate validation function
    target.fold(validateColumn, validateFamily)

    if (!cellSchemaContext.supportsLayoutValidation()) {
      throw new DDLException("Cannot run ALTER TABLE .. SET VALIDATION on a table layout "
          + "that does not support schema validation.")
    }
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {

    def applyValidationMode(columnDesc: ColumnDesc): Unit = {
      val curCellSchema: CellSchema = columnDesc.getColumnSchema()
      curCellSchema.setAvroValidationPolicy(validationMode)
    }

    def updateColumn(columnName: ColumnName): Unit = {
      val curColumnDesc: ColumnDesc = getColumn(layout, columnName)
          .getOrElse(throw new DDLException("No such column: " + columnName))

      applyValidationMode(curColumnDesc)
    }

    def updateFamily(familyName: String): Unit = {
      val curFamilyDesc: FamilyDesc = getFamily(layout, familyName)
          .getOrElse(throw new DDLException("No such column family: " + familyName))

      // Set validation mode if map type family.
      val mapSchema: Option[CellSchema] = Option(curFamilyDesc.getMapSchema())
      mapSchema.map { _.setAvroValidationPolicy(validationMode) }

      // Otherwise set validation mode for all group type columns in the family.
      curFamilyDesc.getColumns().iterator().asScala.foreach(applyValidationMode)
    }

    // Apply the requested updates to the table layout
    target.fold(updateColumn, updateFamily)
  }
}
