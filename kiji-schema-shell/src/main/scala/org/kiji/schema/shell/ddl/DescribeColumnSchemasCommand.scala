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

import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser

import org.apache.avro.Schema

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro._

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.SchemaUsageFlags

/**
 * Pretty-print the set of schemas associated with a column in a table.
 */
@ApiAudience.Private
final class DescribeColumnSchemasCommand(
    val env: Environment,
    val tableName: String,
    val columnName: ColumnName,
    val numSchemas: Int,
    val schemaUsageFlags: SchemaUsageFlags) extends TableDDLCommand {

  private val gson: Gson = new GsonBuilder().setPrettyPrinting().create()
  private val jsonParser = new JsonParser()

  override def validateArguments(): Unit = { }
  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = { }

  override def exec(): Environment = {
    val layout = getInitialLayout()
    echo("Table: " + tableName)
    echo("Column: " + columnName)
    layout.getLocalityGroups().foreach { group =>
      group.getFamilies().foreach { family =>
        if (family.getName().toString() == columnName.family) {
          family.getColumns().foreach { col =>
            if (col.getName().toString() == columnName.qualifier) {
              dumpColumn(col) // Found it!
              return env // Stop searching.
            }
          }
        }
      }
    }

    // If we get here, we couldn't describe the column.
    throw new DDLException("Could not find column " + columnName.family + ":"
        + columnName.qualifier + " in table " + tableName)
  }

  /**
   * Pretty-print the schemas and info for a column.
   *
   * @param the column descriptor
   */
  private def dumpColumn(col: ColumnDesc): Unit = {
    echo("  Description: " + col.getDescription())
    val cellSchema: CellSchema = col.getColumnSchema()

    cellSchema.getType match {
      case SchemaType.COUNTER => { echo("  Schema: (counter)") }
      case SchemaType.CLASS => { echo("  Schema class: "
          + cellSchema.getValue().toString().trim) }
      case SchemaType.INLINE => {
        val jsonSchema = jsonParser.parse(cellSchema.getValue().toString())
        echo("  Schema: " + jsonSchema)
      }
      case SchemaType.AVRO => {
        if (null != cellSchema.getSpecificReaderSchemaClass()) {
          // Show a friendly class name as a courtesy, if it's available.
          echo("  Default reader schema class: " +
              cellSchema.getSpecificReaderSchemaClass())
        }
        echo("")

        if (schemaUsageFlags.reader) {
          // Show the N most recent reader schemas, most recent first.
          echo("  Reader schemas:")
          showSchemas(cellSchema.getReaders().takeRight(numSchemas).reverse, cellSchema)
        }

        if (schemaUsageFlags.writer) {
          echo("  Writer schemas:")
          showSchemas(cellSchema.getWriters().takeRight(numSchemas).reverse, cellSchema)
        }

        if (schemaUsageFlags.recorded) {
          echo("  Recorded schemas:")
          showSchemas(cellSchema.getWritten().takeRight(numSchemas).reverse, cellSchema)
        }
      }
    }
  }

  /**
   * Pretty-print a collection of schemas.
   *
   * @param a list of schemas to display
   * @param the CellSchema they came from
   */
  def showSchemas(schemas: Buffer[java.lang.Long], cellSchema: CellSchema): Unit = {
    schemas.foreach { schemaId: java.lang.Long =>
      val schema: Schema = env.kijiSystem.getSchemaForId(env.instanceURI, schemaId).get
      val jsonSchema = jsonParser.parse(schema.toString())
      if (schemaId == cellSchema.getDefaultReader()) {
        // Mark the default reader schema with an asterisk.
        echoNoNL("(*) ")
      }
      echo("[" + schemaId + "]: " + jsonSchema)
      echo("")
    }
  }
}
