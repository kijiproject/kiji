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

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import java.util.{List => JList}

import org.apache.avro.Schema
import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.CellSchema

import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.SchemaUsageFlags

/** Abstract base class for schema specifications. */
@ApiAudience.Private
abstract class SchemaSpec {
  /**
   * Processes this schema representation and returns a new CellSchema defining the schema
   * associated with a column or family. In layout-1.0 through 1.2, this will create a
   * non-validating CellSchema object. In layout-1.3 and above (as specified in the
   * CellSchemaContext), it will create a validating CellSchema object with the specified schema
   * used as both a reader, writer and the default reader schema.
   *
   * @param cellSchemaContext the context defining the table we are adding a new cell schema to.
   * @return the constructed Avro CellSchema definition.
   */
  def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema

  /**
   * Processes an existing column or family's CellSchema, adding the defined schema
   * to its list of supported schemas. The exact manner of the addition (Reader, writer,
   * or both) is specified in the cellSchemaContext.schemaUsageFlags argument.
   *
   * In non-validating layouts (&le; 1.2), this method may not be used. Non-validating
   * layouts should replace their existing CellSchema with a new one constructed by
   * `toNewCellSchema()`.
   *
   * @param the CellSchema Avro object to modify.
   * @param cellSchemaContext additional context about the table we are operating on and how
   *     we should add this schema to the cell.
   * @return the modified CellSchema object.
   */
  def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext): CellSchema

  /**
   * Processes an existing column or family's CellSchema, removing the specified schema
   * from its list of supported schemas. The exact manner of the removal (Reader, writer,
   * or both) is specified in the cellSchemaContext.schemaUsageFlags argument.
   *
   * In non-validating layouts (&lt;= 1.2), this method may not be used. Non-validating
   * layouts should replace their existing CellSchema with a new one constructed by
   * `toNewCellSchema()`.
   *
   * @param the CellSchema Avro object to modify.
   * @param cellSchemaContext additional context about the table we are operating on and how
   *     we should drop this schema from the cell.
   * @return the modified CellSchema object.
   */
  def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext): CellSchema

  /**
   * Helper method for addToCellSchema() implementations. Given an Avro schema, add it to
   * the list of approved schemas in the CellSchema in accordance with the schemaUsageFlags.
   *
   * @param schema the Avro schema to add
   * @param cellSchema the CellSchema to modify
   * @param schemaUsageFlags the flags specifying which contexts (reader, writer..)
   *     the schema should be added to.
   * @param env the operating Kiji shell environment.
   * @return the modified CellSchema.
   */
  protected def addAvroToCellSchema(
      schema: Schema,
      cellSchema: CellSchema,
      schemaUsageFlags: SchemaUsageFlags,
      env: Environment): CellSchema = {

    // Use the schema table to find the actual uid associated with this schema.
    // If this schema has not yet been encountered, register it and get a new uid.
    val uidForSchemaClass: Long = env.kijiSystem.getOrCreateSchemaId(env.instanceURI, schema)
    val avroSchema = AvroSchema.newBuilder().setUid(uidForSchemaClass).build()

    if (schemaUsageFlags.defaultReader) {
      // Set this schema as the default reader schema.
      cellSchema.setDefaultReader(avroSchema)

      if (schema.getType() == Schema.Type.RECORD
          || schema.getType() == Schema.Type.FIXED
          || schema.getType() == Schema.Type.ENUM) {
        // Setting the default reader schema to a named type sets the name of any
        // associated SpecificRecord to extract.
        val existingFullName: String =
            Option(cellSchema.getSpecificReaderSchemaClass()).getOrElse("")
        val newFullName: String = schema.getFullName()
        if (newFullName != existingFullName) {
          env.printer.println("Warning: Changing specific reader schema class to " + newFullName)
          cellSchema.setSpecificReaderSchemaClass(newFullName)
        }
      } else if (cellSchema.getSpecificReaderSchemaClass() != null) {
        // There was a specific reader schema class specified, but this new default reader
        // is not compatible with it (it has no name). Remove the default reader class name.
        env.printer.println("Warning: Removing specific reader schema class")
        cellSchema.setSpecificReaderSchemaClass(null)
      }
    }

    if (schemaUsageFlags.reader || schemaUsageFlags.defaultReader) {
      // If this is a reader object (or defaultReader, which implies reader), add it
      // to the list of approved reader schemas.
      val readers: Buffer[AvroSchema] = cellSchema.getReaders().asScala
      if (readers.filter { as => (as.getUid == uidForSchemaClass) }.isEmpty) {
        readers += avroSchema
      }
    }

    if (schemaUsageFlags.writer) {
      // Add this class to the list of approved writer schemas.
      val writers: Buffer[AvroSchema] = cellSchema.getWriters().asScala
      if (writers.filter { as => (as.getUid == uidForSchemaClass) }.isEmpty) {
        writers += avroSchema
      }
    }

    if (schemaUsageFlags.recorded || schemaUsageFlags.writer) {
      // If this is being specified as a "recorded" schema (or a writer schema, which implies
      // recorded), add it to the "written" list in the CellSchema.

      val written: Buffer[AvroSchema] = cellSchema.getWritten().asScala
      if (written.filter { as => (as.getUid == uidForSchemaClass) }.isEmpty) {
        written += avroSchema
      }
    }

    return cellSchema
  }

  /**
   * Helper method for dropFromCellSchema() implementations. Given an Avro schema, remove it from
   * the list of approved schemas in the CellSchema in accordance with the schemaUsageFlags.
   *
   * @param the Avro schema to remove
   * @param the CellSchema to modify
   * @param the flags specifying which contexts (reader, writer..) the schema should be dropped
   *     from.
   * @param the operating Kiji shell environment.
   * @return the modified CellSchema.
   */
  protected def dropAvroFromCellSchema(avroSchema: Schema, cellSchema: CellSchema,
      schemaUsageFlags: SchemaUsageFlags, env: Environment): CellSchema = {

    // Use the schema table to find the actual uid associated with this schema.
    // If this returns "None" then the schema never existed -- do nothing.
    val maybeUidForSchemaClass: Option[Long] =
        env.kijiSystem.getSchemaId(env.instanceURI, avroSchema)
    if (maybeUidForSchemaClass.isEmpty) {
      // Nothing to do; this schema is not registered in the schema table.
      return cellSchema
    }

    val uidForSchemaClass: Long = maybeUidForSchemaClass.get

    if (schemaUsageFlags.defaultReader || schemaUsageFlags.reader) {
      // Reader removal implies default reader removal.

      Option(cellSchema.getDefaultReader()) match {
        case None => { /* no default reader to remove */ }
        case Some(curDefaultReader: AvroSchema) => {
          if (curDefaultReader.getUid == uidForSchemaClass) {
            // Detach this from being the default reader.
            env.printer.println("Warning: Removing default reader schema")
            cellSchema.setDefaultReader(null)
          }
        }
      }
    }

    if (schemaUsageFlags.reader) {
      // If this is in the reader schemas list, drop it from the list of approved reader schemas.
      // Note: the shell will not redundantly add a schema, but a user may manually do so in the
      // layout. If he does this, we will only remove the first instance in the list.
      val readers: Buffer[AvroSchema] = cellSchema.getReaders().asScala
      val toRemove = readers.filter { as => (as.getUid == uidForSchemaClass) }
      readers --= toRemove
    }

    if (schemaUsageFlags.writer) {
      // If this is in the writer schemas list, drop it from the list of approved writer schemas.
      val writers: Buffer[AvroSchema] = cellSchema.getWriters().asScala
      val toRemove = writers.filter { as => (as.getUid == uidForSchemaClass) }
      writers --= toRemove
    }

    if (schemaUsageFlags.recorded) {
      // If this is being specified as a "recorded" schema
      // drop it from the "written" list in the CellSchema.
      val written: Buffer[AvroSchema] = cellSchema.getWritten().asScala
      val toRemove = written.filter { as => (as.getUid == uidForSchemaClass) }
      written --= toRemove
    }

    return cellSchema
  }
}
