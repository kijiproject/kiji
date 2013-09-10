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

import java.util.{List => JList}

import com.google.common.collect.Lists

import org.apache.avro.Schema

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaStorage
import org.kiji.schema.avro.SchemaType

import org.kiji.schema.shell.DDLException

/**
 * A SchemaSpec that references a schema by its uid in the schema table.
 */
@ApiAudience.Private
final class UidSchemaSpec(uid: Long) extends SchemaSpec {
  override def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema = {
    if (cellSchemaContext.supportsLayoutValidation()) {
      // layout-1.3 and up support layout validation.
      val avroValidationPolicy: AvroValidationPolicy =
          cellSchemaContext.getValidationPolicy().avroValidationPolicy

      val avroSchema = AvroSchema.newBuilder().setUid(uid).build()

      // Sanity check: does this UID exist?
      cellSchemaContext.env.kijiSystem.getSchemaForId(cellSchemaContext.env.instanceURI, uid)
          .getOrElse(throw new DDLException("Invalid schema id: " + uid))

      // Register the specified schema id as a valid reader and writer schema as well as the
      // default reader schema.
      val readers: JList[AvroSchema] = Lists.newArrayList(avroSchema)
      val writers: JList[AvroSchema] = Lists.newArrayList(avroSchema)
      val written: JList[AvroSchema] = Lists.newArrayList(avroSchema)

      return CellSchema.newBuilder()
          .setStorage(SchemaStorage.UID)
          .setType(SchemaType.AVRO)
          .setValue(null)
          .setAvroValidationPolicy(avroValidationPolicy)
          .setSpecificReaderSchemaClass(null)
          .setDefaultReader(avroSchema) // Set this as the default reader too.
          .setReaders(readers)
          .setWritten(written)
          .setWriters(writers)
          .build()
    } else {
      // Pre-validation layouts require a fully-defined schema at all times.
      throw new DDLException("You must define a schema using WITH SCHEMA ... for this column")
    }
  }

  override def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.addToCellSchema() can only be used on validating layouts")
    }

    val avroSchema: Schema = cellSchemaContext.env.kijiSystem.getSchemaForId(
        cellSchemaContext.env.instanceURI, uid)
        .getOrElse(throw new DDLException("Invalid schema id: " + uid))

    // Add to the main reader/writer/etc lists.
    addAvroToCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }

  override def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {

    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.dropFromCellSchema() can only be used "
          + "on validating layouts")
    }

    val avroSchema: Schema = cellSchemaContext.env.kijiSystem.getSchemaForId(
        cellSchemaContext.env.instanceURI, uid)
        .getOrElse(throw new DDLException("Invalid schema id: " + uid))

    // Drop from the main reader/writer/etc lists.
    dropAvroFromCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }

  override def toString(): String = { "(empty schema specification)" }
}
