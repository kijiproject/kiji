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

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaStorage
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.shell.DDLException

/**
 * A SchemaSpec that is empty; a container to be filled by subsequent
 * ALTER TABLE.. ADD SCHEMA statements.
 */
@ApiAudience.Private
final class EmptySchemaSpec extends SchemaSpec {
  override def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema = {
    if (cellSchemaContext.supportsLayoutValidation()) {
      // layout-1.3 and up support layout validation.
      val avroValidationPolicy: AvroValidationPolicy =
          cellSchemaContext.getValidationPolicy().avroValidationPolicy
      // Create a CellSchema associated with this column but don't add any reader/writer schemas.
      // The user must add these herself with ALTER TABLE.. ADD SCHEMA
      cellSchemaContext.env.printer.println("Warning: Creating a column with no associated schema")
      cellSchemaContext.env.printer.println(
          "Add one with: ALTER TABLE <t> ADD SCHEMA <s> FOR COLUMN family:qualifier;")
      return CellSchema.newBuilder()
          .setStorage(SchemaStorage.UID)
          .setType(SchemaType.AVRO)
          .setValue(null)
          .setAvroValidationPolicy(avroValidationPolicy)
          .setSpecificReaderSchemaClass(null)
          .setDefaultReader(null)
          .setReaders(new ArrayList())
          .setWritten(new ArrayList())
          .setWriters(new ArrayList())
          .build()
    } else {
      // Pre-validation layouts require a fully-defined schema at all times.
      throw new DDLException("You must define a schema using WITH SCHEMA ... for this column")
    }
  }

  override def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    // This method is unsupported; ALTER TABLE.. ADD SCHEMA requires a real schema.
    throw new DDLException("You must specify a non-empty schema to add")
  }

  override def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    // This method is unsupported; ALTER TABLE.. DROP SCHEMA requires a real schema.
    throw new DDLException("You must specify a non-empty schema to drop")
  }

  override def toString(): String = { "(empty schema specification)" }
}
