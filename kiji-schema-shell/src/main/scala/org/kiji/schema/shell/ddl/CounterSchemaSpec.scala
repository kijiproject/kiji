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

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaStorage
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.shell.DDLException

/**
 * A schema that represents an HBase counter.
 */
@ApiAudience.Private
final class CounterSchemaSpec() extends SchemaSpec {
  override def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema = {
    return CellSchema.newBuilder()
        .setType(SchemaType.COUNTER)
        .setValue(null)
        .setStorage(SchemaStorage.FINAL)
        .build()
  }

  override def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    // If we weren't already a COUNTER column, we can't change into one.
    if (cellSchema.getType() != SchemaType.COUNTER) {
      throw new DDLException("Cannot change a non-counter column to support counters.")
    }

    // If we are a COUNTER column, there's nothing to do.
    cellSchemaContext.env.printer.println(
        "Schema type for this column is already COUNTER (no change).")
    return cellSchema
  }

  override def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    // This is a 'final' schema; don't support evolution.
    throw new DDLException("Cannot deregister a column from being a COUNTER type.")
  }

  override def toString(): String = { "(COUNTER schema)" }
}
