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
import org.kiji.schema.avro.ColumnDesc

/**
 * The specification of a column to add in an ALTER TABLE ADD FAMILY, ALTER TABLE ADD COLUMN,
 * etc. The 'family' field is filled out in an ADD COLUMN statement. When a ColumnClause is
 * nested in an encasing GROUP TYPE FAMILY definition, it may be left as None.
 */
@ApiAudience.Private
final class ColumnClause(
    val family: Option[String],
    val qualifier: String,
    val schema: SchemaSpec,
    val desc: Option[String]) {

  /** @return a new ColumnDesc that describes this column. */
  def toAvroColumnDesc(cellSchemaContext: CellSchemaContext): ColumnDesc = {
    return ColumnDesc.newBuilder()
        .setName(qualifier)
        .setAliases(new java.util.ArrayList[String])
        .setEnabled(true)
        .setDescription(desc match {
            case Some(descStr) => descStr
            case None => ""
            })
        .setColumnSchema(schema.toNewCellSchema(cellSchemaContext))
        .build()
  }
}

