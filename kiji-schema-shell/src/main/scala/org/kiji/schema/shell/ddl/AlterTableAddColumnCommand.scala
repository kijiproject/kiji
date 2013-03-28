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

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

/** Add a column to a group-type family in a table. */
@ApiAudience.Private
final class AlterTableAddColumnCommand(
    val env: Environment,
    val tableName: String,
    val colClause: ColumnClause) extends TableDDLCommand {

  val familyName: String = (
    // Extract the column family from the ColumnClause and verify its presence.
    colClause.family.getOrElse {
      throw new DDLException("Column clause must include column family name")
    }
  )

  override def validateArguments(): Unit = {
    checkTableExists()
    val layout = getInitialLayout()
    checkColFamilyExists(layout, familyName)
    checkColFamilyIsGroupType(layout, familyName)
    checkColumnMissing(layout, familyName, colClause.qualifier)
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    // Get the group-type column family from the layout and add the column to its list.
    getFamily(layout, familyName).getOrElse(throw new DDLException("Missing family!"))
        .getColumns().add(colClause.toAvroColumnDesc())
  }
}
