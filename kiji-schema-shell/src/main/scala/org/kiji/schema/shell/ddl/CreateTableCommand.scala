/**
 * (c) Copyright 2012 WibiData, Inc.
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

import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ProtocolVersion

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

object CreateTableCommand {
  /**
   * Version string we embed in layouts created with this tool, advertising the version
   * semantics we declare ourselves to be in line with.
   */
  val DDL_LAYOUT_VERSION = ProtocolVersion.parse("layout-1.1");
}

class CreateTableCommand(val env: Environment,
                         val tableName: String,
                         val desc: Option[String],
                         val rowKeySpec: RowKeySpec,
                         val locGroups: List[LocalityGroupClause]) extends TableDDLCommand
                         with NewLocalityGroup {

  // We always operate on an empty layout when creating a new table.
  override def getInitialLayout(): TableLayoutDesc.Builder = {
    return TableLayoutDesc.newBuilder()
        .setVersion(CreateTableCommand.DDL_LAYOUT_VERSION.toString())
  }

  override def validateArguments(): Unit = {
    // Check that the table doesn't exist.
    env.kijiSystem.getTableLayout(getKijiURI(), tableName) match {
      case Some(layout) => { throw new DDLException("Table " + tableName + " already exists.") }
      case _ => ()
    }

    // Validate that we have at least one locality group specified.
    if (locGroups.length == 0) {
      throw new DDLException("At least one LOCALITY GROUP definition is required.")
    }

    if (tableName.length == 0) {
      throw new DDLException("Table cannot have empty name.")
    }

    // Don't allow a column family to be defined in multiple locality groups.
    val allFamilyNames = locGroups.flatMap(group => group.getFamilyNames())
    if (allFamilyNames.distinct.size != allFamilyNames.size) {
      throw new DDLException("You have defined a column family multiple times")
    }

    // Validate ROW KEY FORMAT arguments.
    rowKeySpec.validate()
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    layout.setName(tableName)
    desc match {
      case Some(descStr) => { layout.setDescription(descStr) }
      case None => { layout.setDescription("") }
    }

    val keysFormat = rowKeySpec.toAvroFormat()
    layout.setKeysFormat(keysFormat)

    val localityGroupBuilders = locGroups.foldLeft[List[LocalityGroupDesc.Builder]](Nil)(
        (lst: List[LocalityGroupDesc.Builder], grpData: LocalityGroupClause) => {
          grpData.updateLocalityGroup(newLocalityGroup()) :: lst
        })
    val localityGroups = localityGroupBuilders.map(builder => builder.build())
    layout.setLocalityGroups(localityGroups)
  }

  override def applyUpdate(layout: TableLayoutDesc): Unit = {
    env.kijiSystem.createTable(getKijiURI(), tableName, KijiTableLayout.newLayout(layout))
  }
}
