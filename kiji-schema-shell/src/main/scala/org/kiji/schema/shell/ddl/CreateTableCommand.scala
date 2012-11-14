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
import scala.collection.mutable.ListBuffer

import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.VersionInfo

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

class CreateTableCommand(val env: Environment,
                         val tableName: String,
                         val desc: Option[String],
                         val rowKeySpec: RowKeySpec,
                         val locGroups: List[LocalityGroupClause]) extends TableDDLCommand
                         with NewLocalityGroup {

  // We always operate on an empty layout when creating a new table.
  override def getInitialLayout(): TableLayoutDesc = {
    val layout = new TableLayoutDesc
    layout.setVersion(VersionInfo.getClientDataVersion())
    return layout
  }

  override def validateArguments(): Unit = {
    // Check that the table doesn't exist.
    env.kijiSystem.getTableLayout(getKijiInstance(), tableName) match {
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

  override def updateLayout(layout: TableLayoutDesc): Unit = {
    layout.setName(tableName)
    desc match {
      case Some(descStr) => { layout.setDescription(descStr) }
      case None => { layout.setDescription("") }
    }

    val keysFormat = rowKeySpec.toAvroFormat()
    layout.setKeysFormat(keysFormat)

    var localityGroups = locGroups.foldLeft[List[LocalityGroupDesc]](Nil)(
        (lst: List[LocalityGroupDesc], grpData:LocalityGroupClause) => {
          grpData.updateLocalityGroup(newLocalityGroup()) :: lst
        })
    layout.setLocalityGroups(ListBuffer(localityGroups: _*))
  }

  override def applyUpdate(layout: TableLayoutDesc): Unit = {
    env.kijiSystem.createTable(getKijiInstance(), tableName, new KijiTableLayout(layout, null))
  }
}
