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

import java.util.ArrayList

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc

/**
 * Holds info describing a group-type column family.
 */
@ApiAudience.Private
final class GroupFamilyInfo(val name: String, val desc: Option[String],
    val cols: List[ColumnClause]) {

  /**
   * Add this new group family definition to a locality group's builder.
   * Assumes that this family name does not exist elsewhere in the layout
   * (verified in AlterTableAddGroupFamilyCommand.validateArguments()).
   */
  def addToLocalityGroup(locGroup: LocalityGroupDesc.Builder,
      cellSchemaContext: CellSchemaContext): Unit = {
    val groupFamily = FamilyDesc.newBuilder()
    groupFamily.setName(name)
    groupFamily.setEnabled(true)
    desc match {
      case Some(descStr) => { groupFamily.setDescription(descStr) }
      case None => { groupFamily.setDescription("") }
    }

    var avroCols = cols.map(c => c.toAvroColumnDesc(cellSchemaContext))
    groupFamily.setColumns(avroCols)
    groupFamily.setAliases(new ArrayList[String])
    locGroup.getFamilies().add(groupFamily.build())
  }
}
