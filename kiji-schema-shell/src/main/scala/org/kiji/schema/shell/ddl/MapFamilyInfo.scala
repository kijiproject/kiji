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
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc

/**
 * Holds info describing a map-type column family.
 */
@ApiAudience.Private
final class MapFamilyInfo(val name: String, val schema: SchemaSpec, val desc: Option[String]) {
  /**
   * Add this new map family definition to a locality group's builder.
   * Assumes that this family name does not exist elsewhere in the layout
   * (verified in AlterTableAddMapFamilyCommand.validateArguments()).
   */
  def addToLocalityGroup(group: LocalityGroupDesc.Builder, cellSchemaContext: CellSchemaContext):
      Unit = {
    val mapFamily = FamilyDesc.newBuilder()
    mapFamily.setName(name)
    mapFamily.setEnabled(true)
    desc match {
      case Some(descStr) => { mapFamily.setDescription(descStr) }
      case None => { mapFamily.setDescription("") }
    }

    mapFamily.setMapSchema(schema.toNewCellSchema(cellSchemaContext))
    mapFamily.setColumns(new ArrayList[ColumnDesc])
    mapFamily.setAliases(new ArrayList[String])
    group.getFamilies().add(mapFamily.build())
  }
}
