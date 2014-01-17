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
import org.kiji.schema.avro.LocalityGroupDesc

/**
 * Represents the specification of a locality group in a CREATE or ALTER TABLE statement.
 */
@ApiAudience.Private
final class LocalityGroupClause(val name: String,
                          val desc: Option[String],
                          val props: List[LocalityGroupProp]) {

  /**
   * Update a LocalityGroupDesc builder with the attributes associated with this locality
   * group clause.
   *
   * @param locGroup a builder for the locality group descriptor to modify.
   * @param cellSchemaContext context about the table being modified.
   * @return The same LocalityGroupDesc builder, modified with updated fields.
   */
  def updateLocalityGroup(locGroup: LocalityGroupDesc.Builder,
      cellSchemaContext: CellSchemaContext): LocalityGroupDesc.Builder = {

    Option(locGroup.getName()) match {
      case Some(existingName) => {
        if (!existingName.equals(name)) {
          // Rename the group
          locGroup.setRenamedFrom(existingName)
          locGroup.setName(name)
        }
      }
      case None => { locGroup.setName(name) }
    }

    desc match {
      case Some(descStr) => { locGroup.setDescription(descStr) }
      case None => { } // Don't overwrite the existing description.
    }
    props.foreach { prop =>
      prop.apply(locGroup, cellSchemaContext)
    }

    return locGroup
  }

  /**
   * @return a list of the names of column families described in this locality group clause.
   */
  def getFamilyNames(): List[String] = {
    // Extract properties of this locality group that specify the creation of map- or group-type
    // families, ignoring all other non-family properties. Then convert these from String options
    // into just strings.
    return props.map({ prop =>
      prop.property match {
        case LocalityGroupPropName.MapFamily => Some(prop.value.asInstanceOf[MapFamilyInfo].name)
        case LocalityGroupPropName.GroupFamily => (
          Some(prop.value.asInstanceOf[GroupFamilyInfo].name)
        )
        case _ => None
      }
    }).filter(item => !item.equals(None)).map(some_item => some_item.get)
  }
}
