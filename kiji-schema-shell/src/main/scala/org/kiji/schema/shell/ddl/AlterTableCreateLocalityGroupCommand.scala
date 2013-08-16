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

/** Add a locality group to a table. */
@ApiAudience.Private
final class AlterTableCreateLocalityGroupCommand(
    val env: Environment,
    val tableName: String,
    val locGroup: LocalityGroupClause) extends TableDDLCommand with NewLocalityGroup {

  override def validateArguments(): Unit = {
    checkTableExists()
    val layout = getInitialLayout()
    // Make sure that the locality group doesn't exist, nor do any column families
    // share names with the column families we want to create in this locality group.
    checkLocalityGroupMissing(layout, locGroup.name)
    val families = locGroup.getFamilyNames()
    families.foreach { name =>
      checkColFamilyMissing(layout, name)
    }

    if (families.distinct.size != families.size) {
      throw new DDLException("A column family is defined more than once in this locality group")
    }
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    val avroLocGroupBuilder = newLocalityGroup()
    locGroup.updateLocalityGroup(avroLocGroupBuilder, CellSchemaContext.create(env, layout))
    layout.getLocalityGroups().add(avroLocGroupBuilder.build())
  }
}
