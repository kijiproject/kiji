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

/** Rename 'locGroupName' to 'targetName' in a table. */
@ApiAudience.Private
final class AlterTableRenameLocalityGroupCommand(
    val env: Environment,
    val tableName: String,
    val locGroupName: String,
    val targetName: String) extends TableDDLCommand {

  override def validateArguments(): Unit = {
    val layout = getInitialLayout()
    checkTableExists()
    checkLocalityGroupExists(layout, locGroupName)
    checkLocalityGroupMissing(layout, targetName)
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    // Rename the locality group.
    val localityGroup = getLocalityGroup(layout, locGroupName).getOrElse(
        throw new DDLException("No such locality group"))
    localityGroup.setName(targetName)
    localityGroup.setRenamedFrom(locGroupName)
  }
}
