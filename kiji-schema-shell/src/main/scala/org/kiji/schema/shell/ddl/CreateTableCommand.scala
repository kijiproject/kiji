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
import scala.collection.mutable.Map

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ProtocolVersion

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.key.FormattedKeySpec

object CreateTableCommand {
  /**
   * Version string we embed in layouts created with this tool, advertising the version
   * semantics we declare ourselves to be in line with.
   *
   * Must be &lt;= TableDDLCommand.MAX_LAYOUT_VERSION.
   */
  val DDL_LAYOUT_VERSION: ProtocolVersion = ProtocolVersion.parse("layout-1.3")

  /**
   * Maximum layout version we can support in the system-1.0 instance format.
   */
  val SYS_1_0_DDL_LAYOUT_VERSION: ProtocolVersion = ProtocolVersion.parse("layout-1.2")

  /** The minimum instance format that supports schema validation (layout-1.3). */
  val MIN_SYS_FOR_VALIDATION: ProtocolVersion = ProtocolVersion.parse("system-2.0")
}

/**
 * Creates a new table.
 *
 * @param env the schema shell environment.
 * @param tableName the name of the table to create.
 * @param desc an optional human-readable description for the table.
 * @param rowKeySpec a specification of how row keys are constructed.
 * @param locGroups a list of initial locality group specifications to create the table with.
 */
@ApiAudience.Private
final class CreateTableCommand(val env: Environment,
    val tableName: String,
    val desc: Option[String],
    val rowKeySpec: FormattedKeySpec,
    val locGroups: List[LocalityGroupClause],
    val tableProps: Map[String, Object]) extends TableDDLCommand
    with NewLocalityGroup with TableProperties {

  // We always operate on an empty layout when creating a new table.
  override def getInitialLayout(): TableLayoutDesc.Builder = {
    val instanceFormat: ProtocolVersion = env.kijiSystem.getSystemVersion(env.instanceURI)

    // Determine the layout version to use when creating the new table. If the instance data
    // format supports validation (system-2.0 or greater), use the maximum layout version
    // we can create. Otherwise, we're in a system-1.0 world; use the maximum layout
    // version supported by system-1.0.
    val layoutVersion: ProtocolVersion = (
      if (instanceFormat.compareTo(CreateTableCommand.MIN_SYS_FOR_VALIDATION) >= 0) {
        CreateTableCommand.DDL_LAYOUT_VERSION
      } else {
        CreateTableCommand.SYS_1_0_DDL_LAYOUT_VERSION
      })

    return TableLayoutDesc.newBuilder()
        .setVersion(layoutVersion.toString())
        .setLayoutId("0")
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

  /**
   * Set metatable properties associated with the table we're creating.
   * In particular, set the user's validation preference.
   */
  override def applyMetaUpdates(): Unit = {
    applyMetaUpdates(tableName, tableProps, env)
  }

  override def updateLayout(layout: TableLayoutDesc.Builder): Unit = {
    layout.setName(tableName)
    desc match {
      case Some(descStr) => { layout.setDescription(descStr) }
      case None => { layout.setDescription("") }
    }

    val keysFormat = rowKeySpec.createFormattedKey() // Assigns it a RowKeyFormat2.
    layout.setKeysFormat(keysFormat)

    // Create a context object to pass in to column creation definitions.
    val cellSchemaContext: CellSchemaContext = CellSchemaContext.create(env, layout)

    val localityGroupBuilders = locGroups.foldLeft[List[LocalityGroupDesc.Builder]](Nil)(
        (lst: List[LocalityGroupDesc.Builder], grpData: LocalityGroupClause) => {
          grpData.updateLocalityGroup(newLocalityGroup(), cellSchemaContext) :: lst
        })
    val localityGroups = localityGroupBuilders.map(builder => builder.build())
    layout.setLocalityGroups(localityGroups)

    applyTableProperties(tableProps, layout)
  }

  override def applyUpdate(layout: TableLayoutDesc): Unit = {
    env.kijiSystem.createTable(getKijiURI(), KijiTableLayout.newLayout(layout),
        getInitialRegionCount(tableProps))
  }
}
