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
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ProtocolVersion

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.TableNotFoundException

/**
 * Abstract base class for DDL command implementations that manipulate properties
 * of a specific table (vs. those which adjust the environment, instance, etc.).
 */
@ApiAudience.Private
abstract class TableDDLCommand extends DDLCommand {

  /**
   * The maximum version string we are comfortable operating on. Newer layers than
   * this must be modified with newer tools.
   */
  val MAX_LAYOUT_VERSION = ProtocolVersion.parse("layout-1.3");

  /** The name of the table being operated on. */
  val tableName: String;

  /**
   * Method called by the runtime to execute this parsed command.
   * @return the environment object to use in subsequent commands.
   */
  def exec(): Environment = {
    // Default behavior: Get the table layout, mutate it, and apply the new layout.
    validateArguments()
    val layoutBuilder = getInitialLayout()
    applyMetaUpdates()
    updateLayout(layoutBuilder)
    applyUpdate(layoutBuilder.build())
    echo("OK.")
    return env
  }

  /**
   * Retrieve the table layout to modify from Kiji.
   */
  def getInitialLayout(): TableLayoutDesc.Builder = {
    env.kijiSystem.getTableLayout(getKijiURI(), tableName) match {
      case Some(layout) => {
        val desc = layout.getDesc()
        val tableProtocol = ProtocolVersion.parse(desc.getVersion())

        // Do not modify tables created with newer versions of KijiSchema. We cannot
        // decode all their Avro in a safe fashion.
        if (tableProtocol.compareTo(MAX_LAYOUT_VERSION) > 0) {
          throw new DDLException("Table " + tableName + " is specified by layout version "
              + tableProtocol.toString() + ", but this tool can not modify layouts newer "
              + "than " + MAX_LAYOUT_VERSION.toString() + ". Please use a newer version of "
              + "the KijiSchema shell.")
        }
        return TableLayoutDesc.newBuilder(desc)
      }
      case None => { throw new TableNotFoundException(tableName) }
    }
  }

  /**
   * Validates that the arguments to this command can be applied correctly.
   * Subclasses should perform checks here (e.g., that a particular column family exists)
   * and throw DDLException if there's an error.
   */
  def validateArguments(): Unit

  /**
   * Given a builder for a table layout (e.g., from getInitialLayout()), apply any mutations to the
   * data structure representing the table's layout.
   */
  def updateLayout(layout: TableLayoutDesc.Builder): Unit

  /**
   * Apply any updates to the metatable necessary for this table DDL operation.
   * These are performed before building and applying the table layout itself.
   */
  def applyMetaUpdates(): Unit = {
    // Default implementation does nothing.
  }

  /**
   * Given a built table layout, apply it to the Kiji instance (e.g., by creating a table,
   * or updating an existing one.) The default behavior is to assume the table
   * already exists, and apply the layout to the table using KijiAdmin.
   */
  def applyUpdate(layout: TableLayoutDesc): Unit = {
    Option(layout.getLayoutId()) match {
      case None => { } // No previous layout to refer to.
      case Some(ref) => {
        layout.setReferenceLayout(ref)
        try {
          // By default, layout IDs are integers incremented with each new layout:
          val refLayoutId = ref.toLong
          layout.setLayoutId((refLayoutId + 1).toString)
        } catch {
          case _: NumberFormatException =>
            // If a reference layout ID is not an integer (eg. a user manually wrote a layout
            // with a custom layout ID),
            // sets the new layout ID to be the current time since the Epoch.
            layout.setLayoutId(System.currentTimeMillis.toString)
        }
      }
    }
    env.kijiSystem.applyLayout(getKijiURI(), tableName, layout)
  }

  // Methods that check properties of tables for use in validateArguments().
  // On error, they throw DDLException. On success they do nothing.

  protected def checkColFamilyExists(
      layout: TableLayoutDesc.Builder, familyName: String): Unit = {
    getFamily(layout, familyName) match {
      case None => {
        throw new DDLException("No such family \"" + familyName + "\" in table " + layout.getName())
      }
      case Some(f) => { }
    }
  }

  protected def checkColFamilyIsGroupType(
      layout: TableLayoutDesc.Builder, familyName: String): Unit = {
    getFamily(layout, familyName) match {
      case None => {
        throw new DDLException("No such family \"" + familyName + "\" in table " + layout.getName())
      }
      case Some(f) => {
        Option(f.getMapSchema()) match {
          case Some(schema) => {
            throw new DDLException("Expected group-type family \"" + familyName + "\"")
          }
          case None => { }
        }
      }
    }
  }

  protected def checkColumnExists(
      layout: TableLayoutDesc.Builder, familyName: String, qualifier: String): Unit = {
    getFamily(layout, familyName).getOrElse(
        throw new DDLException("No such family \"" + familyName + "\""))
    checkColFamilyIsGroupType(layout, familyName)
    getColumn(layout, familyName, qualifier).getOrElse(
        throw new DDLException("Column \"" + familyName + ":" + qualifier
                  + "\" does not exist."))
  }

  protected def checkColumnMissing(layout: TableLayoutDesc.Builder, familyName: String,
      qualifier: String): Unit = {
    getFamily(layout, familyName).getOrElse(
        throw new DDLException("No such family \"" + familyName + "\""))
    checkColFamilyIsGroupType(layout, familyName)

    getColumn(layout, familyName, qualifier) match {
      case None  => { /* expected. */ }
      case Some(c) => {
        throw new DDLException("Column \"" + familyName + ":" + qualifier
                  + "\" already exists.")
      }
    }
  }

  protected def checkColFamilyIsMapType(
      layout: TableLayoutDesc.Builder, familyName: String): Unit = {
    getFamily(layout, familyName) match {
      case None => {
        throw new DDLException("No such family \"" + familyName + "\" in table " + layout.getName())
      }
      case Some(f) => {
        Option(f.getMapSchema()).getOrElse({
          throw new DDLException("Expected map-type family \"" + familyName + "\"")
        })
      }
    }
  }

  protected def checkColFamilyMissing(
      layout: TableLayoutDesc.Builder, familyName: String): Unit = {
    getFamily(layout, familyName) match {
      case Some(f) => {
        throw new DDLException("Family \"" + familyName + "\" already exists in table "
            + layout.getName())
      }
      case None => { }
    }
  }

  protected def checkLocalityGroupExists(
      layout: TableLayoutDesc.Builder, groupName: String): Unit = {
    getLocalityGroup(layout, groupName).getOrElse(
        throw new DDLException("No such locality group \"" + groupName + "\" in table "
            + layout.getName()))
  }

  protected def checkLocalityGroupMissing(
      layout: TableLayoutDesc.Builder, groupName: String): Unit = {
    getLocalityGroup(layout, groupName) match {
      case Some(lg) => {
        throw new DDLException("Locality group \"" + groupName + "\" already exists in table "
            + layout.getName())
      }
      case None => { }
    }
  }

  protected def checkTableExists(): Unit = {
    env.kijiSystem.getTableLayout(getKijiURI(), tableName) match {
      case Some(layout) => { /* success. */ }
      case None => throw new DDLException("No such table \"" + tableName + "\"")
    }
  }

  /**
   * Extracts a mutable ColumnDesc from a TableLayoutDesc builder.
   *
   * @param layout the builder for the Avro table description to walk.
   * @param familyName the family name for the column to extract.
   * @param qualifier the qualifier for the column to extract.
   * @return Some[ColumnDesc] describing the column, or None.
   */
  protected def getColumn(layout: TableLayoutDesc.Builder, familyName: String,
      qualifier: String): Option[ColumnDesc] = {
    layout.getLocalityGroups().foreach { localityGroup =>
      localityGroup.getFamilies().foreach { family =>
        if (family.getName().equals(familyName)) {
          family.getColumns().foreach { column =>
            if (column.getName().equals(qualifier)) {
              return Some(column)
            }
          }
        }
      }
    }

    return None
  }

  /**
   * Extracts a mutable ColumnDesc from a TableLayoutDesc builder.
   *
   * @param layout the builder for the Avro table description to walk.
   * @param columnName the family name and qualifier for the column to extract.
   * @return Some[ColumnDesc] describing the column, or None.
   */
  protected def getColumn(
      layout: TableLayoutDesc.Builder, columnName: ColumnName): Option[ColumnDesc] = {
    getColumn(layout, columnName.family, columnName.qualifier)
  }

  /**
   * Extracts a mutable FamilyDesc from a TableLayoutDesc builder.
   *
   * @param layout the builder for the Avro table description to walk.
   * @param familyName the family name to extract.
   * @return Some[FamilyDesc] describing the family, or None.
   */
  protected def getFamily(
      layout: TableLayoutDesc.Builder, familyName: String): Option[FamilyDesc] = {
    layout.getLocalityGroups().foreach { localityGroup =>
      localityGroup.getFamilies().foreach { family =>
        if (family.getName().equals(familyName)) {
          return Some(family)
        }
      }
    }

    return None
  }

  /**
   * Extracts a mutable LocalityGroupDesc from a TableLayoutDesc builder.
   *
   * @param layout the builder for the Avro table description to walk.
   * @param localityGroupName the locality group name to extract.
   * @return Some[LocalityGroupDesc] describing the group, or None.
   */
  protected def getLocalityGroup(layout: TableLayoutDesc.Builder,
      localityGroupName: String): Option[LocalityGroupDesc] = {
    layout.getLocalityGroups().foreach { localityGroup =>
      if (localityGroup.getName() == localityGroupName) {
        return Some(localityGroup)
      }
    }

    return None
  }
}
