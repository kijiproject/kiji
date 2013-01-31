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

package org.kiji.schema.shell

import java.io.IOException

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin

import org.kiji.schema.Kiji
import org.kiji.schema.KijiConfiguration

import org.kiji.schema.KijiAdmin
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout

/**
 * Abstract base interface implemented by KijiSystem. Provides method signatures
 * that access and modify underlying resources in KijiSchema.
 */
abstract class AbstractKijiSystem {
  /**
   * Gets a collection of Kiji table names and descriptions for the specified Kiji instance.
   *
   * @param instance The Kiji whose tables should be listed.
   * @return An array of pairs, where each pair contains the name of a Kiji table and the
   *     description for that Kiji table.
   */
  def getTableNamesDescriptions(instance: String): Array[(String, String)]

  /**
   * Gets the layout for the specified Kiji table.
   *
   * @param instance The Kiji instance containing the table.
   * @param table The table whose layout should be retrieved.
   * @return The table layout, or None if the layout cannot be retrieved.
   */
  def getTableLayout(instance: String, table: String): Option[KijiTableLayout]

  /** Create a new table in the specified Kiji instance. */
  def createTable(instance: String, table: String, layout: KijiTableLayout): Unit

  /** Apply a table layout update to a Kiji table in the specified instance. */
  def applyLayout(instance: String, table: String, layout: TableLayoutDesc): Unit

  /** Drop a table. */
  def dropTable(instance: String, table: String): Unit

  /**
   * @return a list of all available Kiji instances.
   */
  def listInstances(): Set[String]

  /**
   * Close all Kiji-related resources opened by this object.
   */
  def shutdown(): Unit
}
