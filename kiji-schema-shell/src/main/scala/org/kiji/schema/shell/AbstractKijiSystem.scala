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

package org.kiji.schema.shell

import java.io.IOException

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.avro.Schema
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.Kiji
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.security.KijiSecurityManager
import org.kiji.schema.util.ProtocolVersion

/**
 * Abstract base interface implemented by KijiSystem. Provides method signatures
 * that access and modify underlying resources in KijiSchema.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
abstract class AbstractKijiSystem {
  /**
   * Gets a collection of Kiji table names and descriptions for the specified Kiji instance.
   *
   * @param uri The Kiji instance whose tables should be listed.
   * @return An array of pairs, where each pair contains the name of a Kiji table and the
   *     description for that Kiji table.
   */
  def getTableNamesDescriptions(uri: KijiURI): Array[(String, String)]

  /**
   * Gets the layout for the specified Kiji table.
   *
   * @param uri of the Kiji instance containing the table.
   * @param table whose layout should be retrieved.
   * @return The table layout, or None if the layout cannot be retrieved.
   */
  def getTableLayout(uri: KijiURI, table: String): Option[KijiTableLayout]

  /** Create a new table in the specified Kiji instance. */
  def createTable(uri: KijiURI, layout: KijiTableLayout, numRegions: Int): Unit

  /** Apply a table layout update to a Kiji table in the specified instance. */
  def applyLayout(uri: KijiURI, table: String, layout: TableLayoutDesc): Unit

  /** Drop a table. */
  def dropTable(uri: KijiURI, table: String): Unit

  /**
   * Return the instance data format (system version) of the specified Kiji instance.
   *
   * @param uri is the KijiURI of the instance to open.
   * @return the ProtocolVersion of the corresponding data format (e.g. "system-2.0")
   */
  def getSystemVersion(uri: KijiURI): ProtocolVersion

  /**
   * Return a schema id associated with a given schema, creating an association if necessary.
   *
   * @param uri of the Kiji instance we are operating in.
   * @param schema to lookup.
   * @return its existing uid, or a new id if this schema has not been encountered before.
   */
  def getOrCreateSchemaId(uri: KijiURI, schema: Schema): Long

  /**
   * Return a schema id associated with a given schema, or None if it's not already there.
   *
   * @param uri of the Kiji instance we are operating in.
   * @param schema to lookup.
   * @return its existing uid, or None if this schema has not been encountered before.
   */
  def getSchemaId(uri: KijiURI, schema: Schema): Option[Long]

  /**
   * Return a schema associated with a given schema id, or None if it's not already there.
   *
   * @param uri of the Kiji instance we are operating in.
   * @param uid of the schema id to lookup.
   * @return its existing schema, or None if this schema id has not been encountered before.
   */
  def getSchemaForId(uri: KijiURI, uid: Long): Option[Schema]

  /**
   * Return a schema associated with a given AvroSchema descriptor.
   *
   * @param uri is the Kiji instance we are operating in
   * @param schema is the Avro schema descriptor to evaluate
   * @return its existing schema, or None if this schema id has not been encountered before.
   */
  def getSchemaFor(uri: KijiURI, schema: AvroSchema): Option[Schema]

  /**
   * Set a metatable (key, val) pair for the specified table.
   *
   * @param uri of the Kiji instance.
   * @param table is the name of the table owning the property.
   * @param key to set.
   * @param value to set it to.
   * @throws IOException if there's an error opening the metatable.
   */
  def setMeta(uri: KijiURI, table: String, key: String, value: String): Unit

  /**
   * Get a metatable (key, val) pair for the specified table.
   *
   * @param uri of the Kiji instance.
   * @param table is the name of the table owning the property.
   * @param key specifying the property to retrieve.
   * @return the value of the property or None if it was not yet set.
   * @throws IOException if there's an error opening the metatable.
   */
  def getMeta(uri: KijiURI, table: String, key: String): Option[String]

  /**
   * Get the security manager for the specified instance.
   *
   * @param instanceURI is the KijiURI of the instance to get a security manager for.
   * @return the security manager for the instance.
   */
  def getSecurityManager(instanceURI: KijiURI): KijiSecurityManager

  /**
   * @return a list of all available Kiji instances.
   */
  def listInstances(): Set[String]

  /**
   * Close all Kiji-related resources opened by this object.
   */
  def shutdown(): Unit
}
