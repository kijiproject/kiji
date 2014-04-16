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
import org.kiji.schema.Kiji
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.KijiSchemaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.security.KijiSecurityManager
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ResourceUtils
import org.kiji.schema.util.VersionInfo

/**
 * Instances of this class provide the Kiji schema shell with access to KijiSchema.
 * Clients of this class should use it to obtain handles to Kiji resources.  Clients
 * should avoid closing any Kiji resources they obtain through this object.  Instead,
 * clients should use the {@link KijiSystem#shutdown} method to shutdown all
 * resources when done with interacting with Kiji.
 *
 * <p>Each thread should create its own KijiSystem instance.</p>
 */
@ApiAudience.Private
final class KijiSystem extends AbstractKijiSystem {
  // A map from Kiji instance names to internal implementations of Kiji instances.
  private val kijis = Map[KijiURI, Kiji]()

  // A lazily-initialized HBaseAdmin.
  private var maybeHBaseAdmin: Option[HBaseAdmin] = None

  /** Return an HBaseAdmin if we have one, initializing one if we don't. */
  private def hBaseAdmin: HBaseAdmin = {
    maybeHBaseAdmin match {
      case Some(admin) => admin
      case None => {
        val admin = new HBaseAdmin(HBaseConfiguration.create())
        maybeHBaseAdmin = Some(admin)
        admin
      }
    }
  }

  /**
   * Gets the meta table for the Kiji instance with the specified name.
   *
   * @param uri Name of the Kiji instance.
   * @return A meta table for the specified Kiji instance, or None if the specified instance
   *     cannot be opened.
   */
  private def kijiMetaTable(uri: KijiURI): Option[KijiMetaTable] = {
    kijiCache(uri) match {
      case Some(theKiji) => Some(theKiji.getMetaTable())
      case None => None
    }
  }

  override def getSystemVersion(uri: KijiURI): ProtocolVersion = {
    return VersionInfo.getClusterDataVersion(
        kijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri)))
  }

  override def getOrCreateSchemaId(uri: KijiURI, schema: Schema): Long = {
    val kiji: Kiji = kijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: KijiSchemaTable = kiji.getSchemaTable()
    val id = schemaTable.getOrCreateSchemaId(schema)
    schemaTable.flush()
    return id
  }

  override def getSchemaId(uri: KijiURI, schema: Schema): Option[Long] = {
    val kiji: Kiji = kijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: KijiSchemaTable = kiji.getSchemaTable()

    // Look up the schema entry. If none exists, return None. Otherwise return Some(theId).
    return Option(schemaTable.getSchemaEntry(schema)).map { _.getId}
  }

  override def getSchemaForId(uri: KijiURI, uid: Long): Option[Schema] = {
    val kiji: Kiji = kijiCache(uri).getOrElse(throw new DDLException("Could not open " + uri))
    val schemaTable: KijiSchemaTable = kiji.getSchemaTable()

    return Option(schemaTable.getSchema(uid))
  }

  override def getSchemaFor(uri: KijiURI, avroSchema: AvroSchema): Option[Schema] = {
    if (avroSchema.getJson != null) {
      val schema: Schema = new Schema.Parser().parse(avroSchema.getJson)
      return Some(schema)
    }
    return getSchemaForId(uri, avroSchema.getUid)
  }

  override def setMeta(uri: KijiURI, table: String, key: String, value: String): Unit = {
    val metaTable: KijiMetaTable = kijiMetaTable(uri).getOrElse(
        throw new IOException("Cannot get metatable for URI " + uri))
    metaTable.putValue(table, key, value.getBytes())
  }

  override def getMeta(uri: KijiURI, table: String, key: String): Option[String] = {
    val metaTable: KijiMetaTable = kijiMetaTable(uri).getOrElse(
        throw new IOException("Cannot get metatable for URI " + uri))
    try {
      val bytes: Array[Byte] = metaTable.getValue(table, key)
      return Some(new String(bytes, "UTF-8"))
    } catch {
      case ioe: IOException => return None // Key not found.
    }
  }

  override def getSecurityManager(uri: KijiURI): KijiSecurityManager = {
    kijiCache(uri) match {
      case Some(kiji) =>
        return kiji.getSecurityManager()
      case None =>
        throw new IOException("Cannot open kiji: %s".format(uri.toString))
    }
  }

  /**
   * Gets the Kiji instance implementation for the Kiji instance with the specified name.
   *
   * <p>This method caches the Kiji instances opened.</p>
   *
   * @param uri Name of the Kiji instance.
   * @return An Kiji for the Kiji instance with the specified name, or none if the
   *     instance specified cannot be opened.
   */
  private def kijiCache(uri: KijiURI): Option[Kiji] = {
    if (!kijis.contains(uri)) {
      try {
        val theKiji = Kiji.Factory.open(uri)
        kijis += (uri -> theKiji)
        Some(theKiji)
      } catch {
        case exn: Exception => {
          exn.printStackTrace()
          None
        }
      }
    } else {
      Some(kijis(uri))
    }
  }

  override def getTableNamesDescriptions(uri: KijiURI): Array[(String, String)] = {
    // Get all table names.
    val tableNames: List[String] = kijiCache(uri) match {
      case Some(kiji) => kiji.getTableNames().toList
      case None => List()
    }
    // join table names and descriptions.
    tableNames.map { name =>
      kijiMetaTable(uri) match {
        case Some(metaTable) => {
          val description = metaTable.getTableLayout(name).getDesc().getDescription()
          (name, description)
        }
        case None => (name, "")
      }
    }.toArray
  }

  override def getTableLayout(uri: KijiURI, table: String): Option[KijiTableLayout] = {
    kijiMetaTable(uri) match {
      case Some(metaTable) => {
        try {
          val layout = metaTable.getTableLayout(table)
          Some(layout)
        } catch {
          case _: IOException => None
        }
      }
      case None => None
    }
  }

  override def createTable(uri: KijiURI, layout: KijiTableLayout, numRegions: Int): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.createTable(layout.getDesc(), numRegions) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
    }
  }

  override def applyLayout(uri: KijiURI, table: String, layout: TableLayoutDesc): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.modifyTableLayout(layout, false, Console.out) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
    }
  }

  override def dropTable(uri: KijiURI, table: String): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.deleteTable(table) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
    }
  }

  override def listInstances(): Set[String] = {
    def parseInstanceName(kijiTableName: String): Option[String] = {
      val parts: Seq[String] = kijiTableName.split('.')
      if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts.head)) {
        None
      } else {
        Some(parts(1))
      }
    }

    val hTableDescriptors: List[HTableDescriptor] = hBaseAdmin.listTables().toList;
    val kijiInstanceNames: Set[String] = hTableDescriptors.foldLeft(Set():Set[String])({
      (names: Set[String], htableDesc) =>
        val instanceName: Option[String] = parseInstanceName(htableDesc.getNameAsString())
        instanceName match {
          case Some(instance) => { names + instance }
          case None => { names }
        }
    })
    return kijiInstanceNames
  }

  override def shutdown(): Unit = {
    maybeHBaseAdmin match {
      case None => { /* do nothing. */ }
      case Some(admin) => {
        // Close this.
        ResourceUtils.closeOrLog(hBaseAdmin)
        maybeHBaseAdmin = None
      }
    }

    kijis.foreach { case (key, refCountable) =>
        ResourceUtils.releaseOrLog(refCountable) }
    kijis.clear
  }
}
