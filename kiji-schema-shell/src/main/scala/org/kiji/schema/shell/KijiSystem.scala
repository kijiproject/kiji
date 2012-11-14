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

import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.util.StringUtils

import org.kiji.schema.Kiji
import org.kiji.schema.KijiConfiguration

import org.kiji.schema.KijiAdmin
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout


/**
 * This object serves to provide Kiji schema shell with access to KijiSchema.
 * Clients of this object should use it to obtain handles to Kiji resources.  Clients
 * should avoid closing any Kiji resources they obtain through this object.  Instead,
 * clients should use the {@link KijiSystem#shutdown} method to shutdown all
 * resources when done with interacting with Kiji.
 */
object KijiSystem extends AbstractKijiSystem {
  // A map from Kiji instance names to internal implementations of Kiji instances.
  private val kijiCache = Map[String, Kiji]()
  // A map from Kiji instance names to Kiji admins for those instances.
  private val kijiAdminCache = Map[String, KijiAdmin]()

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
   * @param instance Name of the Kiji instance.
   * @return A meta table for the specified Kiji instance, or None if the specified instance
   *     cannot be opened.
   */
  private def kijiMetaTable(instance: String): Option[KijiMetaTable] = {
    kiji(instance) match {
      case Some(theKiji) => Some(theKiji.getMetaTable())
      case None => None
    }
  }

  /**
   * Gets the Kiji admin for the Kiji instance with the specified name.
   *
   * <p>This method caches admin objects.</p>
   *
   * @param instance Name of the Kiji instance.
   * @return A Kiji admin object for the specified Kiji instance, or None if the specified
   *     instance cannot be opened.
   */
  private def kijiAdmin(instance: String): Option[KijiAdmin] = {
    if (!kijiAdminCache.contains(instance)) {
      kiji(instance) match {
        case Some(theKiji) => {
          val admin = new KijiAdmin(hBaseAdmin, theKiji)
          kijiAdminCache += (instance -> admin)
          Some(admin)
        }
        case None => None
      }
    } else {
      Some(kijiAdminCache(instance))
    }
  }

  /**
   * Gets the Kiji configuration for the specified Kiji instance.
   *
   * <p>This method caches configuration instances for a given Kiji instance.</p>
   *
   * @param instance Name of the Kiji instance.
   * @return A configuration for the specified Kiji instance.
   */
  private def kijiConf(instance: String): KijiConfiguration = {
    new KijiConfiguration(HBaseConfiguration.create(), instance)
  }

  /**
   * Gets the Kiji instance implementation for the Kiji instance with the specified name.
   *
   * <p>This method caches the Kiji instances opened.</p>
   *
   * @param instance Name of the Kiji instance.
   * @return An Kiji for the Kiji instance with the specified name, or none if the
   *     instance specified cannot be opened.
   */
  private def kiji(instance: String): Option[Kiji] = {
    if (!kijiCache.contains(instance)) {
      try {
        val theKiji = Kiji.open(kijiConf(instance))
        kijiCache += (instance -> theKiji)
        Some(theKiji)
      } catch {
        case exn: Exception => {
          println(exn)
          None
        }
      }
    } else {
      Some(kijiCache(instance))
    }
  }

  /** {@inheritDoc} */
  override def getTableNamesDescriptions(instance: String): Array[(String, String)] = {
    // Get all table names.
    val tableNames: List[String] = kijiAdmin(instance) match {
      case Some(admin) => admin.getTableNames().toList
      case None => List()
    }
    // join table names and descriptions.
    tableNames.map { name =>
      kijiMetaTable(instance) match {
        case Some(metaTable) => {
          val description = metaTable.getTableLayout(name).getDesc().getDescription()
          (name, description)
        }
        case None => (name, "")
      }
    }.toArray
  }

  /** {@inheritDoc} */
  override def getTableLayout(instance: String, table: String): Option[KijiTableLayout] = {
    kijiMetaTable(instance) match {
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

  /** {@inheritDoc} */
  override def createTable(instance: String, table: String, layout: KijiTableLayout): Unit = {
    kijiAdmin(instance) match {
      case Some(admin) => { admin.createTable(table, layout, false) }
      case None => { throw new IOException("Cannot get instance admin for \"" + instance + "\"") }
    }
  }

  /** {@inheritDoc} */
  override def applyLayout(instance: String, table: String, layout: TableLayoutDesc): Unit = {
    kijiAdmin(instance) match {
      case Some(admin) => { admin.setTableLayout(table, layout, false, Console.out) }
      case None => { throw new IOException("Cannot get instance admin for \"" + instance + "\"") }
    }
  }

  /** {@inheritDoc} */
  override def dropTable(instance: String, table: String): Unit = {
    kijiAdmin(instance) match {
      case Some(admin) => { admin.deleteTable(table) }
      case None => { throw new IOException("Cannot get instance admin for \"" + instance + "\"") }
    }
  }

  /** {@inheritDoc} */
  override def listInstances(): Set[String] = {
    def parseInstanceName(kijiTableName: String): Option[String] = {
      val parts: List[String] = StringUtils.split(kijiTableName, '.').toList
      if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts.head)) {
        return None;
      }

      // Extract the second component from the list.
      val instanceName = parts.tail.head
      return Some(instanceName)
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

  /** {@inheritDoc} */
  override def shutdown(): Unit = {
    maybeHBaseAdmin match {
      case None => { /* do nothing. */ }
      case Some(admin) => {
        // Close this.
        IOUtils.closeQuietly(hBaseAdmin)
      }
    }

    kijiCache.foreach { case (key, closeable) => IOUtils.closeQuietly(closeable) }
  }
}
