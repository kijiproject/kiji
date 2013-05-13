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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.util.StringUtils

import org.kiji.annotations.ApiAudience
import org.kiji.schema.Kiji
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ResourceUtils

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
          println(exn)
          None
        }
      }
    } else {
      Some(kijis(uri))
    }
  }

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  override def createTable(uri: KijiURI, layout: KijiTableLayout, numRegions: Int): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.createTable(layout.getDesc(), numRegions) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
    }
  }

  /** {@inheritDoc} */
  override def applyLayout(uri: KijiURI, table: String, layout: TableLayoutDesc): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.modifyTableLayout(layout, false, Console.out) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
    }
  }

  /** {@inheritDoc} */
  override def dropTable(uri: KijiURI, table: String): Unit = {
    kijiCache(uri) match {
      case Some(kiji) => { kiji.deleteTable(table) }
      case None => { throw new IOException("Cannot get kiji for \"" + uri.toString() + "\"") }
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
        ResourceUtils.closeOrLog(hBaseAdmin)
        maybeHBaseAdmin = None
      }
    }

    kijis.foreach { case (key, refCountable) =>
        ResourceUtils.releaseOrLog(refCountable) }
    kijis.clear
  }
}
