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

import scala.collection.mutable.Map
import java.util.NoSuchElementException

import org.kiji.schema.Kiji
import org.kiji.schema.KConstants
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.avro.TableLayoutDesc

/**
 * A KijiSystem class that provides in-memory mappings from instance -&gt; table &gt; layout,
 * and does not communicate with HBase.
 */
class MockKijiSystem extends AbstractKijiSystem {
  private val instanceData: Map[KijiURI, Map[String, KijiTableLayout]] = Map()

  {
    val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    val fooURI = KijiURI.newBuilder().withInstanceName("foo").build()
    val barURI = KijiURI.newBuilder().withInstanceName("bar").build()

    // Create 3 empty instances.
    instanceData(defaultURI) = Map[String, KijiTableLayout]()
    instanceData(fooURI) = Map[String, KijiTableLayout]()
    instanceData(barURI) = Map[String, KijiTableLayout]()
  }

  override def getTableNamesDescriptions(uri: KijiURI): Array[(String, String)] = {
    try {
      val tables: Map[String, KijiTableLayout] = instanceData(uri)
      val out: List[(String, String)] = tables.keys.foldLeft(Nil:List[(String,String)])(
          (lst, tableName) =>
              (tableName, tables(tableName).getDesc.getDescription().toString()) :: lst
      )
      return out.toArray
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("No such instance: " + uri.toString)
      }
    }
  }

  override def getTableLayout(uri: KijiURI, table: String): Option[KijiTableLayout] = {
    try {
      Some(KijiTableLayout.newLayout(TableLayoutDesc.newBuilder(
          instanceData(uri)(table).getDesc()).build()))
    } catch {
      case nsee: NoSuchElementException => None
    }
  }

  override def createTable(uri: KijiURI, table: String, layout: KijiTableLayout): Unit = {
    // Verify that the layout has all the required values set.
    TableLayoutDesc.newBuilder(layout.getDesc()).build()
    try {
      instanceData(uri)(table)
      throw new RuntimeException("Table already exists")
    } catch {
      case nsee: NoSuchElementException => {
        try {
          val tableMap = instanceData(uri)
          tableMap(table) = layout
        } catch {
          case nsee: NoSuchElementException => {
              throw new RuntimeException("Instance does not exist")
          }
        }
      }
    }
  }

  override def applyLayout(uri: KijiURI, table: String, layout: TableLayoutDesc): Unit = {
    try {
      // Check that the table already exists.
      instanceData(uri)(table)
      // Verify that the layout has all the required values set.
      val layoutCopy = TableLayoutDesc.newBuilder(layout).build()
      // Verify that the update is legal.
      val wrappedLayout = KijiTableLayout.createUpdatedLayout(layoutCopy, instanceData(uri)(table))
      // TODO: Process deletes manually.
      instanceData(uri)(table) = wrappedLayout // Update our copy to match.
    } catch {
      case nsee: NoSuchElementException => throw new RuntimeException(table + " doesn't exist!")
    }
  }

  override def dropTable(uri: KijiURI, table: String): Unit = {
    try {
      instanceData(uri)(table) // Verify it exists first.
      instanceData(uri).remove(table) // Then remove it.
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("missing instance or table")
      }
    }
  }

  override def listInstances(): Set[String] = {
    instanceData
        .keySet
        .foldLeft(Nil: List[String])((lst, uri) => uri.getInstance() :: lst)
        .toSet
  }

  override def shutdown(): Unit = {
  }
}
