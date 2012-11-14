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

import scala.collection.mutable.Map
import java.util.NoSuchElementException

import org.kiji.schema.Kiji
import org.kiji.schema.KijiConfiguration

import org.kiji.schema.KijiAdmin
import org.kiji.schema.KijiConfiguration
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.avro.TableLayoutDesc

/**
 * A KijiSystem class that provides in-memory mappings from instance -&gt; table &gt; layout,
 * and does not communicate with HBase.
 */
class MockKijiSystem extends AbstractKijiSystem {
  private val instanceData: Map[String, Map[String, KijiTableLayout]] = Map()

  {
    // Create 3 empty instances.
    instanceData(KijiConfiguration.DEFAULT_INSTANCE_NAME) = Map[String, KijiTableLayout]()
    instanceData("foo") = Map[String, KijiTableLayout]()
    instanceData("bar") = Map[String, KijiTableLayout]()
  }

  override def getTableNamesDescriptions(instance: String): Array[(String, String)] = {
    try {
      val tables: Map[String, KijiTableLayout] = instanceData(instance)
      val out: List[(String, String)] = tables.keys.foldLeft(Nil:List[(String,String)])(
          (lst, tableName) =>
              (tableName, tables(tableName).getDesc.getDescription().toString()) :: lst
      )
      return out.toArray
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("No such instance: " + instance)
      }
    }
  }

  override def getTableLayout(instance: String, table: String): Option[KijiTableLayout] = {
    try {
      Some(new KijiTableLayout(TableLayoutDesc.newBuilder(
          instanceData(instance)(table).getDesc()).build(), null))
    } catch {
      case nsee: NoSuchElementException => None
    }
  }

  override def createTable(instance: String, table: String, layout: KijiTableLayout): Unit = {
    // Verify that the layout has all the required values set.
    TableLayoutDesc.newBuilder(layout.getDesc()).build()
    try {
      instanceData(instance)(table)
      throw new RuntimeException("Table already exists")
    } catch {
      case nsee: NoSuchElementException => {
        try {
          val tableMap = instanceData(instance)
          tableMap(table) = layout
        } catch {
          case nsee: NoSuchElementException => {
              throw new RuntimeException("Instance does not exist")
          }
        }
      }
    }
  }

  override def applyLayout(instance: String, table: String, layout: TableLayoutDesc): Unit = {
    try {
      // Check that the table already exists.
      instanceData(instance)(table)
      // Verify that the layout has all the required values set.
      val layoutCopy = TableLayoutDesc.newBuilder(layout).build()
      // Verify that the update is legal.
      val wrappedLayout = new KijiTableLayout(layoutCopy, instanceData(instance)(table))
      // TODO: Process deletes manually.
      instanceData(instance)(table) = wrappedLayout // Update our copy to match.
    } catch {
      case nsee: NoSuchElementException => throw new RuntimeException(table + " doesn't exist!")
    }
  }

  override def dropTable(instance: String, table: String): Unit = {
    try {
      instanceData(instance)(table) // Verify it exists first.
      instanceData(instance).remove(table) // Then remove it.
    } catch {
      case nsee: NoSuchElementException => {
        throw new RuntimeException("missing instance or table")
      }
    }
  }

  override def listInstances(): Set[String] = {
    instanceData.keySet.foldLeft(Nil: List[String])((lst, instance) => instance :: lst).toSet
  }

  override def shutdown(): Unit = {
  }
}
