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

import org.apache.avro.Schema
import org.mockito.Mock
import org.scalatest.mock.EasyMockSugar

import org.kiji.schema.Kiji
import org.kiji.schema.KConstants
import org.kiji.schema.KijiMetaTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.VersionInfo
import org.kiji.schema.security.KijiSecurityManager

/**
 * A KijiSystem class that provides in-memory mappings from instance -&gt; table &gt; layout,
 * and does not communicate with HBase.
 */
class MockKijiSystem extends AbstractKijiSystem with EasyMockSugar {
  /** Mappings from KijiURI -> table name -> KijiTableLayout */
  private val instanceData: Map[KijiURI, Map[String, KijiTableLayout]] = Map()

  /** Mappings from KijiURI -> table name -> key -> value for the metatable. */
  private val metaData: Map[KijiURI, Map[String, Map[String, String]]] = Map()

  // A "schema table" holding schema <--> id bimappings.
  private val idsForSchemas: Map[Schema, Long] = Map()
  private val schemasForIds: Map[Long, Schema] = Map()
  private var nextSchemaId: Int = 0

  // A mock security manager.
  private val mockSecurityManager = mock[KijiSecurityManager]

  {
    val defaultURI = KijiURI.newBuilder().withInstanceName(KConstants.DEFAULT_INSTANCE_NAME).build()
    val fooURI = KijiURI.newBuilder().withInstanceName("foo").build()
    val barURI = KijiURI.newBuilder().withInstanceName("bar").build()

    // Create 3 empty instances.
    instanceData(defaultURI) = Map[String, KijiTableLayout]()
    instanceData(fooURI) = Map[String, KijiTableLayout]()
    instanceData(barURI) = Map[String, KijiTableLayout]()

    metaData(defaultURI) = Map[String, Map[String, String]]()
    metaData(fooURI) = Map[String, Map[String, String]]()
    metaData(barURI) = Map[String, Map[String, String]]()
  }

  override def getOrCreateSchemaId(uri: KijiURI, schema: Schema): Long = {
    if (!idsForSchemas.contains(schema)) {
      val id = nextSchemaId
      nextSchemaId += 1
      idsForSchemas(schema) = id
      schemasForIds(id) = schema
    }

    return idsForSchemas(schema)
  }

  override def getSchemaId(uri: KijiURI, schema: Schema): Option[Long] = {
    return idsForSchemas.get(schema)
  }

  override def getSchemaForId(uri: KijiURI, schemaId: Long): Option[Schema] = {
    return schemasForIds.get(schemaId)
  }

  override def getSchemaFor(uri: KijiURI, schema: AvroSchema): Option[Schema] = {
    if (schema.getJson != null) {
      return Some(new Schema.Parser().parse(schema.getJson))
    }
    return getSchemaForId(uri, schema.getUid)
  }

  override def getSystemVersion(uri: KijiURI): ProtocolVersion = {
    // Just return the max data version supported by this version of Kiji.
    return VersionInfo.getClientDataVersion()
  }

  override def setMeta(uri: KijiURI, table: String, key: String, value: String): Unit = {
    if (!metaData.contains(uri)) {
      metaData(uri) = Map[String, Map[String, String]]()
    }

    val instanceMap: Map[String, Map[String, String]] = metaData(uri)

    if (!instanceMap.contains(table)) {
      instanceMap(table) = Map[String, String]()
    }

    val tableMap = instanceMap(table)
    tableMap(key) = value
  }

  override def getMeta(uri: KijiURI, table: String, key: String): Option[String] = {
    try {
      Some(metaData(uri)(table)(key))
    } catch {
      case nsee: NoSuchElementException => { return None }
    }

  }

  override def getSecurityManager(uri: KijiURI): KijiSecurityManager = {
    // Return a mock security manager for testing purposes.
    return mockSecurityManager
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

  override def createTable(uri: KijiURI, layout: KijiTableLayout, numRegions: Int): Unit = {
    // Verify that the layout has all the required values set.
    TableLayoutDesc.newBuilder(layout.getDesc()).build()
    try {
      instanceData(uri)(layout.getName())
      throw new RuntimeException("Table already exists")
    } catch {
      case nsee: NoSuchElementException => {
        try {
          val tableMap = instanceData(uri)
          tableMap(layout.getName()) = layout
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
