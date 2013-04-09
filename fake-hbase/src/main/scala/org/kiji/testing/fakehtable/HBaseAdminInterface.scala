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

package org.kiji.testing.fakehtable

import java.io.Closeable
import java.lang.{Integer => JInteger}
import java.util.regex.Pattern
import java.util.Arrays
import java.util.{List => JList}

import scala.collection.mutable
import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Pair
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableNotFoundException


/** Core HBaseAdmin interface. */
trait HBaseAdminCore
    extends Closeable {

  type Bytes = Array[Byte]

  def addColumn(tableName: Bytes, column: HColumnDescriptor): Unit

  def createTable(desc: HTableDescriptor, split: Array[Bytes]): Unit
  def createTable(
      desc: HTableDescriptor,
      startKey: Bytes,
      endKey: Bytes,
      numRegions: Int
  ): Unit

  def deleteColumn(tableName: Bytes, columnName: Bytes): Unit

  def deleteTable(tableName: Bytes): Unit

  def disableTable(tableName: Bytes): Unit

  def enableTable(tableName: Bytes): Unit

  def flush(tableName: Bytes): Unit

  def getTableRegions(tableName: Bytes): JList[HRegionInfo]

  def isTableAvailable(tableName: Bytes): Boolean

  def isTableEnabled(tableName: Bytes): Boolean

  def listTables(): Array[HTableDescriptor]

  def modifyColumn(tableName: Bytes, column: HColumnDescriptor): Unit

  def modifyTable(tableName: Bytes, desc: HTableDescriptor): Unit

  def getAlterStatus(tableName: Bytes): Pair[JInteger, JInteger]

  def tableExists(tableName: Bytes): Boolean
}


/**
 * Pure interface for compatibility with the concrete Java class HBaseAdmin.
 *
 * Extends HBaseAdminCore with helpers to convert between Bytes and String.
 */
trait HBaseAdminInterface
    extends HBaseAdminCore {

  def addColumn(tableName: String, column: HColumnDescriptor): Unit
  def addColumn(tableName: Bytes, column: HColumnDescriptor): Unit

  def createTable(desc: HTableDescriptor): Unit
  def createTable(desc: HTableDescriptor, split: Array[Bytes]): Unit
  def createTable(
      desc: HTableDescriptor,
      startKey: Bytes, endKey: Bytes, numRegions: Int
  ): Unit
  def createTableAsync(desc: HTableDescriptor, split: Array[Bytes]): Unit

  def deleteColumn(tableName: String, columnName: String): Unit
  def deleteColumn(tableName: Bytes, columnName: Bytes): Unit

  def deleteTable(tableName: String): Unit
  def deleteTable(tableName: Bytes): Unit
  def deleteTables(regex: String): Unit
  def deleteTables(pattern: Pattern): Unit

  def disableTable(tableName: String): Unit
  def disableTable(tableName: Bytes): Unit
  def disableTableAsync(tableName: String): Unit
  def disableTableAsync(tableName: Bytes): Unit
  def disableTables(regex: String): Unit
  def disableTables(pattern: Pattern): Unit

  def enableTable(tableName: String): Unit
  def enableTable(tableName: Bytes): Unit
  def enableTableAsync(tableName: String): Unit
  def enableTableAsync(tableName: Bytes): Unit
  def enableTables(regex: String): Unit
  def enableTables(pattern: Pattern): Unit

  def flush(tableName: String): Unit
  def flush(tableName: Bytes): Unit

  // Similar to listTables()
  def getTableDescriptor(tableName: Bytes): HTableDescriptor
  def getTableDescriptors(tableNames: JList[String]): Array[HTableDescriptor]

  def getTableRegions(tableName: Bytes): JList[HRegionInfo]

  def isTableAvailable(tableName: String): Boolean
  def isTableAvailable(tableName: Bytes): Boolean

  def isTableDisabled(tableName: String): Boolean
  def isTableDisabled(tableName: Bytes): Boolean

  def isTableEnabled(tableName: String): Boolean
  def isTableEnabled(tableName: Bytes): Boolean

  def listTables(): Array[HTableDescriptor]
  def listTables(regex: String): Array[HTableDescriptor]
  def listTables(pattern: Pattern): Array[HTableDescriptor]

  def modifyColumn(tableName: String, column: HColumnDescriptor): Unit
  def modifyColumn(tableName: Bytes, column: HColumnDescriptor): Unit

  def modifyTable(tableName: Bytes, desc: HTableDescriptor): Unit
  def getAlterStatus(tableName: Bytes): Pair[JInteger, JInteger]

  def tableExists(tableName: String): Boolean
  def tableExists(tableName: Bytes): Boolean
}


/** Implements conversion helpers (Bytes/String, Regex/Pattern, etc). */
trait HBaseAdminConversionHelpers extends HBaseAdminInterface {
  override def addColumn(tableName: String, column: HColumnDescriptor): Unit = {
    addColumn(toBytes(tableName), column)
  }

  override def close(): Unit = {
    // Do nothing
  }

  override def createTable(desc: HTableDescriptor): Unit = {
    createTable(desc, split = Array())
  }

  override def createTableAsync(desc: HTableDescriptor, split: Array[Bytes]): Unit = {
    createTable(desc, split)
  }

  override def deleteColumn(tableName: String, columnName: String): Unit = {
    deleteColumn(toBytes(tableName), toBytes(columnName))
  }

  override def deleteTable(tableName: String): Unit = {
    deleteTable(toBytes(tableName))
  }

  override def deleteTables(regex: String): Unit = {
    deleteTables(Pattern.compile(regex))
  }
  override def deleteTables(pattern: Pattern): Unit = {
    for (desc <- listTables()) {
      if (pattern.matcher(desc.getNameAsString).matches) {
        deleteTable(desc.getName)
      }
    }
  }

  override def disableTable(tableName: String): Unit = {
    disableTable(toBytes(tableName))
  }
  override def disableTableAsync(tableName: String): Unit = {
    disableTableAsync(toBytes(tableName))
  }
  override def disableTableAsync(tableName: Bytes): Unit = {
    disableTable(tableName)
  }
  override def disableTables(regex: String): Unit = {
    disableTables(Pattern.compile(regex))
  }
  override def disableTables(pattern: Pattern): Unit = {
    for (desc <- listTables()) {
      if (pattern.matcher(desc.getNameAsString).matches) {
        disableTable(desc.getName)
      }
    }
  }

  override def enableTable(tableName: String): Unit = {
    enableTable(toBytes(tableName))
  }
  override def enableTableAsync(tableName: String): Unit = {
    enableTableAsync(toBytes(tableName))
  }
  override def enableTableAsync(tableName: Bytes): Unit = {
    enableTable(tableName)
  }
  override def enableTables(regex: String): Unit = {
    enableTables(Pattern.compile(regex))
  }
  override def enableTables(pattern: Pattern): Unit = {
    for (desc <- listTables()) {
      if (pattern.matcher(desc.getNameAsString).matches) {
        enableTable(desc.getName)
      }
    }
  }

  override def flush(tableName: String): Unit = {
    flush(toBytes(tableName))
  }

  override def getTableDescriptor(tableName: Bytes): HTableDescriptor = {
    for (desc <- listTables()) {
      if (Arrays.equals(desc.getName, tableName)) {
        return desc
      }
    }
    throw new TableNotFoundException(Bytes.toStringBinary(tableName))
  }
  override def getTableDescriptors(tableNames: JList[String]): Array[HTableDescriptor] = {
    val descs = mutable.Buffer[HTableDescriptor]()
    val names = Set() ++ tableNames.asScala
    for (desc <- listTables()) {
      if (names.contains(desc.getNameAsString)) {
        descs += desc
      }
    }
    return descs.toArray
  }

  override def isTableAvailable(tableName: String): Boolean = {
    return isTableAvailable(toBytes(tableName))
  }

  override def isTableDisabled(tableName: String): Boolean = {
    return isTableDisabled(toBytes(tableName))
  }
  override def isTableDisabled(tableName: Bytes): Boolean = {
    return !isTableEnabled(tableName)
  }

  override def isTableEnabled(tableName: String): Boolean = {
    return isTableEnabled(toBytes(tableName))
  }

  override def listTables(regex: String): Array[HTableDescriptor] = {
    return listTables(Pattern.compile(regex))
  }
  override def listTables(pattern: Pattern): Array[HTableDescriptor] = {
    val descs = mutable.Buffer[HTableDescriptor]()
    for (desc <- listTables()) {
      if (pattern.matcher(desc.getNameAsString).matches) {
        descs += desc
      }
    }
    return descs.toArray
  }

  override def modifyColumn(tableName: String, column: HColumnDescriptor): Unit = {
    modifyColumn(toBytes(tableName), column)
  }

  override def tableExists(tableName: String): Boolean = {
    return tableExists(toBytes(tableName))
  }
}
