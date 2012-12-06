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

import java.util.{List => JList}
import java.util.{TreeMap => JTreeMap}

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.TableNotDisabledException
import org.apache.hadoop.hbase.TableExistsException
import net.sf.cglib.proxy.MethodInterceptor
import net.sf.cglib.proxy.MethodProxy

// -------------------------------------------------------------------------------------------------


/** Fake HBase instance, as a collection of fake HTable instances. */
class FakeHBase {
  type Bytes = Array[Byte]

  /** Controls whether to automatically create unknown tables or throw a TableNotFoundException. */
  // TODO Make this configurable
  private val CreateUnknownTable = true

  /** Map of the tables. */
  private[fakehtable] val tableMap = new JTreeMap[Bytes, FakeHTable](Bytes.BYTES_COMPARATOR)

  // -----------------------------------------------------------------------------------------------

  /** Factory for HTableInterface instances. */
  object InterfaceFactory
      extends org.kiji.testing.fakehtable.HTableInterfaceFactory
      with org.apache.hadoop.hbase.client.HTableInterfaceFactory {

    override def create(conf: Configuration, tableName: String): HTableInterface = {
      val tableNameBytes = Bytes.toBytes(tableName)
      synchronized {
        var table = tableMap.get(tableNameBytes)
        if (table == null) {
          if (!CreateUnknownTable) {
            throw new TableNotFoundException(tableName)
          }
          val desc = new HTableDescriptor(tableName)
          table = new FakeHTable(name = tableName, conf = conf, desc = desc)
          tableMap.put(tableNameBytes, table)
        }
        return table
      }
    }

    override def createHTableInterface(
        conf: Configuration,
        tableName: Bytes
    ): HTableInterface = {
      return create(tableName = Bytes.toString(tableName), conf = conf)
    }

    override def releaseHTableInterface(table: HTableInterface): Unit = {
      // Do nothing
    }
  }

  // -----------------------------------------------------------------------------------------------

  object Admin extends HBaseAdminCore with HBaseAdminConversionHelpers {
    def addColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
        // TODO(taton) Implement metadata
      // For now, do nothing
    }

    def createTable(desc: HTableDescriptor, split: Array[Bytes]): Unit = {
      // TODO(taton) Implement split
      synchronized {
        if (tableMap.containsKey(desc.getName)) {
          throw new TableExistsException(desc.getNameAsString)
        }
        val table = new FakeHTable(
            name = desc.getNameAsString,
            conf = null,
            desc = desc
        )
        tableMap.put(desc.getName, table)
      }
    }

    def createTable(
        desc: HTableDescriptor,
        startKey: Bytes,
        endKey: Bytes,
        numRegions: Int
    ): Unit = {
      // TODO(taton) Implement split
      createTable(desc = desc, split = null)
    }

    def deleteColumn(tableName: Bytes, columnName: Bytes): Unit = {
      // TODO(taton) Implement metadata
      // For now, do nothing
    }

    def deleteTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        if (table.enabled) {
          throw new TableNotDisabledException(tableName)
        }
        tableMap.remove(tableName)
      }
    }

    def disableTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        table.enabled = false
      }
    }

    def enableTable(tableName: Bytes): Unit = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        table.enabled = true
      }
    }

    def flush(tableName: Bytes): Unit = {
      // Nothing to do
    }

    def getTableRegions(tableName: Bytes): JList[HRegionInfo] = {
      synchronized {
        val list = new java.util.ArrayList[HRegionInfo]()
        val region = new HRegionInfo(tableName)
        list.add(region)
        return list
      }
    }

    def isTableAvailable(tableName: Bytes): Boolean = {
      return isTableEnabled(tableName)
    }

    def isTableEnabled(tableName: Bytes): Boolean = {
      synchronized {
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        return table.enabled
      }
    }

    def listTables(): Array[HTableDescriptor] = {
      synchronized {
        return tableMap.values.iterator.asScala
            .map { table => table.desc }
            .toArray
      }
    }

    def modifyColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def modifyTable(tableName: String, desc: HTableDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def tableExists(tableName: Bytes): Boolean = {
      synchronized {
        return tableMap.containsKey(tableName)
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Factory for HBaseAdmin instances. */
  object AdminFactory {
    /** Creates a new HBaseAdmin for this HBase instance. */
    def create(): HBaseAdmin = {
      return Proxy.create(classOf[HBaseAdmin], new PythonProxy(Admin))
    }
  }

}
