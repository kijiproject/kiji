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

import java.lang.{Integer => JInteger}
import java.util.Arrays
import java.util.{List => JList}
import java.util.{TreeMap => JTreeMap}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.Buffer
import scala.math.BigInt.int2bigInt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableExistsException
import org.apache.hadoop.hbase.TableNotDisabledException
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Pair
import org.kiji.schema.impl.HBaseAdminFactory
import org.kiji.schema.impl.HBaseInterface

/**
 * Fake HBase instance, as a collection of fake HTable instances.
 *
 * FakeHBase/FakeHConnection act as factories for FakeHTable.
 * Conceptually, there is a single FakeHConnection per FakeHBase.
 */
class FakeHBase
    extends HBaseInterface
    with FakeTypes {

  /**
   * Controls whether to automatically create unknown tables or throw a TableNotFoundException.
   *
   * TODO: Drop this feature, it seems like a bad idea as unknown tables have unspecified
   *     properties (eg. max-versions, TTL, etc).
   */
  private var createUnknownTable: Boolean = false

  /** Default HConnection to connect to this HBase instance and the HTables it contains. */
  private val mFakeHConnection: FakeHConnection = new FakeHConnection(this)
  private val mHConnection: HConnection =
      UntypedProxy.create(classOf[HConnection], mFakeHConnection)

  /** Map of the FakeHTable in this FakeHBase instance, keyed on HTable name. */
  private[fakehtable] val tableMap = new JTreeMap[Bytes, FakeHTable](Bytes.BYTES_COMPARATOR)

  /**
   * Enables or disables the «create unknown table» feature.
   *
   * @param createUnknownTableFlag Whether unknown tables should be implicitly created.
   *     When disabled, TableNotFoundException is raised.
   */
  def setCreateUnknownTable(createUnknownTableFlag: Boolean): Unit = {
    synchronized {
      this.createUnknownTable = createUnknownTableFlag
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Factory for HTableInterface instances. */
  object InterfaceFactory
      extends org.kiji.schema.impl.HTableInterfaceFactory
      with org.apache.hadoop.hbase.client.HTableInterfaceFactory {

    override def create(conf: Configuration, tableName: String): HTableInterface = {
      val tableNameBytes = Bytes.toBytes(tableName)
      synchronized {
        var table = tableMap.get(tableNameBytes)
        if (table == null) {
          if (!createUnknownTable) {
            throw new TableNotFoundException(tableName)
          }
          val desc = new HTableDescriptor(tableName)
          table = new FakeHTable(
              name = tableName,
              conf = conf,
              desc = desc,
              hconnection = mFakeHConnection
          )
          tableMap.put(tableNameBytes, table)
        }
        return UntypedProxy.create(classOf[HTable], table)
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

  override def getHTableFactory(): org.kiji.schema.impl.HTableInterfaceFactory = InterfaceFactory

  // -----------------------------------------------------------------------------------------------

  object Admin extends HBaseAdminCore with HBaseAdminConversionHelpers {
    def addColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
      // TODO(taton) Implement metadata
      // For now, do nothing
    }

    def createTable(desc: HTableDescriptor, split: Array[Bytes]): Unit = {
      synchronized {
        if (tableMap.containsKey(desc.getName)) {
          throw new TableExistsException(desc.getNameAsString)
        }
        val table = new FakeHTable(
            name = desc.getNameAsString,
            desc = desc,
            hconnection = mFakeHConnection
        )
        Arrays.sort(split, Bytes.BYTES_COMPARATOR)
        table.setSplit(split)
        tableMap.put(desc.getName, table)
      }
    }

    def createTable(
        desc: HTableDescriptor,
        startKey: Bytes,
        endKey: Bytes,
        numRegions: Int
    ): Unit = {
      // TODO Handle startKey/endKey
      val split = Buffer[Bytes]()
      val min = 0
      val max: BigInt = (BigInt(1) << 128) - 1
      for (n <- 1 until numRegions) {
        val boundary: Bytes = MD5Space(n, numRegions)
        split.append(boundary)
      }
      createTable(desc = desc, split = split.toArray)
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
        val table = tableMap.get(tableName)
        if (table == null) {
          throw new TableNotFoundException(Bytes.toStringBinary(tableName))
        }
        return table.getRegions()
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
            .map { table => table.getTableDescriptor }
            .toArray
      }
    }

    def modifyColumn(tableName: Bytes, column: HColumnDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def modifyTable(tableName: Bytes, desc: HTableDescriptor): Unit = {
      // TODO(taton) Implement metadata
    }

    def getAlterStatus(tableName: Bytes): Pair[JInteger, JInteger] = {
      return new Pair(0, getTableRegions(tableName).size)
    }

    def tableExists(tableName: Bytes): Boolean = {
      synchronized {
        return tableMap.containsKey(tableName)
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Factory for HBaseAdmin instances. */
  object AdminFactory extends HBaseAdminFactory {
    /** Creates a new HBaseAdmin for this HBase instance. */
    override def create(conf: Configuration): HBaseAdmin = {
      return UntypedProxy.create(classOf[HBaseAdmin], Admin)
    }
  }

  override def getAdminFactory(): HBaseAdminFactory = {
    AdminFactory
  }

  /**
   * Returns an HConnection for this fake HBase instance.
   *
   * @returns an HConnection for this fake HBase instance.
   */
  def getHConnection(): HConnection = {
    return mHConnection
  }
}
