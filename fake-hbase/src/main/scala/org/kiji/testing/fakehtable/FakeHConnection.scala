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

package org.kiji.testing.fakehtable

import org.apache.hadoop.hbase.client.{ServerCallable, Row, HConnection}
import java.util
import org.apache.hadoop.hbase.{HRegionLocation, HServerAddress, HRegionInfo, HTableDescriptor}
import org.apache.hadoop.hbase.ipc.{HMasterInterface, HRegionInterface, CoprocessorProtocol}
import java.util.concurrent.ExecutorService
import org.apache.hadoop.hbase.client.coprocessor.Batch.{Callback, Call}
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher
import org.apache.hadoop.conf.Configuration

/**
 * A fake implementation of HConnection, useful only for support FakeHTable's usage in pools.
 * Implements calls to change its closeable state.  Everything else is a no-op and should not be
 * used.
 */
class FakeHConnection extends HConnection {
  var closed: Boolean = false

  override def close() {
    closed = true
  }

  override def isClosed(): Boolean = closed

  // ---------- Unimplemented methods below here -----------
  def abort(why: String, e: Throwable) = throw new UnsupportedOperationException

  def clearCaches(sn: String) = throw new UnsupportedOperationException

  def prewarmRegionCache(tableName: Array[Byte], regions: util.Map[HRegionInfo, HServerAddress]) =
     throw new UnsupportedOperationException

  def setRegionCachePrefetch(tableName: Array[Byte], enable: Boolean) =
     throw new UnsupportedOperationException

  def processExecs[T <: CoprocessorProtocol, R](protocol: Class[T], rows: util.List[Array[Byte]], tableName: Array[Byte], pool: ExecutorService, call: Call[T, R], callback: Callback[R]) =
      throw new UnsupportedOperationException

  def processBatchCallback[R](list: util.List[_ <: Row], tableName: Array[Byte], pool: ExecutorService, results: Array[AnyRef], callback: Callback[R]) =
      throw new UnsupportedOperationException

  def processBatch(actions: util.List[_ <: Row], tableName: Array[Byte], pool: ExecutorService, results: Array[AnyRef]) =
     throw new UnsupportedOperationException

  def clearRegionCache(tableName: Array[Byte]) = throw new UnsupportedOperationException

  def clearRegionCache() = throw new UnsupportedOperationException

  def getConfiguration: Configuration = throw new UnsupportedOperationException

  def getZooKeeperWatcher: ZooKeeperWatcher = throw new UnsupportedOperationException

  def getMaster: HMasterInterface = throw new UnsupportedOperationException

  def isMasterRunning: Boolean = throw new UnsupportedOperationException

  def isTableEnabled(tableName: Array[Byte]): Boolean = throw new UnsupportedOperationException

  def isTableDisabled(tableName: Array[Byte]): Boolean = throw new UnsupportedOperationException

  def isTableAvailable(tableName: Array[Byte]): Boolean = throw new UnsupportedOperationException

  def listTables(): Array[HTableDescriptor] = throw new UnsupportedOperationException

  def getHTableDescriptor(tableName: Array[Byte]): HTableDescriptor =
      throw new UnsupportedOperationException

  def locateRegion(tableName: Array[Byte], row: Array[Byte]): HRegionLocation =
      throw new UnsupportedOperationException

  def relocateRegion(tableName: Array[Byte], row: Array[Byte]): HRegionLocation =
      throw new UnsupportedOperationException

  def locateRegion(regionName: Array[Byte]): HRegionLocation =
      throw new UnsupportedOperationException

  def locateRegions(tableName: Array[Byte]): util.List[HRegionLocation] =
      throw new UnsupportedOperationException

  def getHRegionConnection(regionServer: HServerAddress): HRegionInterface =
      throw new UnsupportedOperationException

  def getHRegionConnection(hostname: String, port: Int): HRegionInterface =
      throw new UnsupportedOperationException

  def getHRegionConnection(regionServer: HServerAddress, getMaster: Boolean): HRegionInterface =
     throw new UnsupportedOperationException

  def getHRegionConnection(hostname: String, port: Int, getMaster: Boolean): HRegionInterface =
     throw new UnsupportedOperationException

  def getRegionLocation(tableName: Array[Byte], row: Array[Byte], reload: Boolean): HRegionLocation =
      throw new UnsupportedOperationException

  def getRegionServerWithRetries[T](callable: ServerCallable[T]): T =
      throw new UnsupportedOperationException

  def getRegionServerWithoutRetries[T](callable: ServerCallable[T]): T =
      throw new UnsupportedOperationException


  def getRegionCachePrefetch(tableName: Array[Byte]): Boolean =
      throw new UnsupportedOperationException

  def getCurrentNrHRS: Int = 0

  def getHTableDescriptors(tableNames: util.List[String]): Array[HTableDescriptor] =
      throw new UnsupportedOperationException

  def isAborted: Boolean = false
}
