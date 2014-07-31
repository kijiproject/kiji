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

import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.TableName
import java.util.concurrent.ExecutorService
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * A fake implementation of HConnection, useful only for support FakeHTable's usage in pools.
 * Implements calls to change its closeable state.
 *
 * In HBase 0.96+, HConnection becomes the interface through which HTableInterface are created.
 *
 * @param fakeHBase FakeHBase instance this connection is for.
 * @param conf Optional explicit HBase configuration for this HConnection.
 */
class FakeHConnection(
    fakeHBase: FakeHBase,
    conf: Configuration = HBaseConfiguration.create()
) extends FakeTypes {
    // implements HConnection (but partially)

  private val mFakeHBase: FakeHBase = fakeHBase
  private val mConf: Configuration = conf

  var closed: Boolean = false

  def close() {
    closed = true
  }

  def isClosed(): Boolean = closed

  // -----------------------------------------------------------------------------------------------
  // getTable() and aliases:

  def getTable(name: Bytes): HTableInterface = {
    getTable(TableName.valueOf(name), null)
  }

  def getTable(name: Bytes, pool: ExecutorService): HTableInterface = {
    getTable(TableName.valueOf(name), pool)
  }

  def getTable(name: String): HTableInterface = {
    getTable(TableName.valueOf(name), null)
  }

  def getTable(name: String, pool: ExecutorService): HTableInterface = {
    getTable(TableName.valueOf(name), pool)
  }

  def getTable(name: TableName): HTableInterface = {
    getTable(name, null)
  }

  def getTable(name: TableName, pool: ExecutorService): HTableInterface = {
    val fullName = name.getNameAsString()  // namespace:table-name
    mFakeHBase.InterfaceFactory.create(mConf, fullName)
  }
}
