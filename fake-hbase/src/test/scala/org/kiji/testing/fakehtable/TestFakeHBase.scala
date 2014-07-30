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

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.junit.Assert
import org.junit.Test

/** Tests for the FakeHBase class. */
class TestFakeHBase {

  /** Test the basic API of FakeHBase. */
  @Test
  def testFakeHBase(): Unit = {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table-name")
    hbase.Admin.createTable(desc)

    val tables = hbase.Admin.listTables()
    Assert.assertEquals(1, tables.length)
    Assert.assertEquals("table-name", tables(0).getNameAsString())
  }

  /** Test the fake implementation of HBaseAdmin.getTableRegions(). */
  @Test
  def testSimpleRegionSplit(): Unit = {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table-name")
    hbase.Admin.createTable(desc, null, null, numRegions = 2)

    val regions = hbase.Admin.getTableRegions("table-name".getBytes).asScala
    Assert.assertEquals(2, regions.size)
    assert(regions.head.getStartKey.isEmpty)
    assert(regions.last.getEndKey.isEmpty)
    for (i <- 0 until regions.size - 1) {
      Assert.assertEquals(
          regions(i).getEndKey.toSeq,
          regions(i + 1).getStartKey.toSeq)
    }
    Assert.assertEquals(
        "7fffffffffffffffffffffffffffffff",
        Hex.encodeHexString(regions(0).getEndKey))
  }

  /** Tests that FakeHTable instances appear as valid instances of HTable. */
  @Test
  def testFakeHTableAsInstanceOfHTable(): Unit = {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table")
    hbase.Admin.createTable(desc)
    val conf = HBaseConfiguration.create()
    val htable: HTable = hbase.InterfaceFactory.create(conf, "table").asInstanceOf[HTable]
    val locations = htable.getRegionLocations()
    Assert.assertEquals(1, locations.size)
    val location = htable.getRegionLocation("row key")
    Assert.assertEquals(locations.keySet.iterator.next, location.getRegionInfo)
  }

  @Test
  def testAdminFactory(): Unit = {
    val hbase = new FakeHBase()
    val conf = HBaseConfiguration.create()
    val admin = hbase.AdminFactory.create(conf)
    admin.close()
  }
}
