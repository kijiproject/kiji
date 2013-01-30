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
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable

@RunWith(classOf[JUnitRunner])
class TestFakeHBase extends FunSuite {

  test("FakeHBase") {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table-name")
    hbase.Admin.createTable(desc)

    val tables = hbase.Admin.listTables()
    expect(1)(tables.length)
    expect("table-name")(tables(0).getNameAsString())
  }

  test("Simple region split") {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table-name")
    hbase.Admin.createTable(desc, null, null, numRegions = 2)

    val regions = hbase.Admin.getTableRegions("table-name".getBytes).asScala
    expect(2)(regions.size)
    assert(regions.head.getStartKey.isEmpty)
    assert(regions.last.getEndKey.isEmpty)
    for (i <- 0 until regions.size - 1) {
      expect(regions(i).getEndKey.toSeq)(regions(i + 1).getStartKey.toSeq)
    }
    expect("7fffffffffffffffffffffffffffffff")(Hex.encodeHexString(regions(0).getEndKey))
  }

  test("FakeHTable.asInstanceOf[HTable]") {
    val hbase = new FakeHBase()
    val desc = new HTableDescriptor("table")
    hbase.Admin.createTable(desc)
    val conf = HBaseConfiguration.create()
    val htable = hbase.InterfaceFactory.create(conf, "table").asInstanceOf[HTable]
    val locations = htable.getRegionLocations()
    expect(1)(locations.size)
    val location = htable.getRegionLocation("row key")
    expect(locations.keySet.iterator.next)(location.getRegionInfo)
  }
}
