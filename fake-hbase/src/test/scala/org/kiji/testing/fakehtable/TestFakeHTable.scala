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

import scala.collection.JavaConverters._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HConstants

@RunWith(classOf[JUnitRunner])
class TestFakeHTable extends FunSuite {

  test("FakeHTable.get(unknownRow).isEmpty") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    expect(true)(table.get(new Get("key".getBytes)).isEmpty)
  }

  test("FakeHTable.scan(emptyTable)") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    expect(null)(table.getScanner("family".getBytes).next)
  }

  test("FakeHTable.delete(unknownRow)") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.delete(new Delete("key".getBytes))
  }

  test("FakeHTable.put(row), FakeHTable.get(row)") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 12345L, "value".getBytes))

    val result = table.get(new Get("key".getBytes))
    expect(false)(result.isEmpty)
    expect("key")(new String(result.getRow))
    expect("value")(new String(result.value))

    expect(1)(result.getMap.size)
    expect(1)(result.getMap.get("family".getBytes).size)
    expect(1)(result.getMap.get("family".getBytes).get("qualifier".getBytes).size)
    expect("value")(
        new String(result.getMap.get("family".getBytes).get("qualifier".getBytes).get(12345L)))
  }

  test("FakeHTable.put(row), FakeHTable.scan(family)") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 12345L, "value".getBytes))

    val scanner = table.getScanner("family".getBytes)
    val it = scanner.iterator
    expect(true)(it.hasNext)
    val result = it.next
    expect(false)(result.isEmpty)
    expect("key")(new String(result.getRow))
    expect("value")(new String(result.value))

    expect(1)(result.getMap.size)
    expect(1)(result.getMap.get("family".getBytes).size)
    expect(1)(result.getMap.get("family".getBytes).get("qualifier".getBytes).size)
    expect("value")(
        new String(result.getMap.get("family".getBytes).get("qualifier".getBytes).get(12345L)))

    expect(false)(it.hasNext)
  }

  test("FakeHTable.put(row), FakeHTable.delete(row)") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 12345L, "value".getBytes))

    table.delete(new Delete("key".getBytes))
    expect(true)(table.get(new Get("key".getBytes)).isEmpty)
  }

  test("FakeHTable.incrementeColumnValue") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    expect(1)(table.incrementColumnValue(
        row = "row".getBytes,
        family = "family".getBytes,
        qualifier = "qualifier".getBytes,
        amount = 1))
    expect(2)(table.incrementColumnValue(
        row = "row".getBytes,
        family = "family".getBytes,
        qualifier = "qualifier".getBytes,
        amount = 1))
    expect(3)(table.incrementColumnValue(
        row = "row".getBytes,
        family = "family".getBytes,
        qualifier = "qualifier".getBytes,
        amount = 1))
  }

  test("delete specific cell") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 1L, "value1".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 2L, "value2".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 3L, "value3".getBytes))
    table.delete(new Delete("key".getBytes)
        .deleteColumn("family".getBytes, "qualifier".getBytes, 2L))
    val scanner = table.getScanner(new Scan("key".getBytes)
        .setMaxVersions(Int.MaxValue)
        .addColumn("family".getBytes, "qualifier".getBytes))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family".getBytes, "qualifier".getBytes)
    expect(2)(cells.size())
    expect(3L)(cells.get(0).getTimestamp)
    expect(1L)(cells.get(1).getTimestamp)
  }

  test("delete most recent cell") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 1L, "value1".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 2L, "value2".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 3L, "value3".getBytes))
    table.delete(new Delete("key".getBytes)
        .deleteColumn("family".getBytes, "qualifier".getBytes))
    val scanner = table.getScanner(new Scan("key".getBytes)
        .setMaxVersions(Int.MaxValue)
        .addColumn("family".getBytes, "qualifier".getBytes))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family".getBytes, "qualifier".getBytes)
    expect(2)(cells.size())
    expect(2L)(cells.get(0).getTimestamp)
    expect(1L)(cells.get(1).getTimestamp)
  }

  test("delete older versions of qualifier") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 1L, "value1".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 2L, "value2".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 3L, "value3".getBytes))
    table.delete(new Delete("key".getBytes)
        .deleteColumns("family".getBytes, "qualifier".getBytes, 2L))
    val scanner = table.getScanner(new Scan("key".getBytes)
        .setMaxVersions(Int.MaxValue)
        .addColumn("family".getBytes, "qualifier".getBytes))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family".getBytes, "qualifier".getBytes)
    expect(1)(cells.size())
    expect(3L)(cells.get(0).getTimestamp)
  }

  test("delete all versions of qualifier") {
    val table = new FakeHTable(name = "table", conf = null, desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 1L, "value1".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 2L, "value2".getBytes))
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 3L, "value3".getBytes))
    table.delete(new Delete("key".getBytes)
        .deleteColumns("family".getBytes, "qualifier".getBytes))
    val scanner = table.getScanner(new Scan("key".getBytes)
        .setMaxVersions(Int.MaxValue)
        .addColumn("family".getBytes, "qualifier".getBytes))
    expect(null)(scanner.next())
  }

}