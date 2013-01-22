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

import java.util.Arrays

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestFakeHTable extends FunSuite {

  test("HTable.get() on unknown row") {
    val table = new FakeHTable(name = "table", desc = null)
    expect(true)(table.get(new Get("key".getBytes)).isEmpty)
  }

  test("HTable.scan() on empty table") {
    val table = new FakeHTable(name = "table", desc = null)
    expect(null)(table.getScanner("family".getBytes).next)
  }

  test("HTable.delete() on unknown row") {
    val table = new FakeHTable(name = "table", desc = null)
    table.delete(new Delete("key".getBytes))
  }

  test("HTable.put(row) then HTable.get(row)") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.put(row) then HTable.scan(family)") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.put(row) then HTable.delete(row)") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 12345L, "value".getBytes))

    table.delete(new Delete("key".getBytes))
    expect(true)(table.get(new Get("key".getBytes)).isEmpty)
  }

  test("HTable.incrementeColumnValue()") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.delete() on specific cell") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.delete() on most recent cell") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.delete() on older versions of qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.delete() all versions of qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
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

  test("HTable.delete() with family/qualifier/time-series cleanups") {
    val table = new FakeHTable(name = "table", desc = null)

    // Populate one row with 4 families, each with 4 qualifiers, each with 4 versions:
    val count = 4
    val rowKey = "key1".getBytes
    populateTable(table, count = count)

    // Delete all versions of family1:qualifier1 one by one, and check:
    for (timestamp <- 0 until count) {
      table.delete(new Delete(rowKey)
          .deleteColumn("family1".getBytes, "qualifier1".getBytes, timestamp))
    }
    {
      val scanner = table.getScanner(new Scan(rowKey, nextRow(rowKey))
          .setMaxVersions(Int.MaxValue)
          .addColumn("family1".getBytes, "qualifier1".getBytes))
      expect(null)(scanner.next())
    }

    // Delete all qualifiers in family2 and check:
    for (cId <- 0 until count) {
      table.delete(new Delete(rowKey)
          .deleteColumns("family2".getBytes, "qualifier%d".format(cId).getBytes))
    }
    {
      val scanner = table.getScanner(new Scan(rowKey, nextRow(rowKey))
          .setMaxVersions(Int.MaxValue)
          .addFamily("family2".getBytes))
      expect(null)(scanner.next())
    }
  }

  test("ResultScanner.hasNext() on empty table") {
    val table = new FakeHTable(name = "table", desc = null)
    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("ResultScanner.hasNext()") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key".getBytes)
        .add("family".getBytes, "qualifier".getBytes, 1L, "value1".getBytes))

    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    expect(true)(iterator.hasNext())
    assert(iterator.next() != null)
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("ResultScanner.hasNext() with qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key1".getBytes)
        .add("family".getBytes, "qualifier1".getBytes, 1L, "value1".getBytes))
    table.put(new Put("key2".getBytes)
        .add("family".getBytes, "qualifier2".getBytes, 1L, "value1".getBytes))

    val scanner = table.getScanner(new Scan()
        .addColumn("family".getBytes, "qualifier1".getBytes))
    val iterator = scanner.iterator()
    expect(true)(iterator.hasNext())
    assert(iterator.next() != null)
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("HTable.get() with filter") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )

    val count = 2
    populateTable(table, count=count)

    val get = new Get("key1".getBytes)
        .setMaxVersions()
        .setFilter(new KeyOnlyFilter)
    val result = table.get(get)
    expect(count)(result.getMap.size)
    for ((family, qmap) <- result.getMap.asScala) {
      expect(count)(qmap.size)
      for ((qualifier, tseries) <- qmap.asScala) {
        expect(count)(tseries.size)
        for ((timestamp, value) <- tseries.asScala) {
          assert(value.isEmpty)
        }
      }
    }
  }

  test("HTable.get() with max versions") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )

    val count = 4
    populateTable(table, count=count)

    // We generated 4 versions, but request only 2:
    val maxVersions = 2
    val get = new Get("key1".getBytes)
        .setMaxVersions(maxVersions)
        .setFilter(new KeyOnlyFilter)
    val result = table.get(get)
    expect(count)(result.getMap.size)
    for ((family, qmap) <- result.getMap.asScala) {
      expect(count)(qmap.size)
      for ((qualifier, tseries) <- qmap.asScala) {
        expect(maxVersions)(tseries.size)
        assert(tseries.containsKey(2L))
        assert(tseries.containsKey(3L))
      }
    }
  }

  /**
   * Returns the smallest row key strictly greater than the specified row.
   *
   * @param row HBase row key.
   * @return the smallest row strictly greater than the specified row.
   */
  private def nextRow(row: Array[Byte]): Array[Byte] = {
    return Arrays.copyOfRange(row, 0, row.size + 1)
  }

  /**
   * Populates a given table with some data.
   *
   * @param table HBase table to fill in.
   * @param count Number of rows, families, columns and versions to write.
   */
  private def populateTable(
      table: HTableInterface,
      count: Int = 4
  ): Unit = {
    for (rId <- 0 until count) {
      val rowKey = "key%d".format(rId).getBytes
      for (fId <- 0 until count) {
        val family = "family%d".format(fId).getBytes
        for (cId <- 0 until count) {
          val qualifier = "qualifier%d".format(cId).getBytes
          for (timestamp <- 0L until count) {
            table.put(new Put(rowKey)
                .add(family, qualifier, timestamp, "value%d".format(timestamp).getBytes))
          }
        }
      }
    }
  }
}