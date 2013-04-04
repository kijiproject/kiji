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
import scala.collection.JavaConverters._
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
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.ColumnRangeFilter

@RunWith(classOf[JUnitRunner])
class TestFakeHTable extends FunSuite {
  type Bytes = Array[Byte]

  /**
   * Implicitly converts string to UTF-8 bytes.
   *
   * @param string String to convert to bytes.
   * @return the string as UTF-8 encoded bytes.
   */
  implicit def stringToBytes(string: String): Bytes = {
    return Bytes.toBytes(string)
  }

  /**
   * Decodes UTF-8 encoded bytes to a string.
   *
   * @param bytes UTF-8 encoded bytes.
   * @return the decoded string.
   */
  def bytesToString(bytes: Bytes): String = {
    return Bytes.toString(bytes)
  }

  /**
   * Implicit string wrapper to add convenience encoding methods
   *
   * <p>
   *   Allows to write <code>"the string"b</code> to represent the UTF-8 bytes for "the string".
   * </p>
   *
   * @param str String to wrap.
   */
  class StringAsBytes(str: String) {
    /** Encodes the string to UTF-8 bytes. */
    def bytes(): Bytes = stringToBytes(str)

    /** Shortcut for bytes(). */
    def b = bytes()
  }

  implicit def implicitToStringAsBytes(str: String): StringAsBytes = {
    return new StringAsBytes(str)
  }

  test("HTable.get on unknown row") {
    val table = new FakeHTable(name = "table", desc = null)
    expect(true)(table.get(new Get("key")).isEmpty)
  }

  test("HTable.scan on empty table") {
    val table = new FakeHTable(name = "table", desc = null)
    expect(null)(table.getScanner("family").next)
  }

  test("HTable.delete on unknown row") {
    val table = new FakeHTable(name = "table", desc = null)
    table.delete(new Delete("key"))
  }

  test("HTable.put row then HTable.get row") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    val result = table.get(new Get("key"))
    expect(false)(result.isEmpty)
    expect("key")(Bytes.toString(result.getRow))
    expect("value")(Bytes.toString(result.value))

    expect(1)(result.getMap.size)
    expect(1)(result.getMap.get("family"b).size)
    expect(1)(result.getMap.get("family"b).get("qualifier"b).size)
    expect("value")(
        Bytes.toString(result.getMap.get("family"b).get("qualifier"b).get(12345L)))
  }

  test("HTable.put row then HTable.scan family") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    val scanner = table.getScanner("family")
    val it = scanner.iterator
    expect(true)(it.hasNext)
    val result = it.next
    expect(false)(result.isEmpty)
    expect("key")(Bytes.toString(result.getRow))
    expect("value")(Bytes.toString(result.value))

    expect(1)(result.getMap.size)
    expect(1)(result.getMap.get("family"b).size)
    expect(1)(result.getMap.get("family"b).get("qualifier"b).size)
    expect("value")(
        Bytes.toString(result.getMap.get("family"b).get("qualifier"b).get(12345L)))

    expect(false)(it.hasNext)
  }

  test("HTable.put row then HTable.delete row") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    table.delete(new Delete("key"))
    expect(true)(table.get(new Get("key")).isEmpty)
  }

  test("HTable.incrementeColumnValue") {
    val table = new FakeHTable(name = "table", desc = null)
    expect(1)(table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
    expect(2)(table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
    expect(3)(table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
  }

  test("HTable.delete on specific cell") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))
    table.put(new Put("key")
        .add("family", "qualifier", 2L, "value2"))
    table.put(new Put("key")
        .add("family", "qualifier", 3L, "value3"))
    table.delete(new Delete("key")
        .deleteColumn("family", "qualifier", 2L))
    val scanner = table.getScanner(new Scan("key")
        .setMaxVersions(Int.MaxValue)
        .addColumn("family", "qualifier"))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family", "qualifier")
    expect(2)(cells.size())
    expect(3L)(cells.get(0).getTimestamp)
    expect(1L)(cells.get(1).getTimestamp)
  }

  test("HTable.delete on most recent cell") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))
    table.put(new Put("key")
        .add("family", "qualifier", 2L, "value2"))
    table.put(new Put("key")
        .add("family", "qualifier", 3L, "value3"))
    table.delete(new Delete("key")
        .deleteColumn("family", "qualifier"))
    val scanner = table.getScanner(new Scan("key")
        .setMaxVersions(Int.MaxValue)
        .addColumn("family", "qualifier"))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family", "qualifier")
    expect(2)(cells.size())
    expect(2L)(cells.get(0).getTimestamp)
    expect(1L)(cells.get(1).getTimestamp)
  }

  test("HTable.delete on older versions of qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))
    table.put(new Put("key")
        .add("family", "qualifier", 2L, "value2"))
    table.put(new Put("key")
        .add("family", "qualifier", 3L, "value3"))
    table.delete(new Delete("key")
        .deleteColumns("family", "qualifier", 2L))
    val scanner = table.getScanner(new Scan("key")
        .setMaxVersions(Int.MaxValue)
        .addColumn("family", "qualifier"))
    val row = scanner.next()
    expect(null)(scanner.next())
    val cells = row.getColumn("family", "qualifier")
    expect(1)(cells.size())
    expect(3L)(cells.get(0).getTimestamp)
  }

  test("HTable.delete all versions of qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))
    table.put(new Put("key")
        .add("family", "qualifier", 2L, "value2"))
    table.put(new Put("key")
        .add("family", "qualifier", 3L, "value3"))
    table.delete(new Delete("key")
        .deleteColumns("family", "qualifier"))
    val scanner = table.getScanner(new Scan("key")
        .setMaxVersions(Int.MaxValue)
        .addColumn("family", "qualifier"))
    expect(null)(scanner.next())
  }

  test("HTable.delete with family/qualifier/time-series cleanups") {
    val table = new FakeHTable(name = "table", desc = null)

    // Populate one row with 4 families, each with 4 qualifiers, each with 4 versions:
    val count = 4
    val rowKey = "key1"
    populateTable(table, count = count)

    // Delete all versions of family1:qualifier1 one by one, and check:
    for (timestamp <- 0 until count) {
      table.delete(new Delete(rowKey)
          .deleteColumn("family1", "qualifier1", timestamp))
    }
    {
      val scanner = table.getScanner(new Scan(rowKey, nextRow(rowKey))
          .setMaxVersions(Int.MaxValue)
          .addColumn("family1", "qualifier1"))
      expect(null)(scanner.next())
    }

    // Delete all qualifiers in family2 and check:
    for (cId <- 0 until count) {
      table.delete(new Delete(rowKey)
          .deleteColumns("family2", "qualifier%d".format(cId)))
    }
    {
      val scanner = table.getScanner(new Scan(rowKey, nextRow(rowKey))
          .setMaxVersions(Int.MaxValue)
          .addFamily("family2"))
      expect(null)(scanner.next())
    }
  }

  test("ResultScanner.hasNext on empty table") {
    val table = new FakeHTable(name = "table", desc = null)
    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("ResultScanner.hasNext") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))

    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    expect(true)(iterator.hasNext())
    assert(iterator.next() != null)
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("ResultScanner.hasNext with qualifier") {
    val table = new FakeHTable(name = "table", desc = null)
    table.put(new Put("key1")
        .add("family", "qualifier1", 1L, "value1"))
    table.put(new Put("key2")
        .add("family", "qualifier2", 1L, "value1"))

    val scanner = table.getScanner(new Scan()
        .addColumn("family", "qualifier1"))
    val iterator = scanner.iterator()
    expect(true)(iterator.hasNext())
    assert(iterator.next() != null)
    expect(false)(iterator.hasNext())
    expect(null)(iterator.next())
  }

  test("HTable.get with filter") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )

    val count = 2
    populateTable(table, count=count)

    val get = new Get("key1")
        .setMaxVersions()
        .setFilter(new KeyOnlyFilter)
    val result = table.get(get)
    expect(count)(result.getMap.size)
    for ((family, qmap) <- result.getMap.asScala) {
      expect(count)(qmap.size)
      for ((qualifier, tseries) <- qmap.asScala) {
        expect(count)(tseries.size)
        for ((timestamp, value) <- tseries.asScala) {
          expect(0)(value.size)
        }
      }
    }
  }

  test("HTable.get with max versions") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )

    val count = 4
    populateTable(table, count=count)

    // We generated 4 versions, but request only 2:
    val maxVersions = 2
    val get = new Get("key1")
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

  test("HTable.scan with start-row") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )

    val count = 3
    populateTable(table, count=count)

    {
      val scanner = table.getScanner(new Scan().setStartRow("key1"))
      val rows = scanner.iterator().asScala.toList
      expect(2)(rows.size)
      expect("key1")(Bytes.toString(rows(0).getRow))
      expect("key2")(Bytes.toString(rows(1).getRow))
    }
    {
      val scanner = table.getScanner(new Scan().setStartRow("key1a"))
      val rows = scanner.iterator().asScala.toList
      expect(1)(rows.size)
      expect("key2")(Bytes.toString(rows(0).getRow))
    }
  }

  test("HTable.get with ColumnRangefilter inclusive") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )
    val count = 5
    populateTable(table, count=count)

    // Only fetch qualifiers >= 'qualifier2' and <= 'qualifier3',
    // this should be exactly "qualifier2" and "qualifier3":
    val get = new Get("key1")
        .setFilter(new ColumnRangeFilter("qualifier2", true, "qualifier3", true))
        .setMaxVersions()

    val result = table.get(get)
    expect("key1")(bytesToString(result.getRow))
    val families = for (x <- 0 until count) yield "family%d".format(x)
    expect(families)(result.getMap.keySet.asScala.toList.map {bytesToString(_)})
    for (family <- families) {
      val qmap = result.getMap.get(family.bytes())
      expect(List("qualifier2", "qualifier3"))(qmap.keySet.asScala.toList.map {Bytes.toString(_)})
      expect(5)(qmap.firstEntry.getValue.size)
      expect(5)(qmap.lastEntry.getValue.size)
    }
  }

  test("HTable.get with ColumnRangefilter exclusive") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )
    val count = 5
    populateTable(table, count=count)

    // Only fetch qualifiers > 'qualifier2' and < 'qualifier4',
    // this should be exactly "qualifier3":
    val get = new Get("key1")
        .setFilter(
            new ColumnRangeFilter("qualifier2", false, "qualifier4", false))
        .setMaxVersions()

    val result = table.get(get)
    expect("key1")(bytesToString(result.getRow))
    val families = for (x <- 0 until count) yield "family%d".format(x)
    expect(families)(result.getMap.keySet.asScala.toList.map {Bytes.toString(_)})
    for (family <- families) {
      val qmap = result.getMap.get(family.bytes())
      expect(List("qualifier3"))(qmap.keySet.asScala.toList.map {Bytes.toString(_)})
      expect(5)(qmap.firstEntry.getValue.size)
    }
  }

  test("HTable.get with ColumnRangefilter exclusive empty") {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = null
    )
    val count = 5
    populateTable(table, count=count)

    // Only fetch qualifiers >= 'qualifier2.5' and < 'qualifier3',
    // this should be empty:
    val get = new Get("key1")
        .setFilter(
            new ColumnRangeFilter("qualifier2.5", true, "qualifier3", false))
        .setMaxVersions()

    val result = table.get(get)
    expect(true)(result.isEmpty)
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
      val rowKey = "key%d".format(rId)
      for (fId <- 0 until count) {
        val family = "family%d".format(fId)
        for (cId <- 0 until count) {
          val qualifier = "qualifier%d".format(cId)
          for (timestamp <- 0L until count) {
            table.put(new Put(rowKey)
                .add(family, qualifier, timestamp, "value%d".format(timestamp)))
          }
        }
      }
    }
  }
}
