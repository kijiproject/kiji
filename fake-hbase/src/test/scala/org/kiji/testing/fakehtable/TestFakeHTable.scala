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
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.Append
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter
import org.apache.hadoop.hbase.filter.ColumnRangeFilter
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Assert
import org.junit.Test
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.slf4j.LoggerFactory

class TestFakeHTable {
  private final val Log = LoggerFactory.getLogger(classOf[TestFakeHTable])

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

  // -----------------------------------------------------------------------------------------------

  /** Table descriptor for a test table with an HBase family named "family". */
  def defaultTableDesc(): HTableDescriptor = {
    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(HConstants.ALL_VERSIONS)
        .setMinVersions(0)
        .setTimeToLive(HConstants.FOREVER)
        .setInMemory(false)
    )
    desc
  }

  /** Table descriptor for a test table with N HBase families named "family<N>". */
  def makeTableDesc(nfamilies: Int): HTableDescriptor = {
    val desc = new HTableDescriptor("table")
    for (ifamily <- 0 until nfamilies) {
      desc.addFamily(new HColumnDescriptor("family%s".format(ifamily))
          .setMaxVersions(HConstants.ALL_VERSIONS)
          .setMinVersions(0)
          .setTimeToLive(HConstants.FOREVER)
          .setInMemory(false)
      )
    }
    desc
  }

  // -----------------------------------------------------------------------------------------------

  /** get() on an unknown row. */
  @Test
  def testGetUnknownRow(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    Assert.assertEquals(true, table.get(new Get("key")).isEmpty)
  }

  /** scan() on an empty table. */
  @Test
  def testScanEmptyTable(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    Assert.assertEquals(null, table.getScanner("family").next)
  }

  /** delete() on an unknown row. */
  @Test
  def testDeleteUnknownRow(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.delete(new Delete("key"))
  }

  /** Write a few cells and read them back. */
  @Test
  def testPutThenGet(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    val result = table.get(new Get("key"))
    Assert.assertEquals(false, result.isEmpty)
    Assert.assertEquals("key", Bytes.toString(result.getRow))
    Assert.assertEquals("value", Bytes.toString(result.value))

    Assert.assertEquals(1, result.getMap.size)
    Assert.assertEquals(1, result.getMap.get("family"b).size)
    Assert.assertEquals(1, result.getMap.get("family"b).get("qualifier"b).size)
    Assert.assertEquals(
        "value",
        Bytes.toString(result.getMap.get("family"b).get("qualifier"b).get(12345L)))
  }

  /** Write a few cells and read them back as a family scan. */
  @Test
  def testPutThenScan(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    val scanner = table.getScanner("family")
    val it = scanner.iterator
    Assert.assertEquals(true, it.hasNext)
    val result = it.next
    Assert.assertEquals(false, result.isEmpty)
    Assert.assertEquals("key", Bytes.toString(result.getRow))
    Assert.assertEquals("value", Bytes.toString(result.value))

    Assert.assertEquals(1, result.getMap.size)
    Assert.assertEquals(1, result.getMap.get("family"b).size)
    Assert.assertEquals(1, result.getMap.get("family"b).get("qualifier"b).size)
    Assert.assertEquals(
        "value",
        Bytes.toString(result.getMap.get("family"b).get("qualifier"b).get(12345L)))

    Assert.assertEquals(false, it.hasNext)
  }

  /** Create a row and delete it. */
  @Test
  def testCreateAndDeleteRow(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
        .add("family", "qualifier", 12345L, "value"))

    table.delete(new Delete("key"))
    Assert.assertEquals(true, table.get(new Get("key")).isEmpty)
  }

  /** Increment a column. */
  @Test
  def testIncrementColumn(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    Assert.assertEquals(1, table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
    Assert.assertEquals(2, table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
    Assert.assertEquals(3, table.incrementColumnValue(
        row = "row",
        family = "family",
        qualifier = "qualifier",
        amount = 1))
  }

  /** Delete a specific cell. */
  @Test
  def testDeleteSpecificCell(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
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
    Assert.assertEquals(null, scanner.next())
    val cells = row.getColumn("family", "qualifier")
    Assert.assertEquals(2, cells.size())
    Assert.assertEquals(3L, cells.get(0).getTimestamp)
    Assert.assertEquals(1L, cells.get(1).getTimestamp)
  }

  /** Delete the most recent cell in a column. */
  @Test
  def testDeleteMostRecentCell(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))
    table.put(new Put("key")
        .add("family", "qualifier", 2L, "value2"))
    table.put(new Put("key")
        .add("family", "qualifier", 3L, "value3"))

    if (Log.isDebugEnabled)
      table.dump()

    table.delete(new Delete("key")
        .deleteColumn("family", "qualifier"))

    if (Log.isDebugEnabled)
      table.dump()

    val row = {
      if (false) {
        val scanner = table.getScanner(new Scan("key")
            .setMaxVersions(Int.MaxValue)
            .addColumn("family", "qualifier"))
        val row = scanner.next()
        Assert.assertEquals(null, scanner.next())
        row
      } else {
        table.get(new Get("key")
            .setMaxVersions(Int.MaxValue)
            .addColumn("family", "qualifier"))
      }
    }

    val cells = row.getColumnCells("family", "qualifier")
    Assert.assertEquals(2, cells.size())
    Assert.assertEquals(2L, cells.get(0).getTimestamp)
    Assert.assertEquals(1L, cells.get(1).getTimestamp)
  }

  /** Delete older versions of a qualifier. */
  @Test
  def testDeleteOlderCellVersions(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
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
    Assert.assertEquals(null, scanner.next())
    val cells = row.getColumn("family", "qualifier")
    Assert.assertEquals(1, cells.size())
    Assert.assertEquals(3L, cells.get(0).getTimestamp)
  }

  /** Delete all versions of a specific qualifier. */
  @Test
  def testDeleteSpecificQualifierAllVersions(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
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
    Assert.assertEquals(null, scanner.next())
  }

  /** Test that families are cleaned up properly when the last qualifier disappears. */
  @Test
  def testFamilyCleanupAfterDelete(): Unit = {
    val table = new FakeHTable(name = "table", desc = makeTableDesc(nfamilies = 4))

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
      Assert.assertEquals(null, scanner.next())
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
      Assert.assertEquals(null, scanner.next())
    }
  }

  /** Test ResultScanner.hasNext() on a empty table. */
  @Test
  def testResultScannerHasNextOnEmptyTable(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    Assert.assertEquals(false, iterator.hasNext())
    Assert.assertEquals(null, iterator.next())
  }

  /** Test ResultScanner.hasNext() with stop row-key on an empty table. */
  @Test
  def testResultScannerWithStopRowKeyOnEmptyTable(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    val scanner = table.getScanner(new Scan().setStopRow("stop"))
    val iterator = scanner.iterator()
    Assert.assertEquals(false, iterator.hasNext())
    Assert.assertEquals(null, iterator.next())
  }

  /** Test ResultScanner.hasNext() while scanning a full table. */
  @Test
  def testResultScannerHasNext(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key")
        .add("family", "qualifier", 1L, "value1"))

    val scanner = table.getScanner(new Scan())
    val iterator = scanner.iterator()
    Assert.assertEquals(true, iterator.hasNext())
    assert(iterator.next() != null)
    Assert.assertEquals(false, iterator.hasNext())
    Assert.assertEquals(null, iterator.next())
  }

  /** Test ResultScanner.hasNext() while scanning a specific column. */
  @Test
  def testResultScannerHasNextWithQualifier(): Unit = {
    val table = new FakeHTable(name = "table", desc = defaultTableDesc)
    table.put(new Put("key1")
        .add("family", "qualifier1", 1L, "value1"))
    table.put(new Put("key2")
        .add("family", "qualifier2", 1L, "value1"))

    val scanner = table.getScanner(new Scan()
        .addColumn("family", "qualifier1"))
    val iterator = scanner.iterator()
    Assert.assertEquals(true, iterator.hasNext())
    assert(iterator.next() != null)
    Assert.assertEquals(false, iterator.hasNext())
    Assert.assertEquals(null, iterator.next())
  }

  /** Test a get request with a filter. */
  @Test
  def testGetWithFilter(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 2)
    )

    val count = 2
    populateTable(table, count=count)

    // if (Log.isDebugEnabled)
      table.dump()

    val get = new Get("key1")
        .setMaxVersions()
        .setFilter(new KeyOnlyFilter)
    val result = table.get(get)
    Assert.assertEquals(count, result.getMap.size)
    for ((family, qmap) <- result.getMap.asScala) {
      Assert.assertEquals(count, qmap.size)
      for ((qualifier, tseries) <- qmap.asScala) {
        Assert.assertEquals(count, tseries.size)
        for ((timestamp, value) <- tseries.asScala) {
          Assert.assertEquals(0, value.size)
        }
      }
    }
  }

  /**
   * Test a get request with the FirstKeyOnly filter.
   *
   * This validates some behaviors around ReturnCode.NEXT_ROW.
   */
  @Test
  def testGetWithFirstKeyOnlyFilter(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 2)
    )

    val count = 2
    populateTable(table, count=count)

    val get = new Get("key1")
        .setMaxVersions()
        .setFilter(new FirstKeyOnlyFilter)
    val result = table.get(get)

    Assert.assertEquals(1, result.getMap.size)
    Assert.assertTrue(result.containsColumn("family0", "qualifier0"))
  }

  /** Test a get request with max versions. */
  @Test
  def testGetWithMaxVersions(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 4)
    )

    val count = 4
    populateTable(table, count=count)

    // We generated 4 versions, but request only 2:
    val maxVersions = 2
    val get = new Get("key1")
        .setMaxVersions(maxVersions)
        // .setFilter(new KeyOnlyFilter)
    val result = table.get(get)
    Assert.assertEquals(count, result.getMap.size)
    for ((family, qmap) <- result.getMap.asScala) {
      Assert.assertEquals(count, qmap.size)
      for ((qualifier, tseries) <- qmap.asScala) {
        Assert.assertEquals(maxVersions, tseries.size)
        assert(tseries.containsKey(2L))
        assert(tseries.containsKey(3L))
      }
    }
  }

  /** Test a scan with an explicit start row. */
  @Test
  def testScanWithStartRow(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 3)
    )

    val count = 3
    populateTable(table, count=count)

    {
      val scanner = table.getScanner(new Scan().setStartRow("key1"))
      val rows = scanner.iterator().asScala.toList
      Assert.assertEquals(2, rows.size)
      Assert.assertEquals("key1", Bytes.toString(rows(0).getRow))
      Assert.assertEquals("key2", Bytes.toString(rows(1).getRow))
    }
    {
      val scanner = table.getScanner(new Scan().setStartRow("key1a"))
      val rows = scanner.iterator().asScala.toList
      Assert.assertEquals(1, rows.size)
      Assert.assertEquals("key2", Bytes.toString(rows(0).getRow))
    }
  }

  /** Test scanning with a prefix filter. */
  @Test
  def testScanWithFilter(): Unit = {
    val table = new FakeHTable(
      name = "table",
      conf = HBaseConfiguration.create(),
      desc = defaultTableDesc
    )

    val rowKey = "key"
    val family = "family"
    for (qualifier <- List("non-prefixed", "prefix:a")) {
      table.put(new Put(rowKey).add(family, qualifier, qualifier))
    }

    {
      val get = new Get(rowKey).setFilter(new ColumnPrefixFilter("prefix"))
      val result = table.get(get)
      val values = result.raw().toList.map(kv => Bytes.toString(kv.getValue))
      Assert.assertEquals(List("prefix:a"), values)
    }

    {
      val scan = new Scan(rowKey).setFilter(new ColumnPrefixFilter("prefix"))
      val scanner = table.getScanner(scan)
      val rows = scanner.iterator().asScala.toList

      val values = rows(0).raw().toList.map(kv => Bytes.toString(kv.getValue))
      Assert.assertEquals(List("prefix:a"), values)
    }
  }

  /**
   * Test a get request with ColumnRangefilter.
   * Range is inclusive and yields a non-empty result.
   */
  @Test
  def testColumnRangeFilterWithInclusiveBound(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 5)
    )
    val count = 5
    for (x <- 0 until count) {
      val familyDesc = new HColumnDescriptor("family%d".format(x))
          .setMaxVersions(HConstants.ALL_VERSIONS)
      table.getTableDescriptor.addFamily(familyDesc)
    }
    populateTable(table, count=count)

    // Only fetch qualifiers >= 'qualifier2' and <= 'qualifier3',
    // this should be exactly "qualifier2" and "qualifier3":
    val get = new Get("key1")
        .setFilter(new ColumnRangeFilter("qualifier2", true, "qualifier3", true))
        .setMaxVersions()

    val result = table.get(get)
    Assert.assertEquals("key1", bytesToString(result.getRow))
    val families = for (x <- 0 until count) yield "family%d".format(x)
    Assert.assertEquals(families, result.getMap.keySet.asScala.toList.map {bytesToString(_)})
    for (family <- families) {
      val qmap = result.getMap.get(family.bytes())
      Assert.assertEquals(List("qualifier2", "qualifier3"), qmap.keySet.asScala.toList.map {Bytes.toString(_)})
      Assert.assertEquals(5, qmap.firstEntry.getValue.size)
      Assert.assertEquals(5, qmap.lastEntry.getValue.size)
    }
  }

  /**
   * Test a get request with a ColumnRangeFilter.
   * Range is exclusive and yields a non-empty result.
   */
  @Test
  def testColumnRangeFilterWithExclusiveBoundWithResult(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 5)
    )
    val count = 5
    for (x <- 0 until count) {
      val familyDesc = new HColumnDescriptor("family%d".format(x))
          .setMaxVersions(HConstants.ALL_VERSIONS)
      table.getTableDescriptor.addFamily(familyDesc)
    }
    populateTable(table, count=count)

    // Only fetch qualifiers > 'qualifier2' and < 'qualifier4',
    // this should be exactly "qualifier3":
    val get = new Get("key1")
        .setFilter(
            new ColumnRangeFilter("qualifier2", false, "qualifier4", false))
        .setMaxVersions()

    val result = table.get(get)
    Assert.assertEquals("key1", bytesToString(result.getRow))
    val families = for (x <- 0 until count) yield "family%d".format(x)
    Assert.assertEquals(families, result.getMap.keySet.asScala.toList.map {Bytes.toString(_)})
    for (family <- families) {
      val qmap = result.getMap.get(family.bytes())
      Assert.assertEquals(List("qualifier3"), qmap.keySet.asScala.toList.map {Bytes.toString(_)})
      Assert.assertEquals(5, qmap.firstEntry.getValue.size)
    }
  }

  /**
   * Test a get request with a ColumnRangeFilter.
   * Range is exclusive and yields an empty result.
   */
  @Test
  def testColumnRangeFilterWithExclusiveBoundAndEmptyResult(): Unit = {
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = makeTableDesc(nfamilies = 5)
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
    Assert.assertEquals(true, result.isEmpty)
  }

  /** Test that max versions applies correctly while putting many cells. */
  @Test
  def testMaxVersions(): Unit = {
    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(5)
        // min versions defaults to 0, TTL defaults to forever.
    )
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = desc
    )

    for (index <- 0 until 9) {
      val timestamp = index
      table.put(new Put("row").add("family", "q", timestamp, "value-%d".format(index)))
    }

    val result = table.get(new Get("row").addFamily("family").setMaxVersions())
    val kvs = result.getColumn("family", "q")
    Assert.assertEquals(5, kvs.size)
  }

  /** Test that TTL applies correctly with no min versions (ie. min versions = 0). */
  @Test
  def testTTLWithoutMinVersion(): Unit = {
    val ttl = 3600  // 1h TTL

    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(HConstants.ALL_VERSIONS)  // retain all versions
        .setTimeToLive(ttl)                       // 1h TTL
        .setMinVersions(0)                        // no min versions to retain wrt TTL
    )
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = desc
    )

    val nowMS = System.currentTimeMillis
    val minTimestamp = nowMS - (ttl * 1000)

    // Write cells older than TTL, all these cells should be discarded:
    for (index <- 0 until 9) {
      val timestamp = minTimestamp - index  // absolutely older than TTL allows
      table.put(new Put("row").add("family", "q", timestamp, "value-%d".format(index)))
    }

    {
      val result = table.get(new Get("row").addFamily("family").setMaxVersions())
      val kvs = result.getColumn("family", "q")
      // Must be empty since all the puts were older than TTL allows and no min versions set:
      Assert.assertEquals(0, kvs.size)
    }

    // Write cells within TTL range, all these cells should be kept:
    for (index <- 0 until 9) {
      val timestamp = nowMS + index  // within TTL range
      table.put(new Put("row").add("family", "q", timestamp, "value-%d".format(index)))
    }

    {
      val result = table.get(new Get("row").addFamily("family").setMaxVersions())
      val kvs = result.getColumn("family", "q")
      Assert.assertEquals(9, kvs.size)
    }
  }

  /** Test that the min versions is respected while max TTL is being applied. */
  @Test
  def testMinVersionsWithTTL(): Unit = {
    val ttl = 3600  // 1h TTL

    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(HConstants.ALL_VERSIONS)  // retain all versions
        .setTimeToLive(ttl)                       // 1h TTL
        .setMinVersions(2)                        // retain at least 2 versions wrt TTL
    )
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = desc
    )

    val nowMS = System.currentTimeMillis
    val minTimestamp = nowMS - (ttl * 1000)

    // Write cells older than TTL, only 2 of these cells should be retained:
    for (index <- 0 until 9) {
      val timestamp = minTimestamp - index  // absolutely older than TTL allows
      table.put(new Put("row").add("family", "q", timestamp, "value-%d".format(index)))
    }

    {
      val result = table.get(new Get("row").addFamily("family").setMaxVersions())
      val kvs = result.getColumn("family", "q")
      Assert.assertEquals(2, kvs.size)
      Assert.assertEquals(minTimestamp, kvs.get(0).getTimestamp)
      Assert.assertEquals(minTimestamp - 1, kvs.get(1).getTimestamp)
    }

    // Write cells within TTL range, all these cells should be kept,
    // but the 2 cells older than TTL should disappear:
    for (index <- 0 until 9) {
      val timestamp = nowMS + index  // within TTL range
      table.put(new Put("row").add("family", "q", timestamp, "value-%d".format(index)))
    }

    {
      val result = table.get(new Get("row").addFamily("family").setMaxVersions())
      val kvs = result.getColumn("family", "q")
      Assert.assertEquals(9, kvs.size)
      Assert.assertEquals(nowMS, kvs.get(8).getTimestamp)
    }
  }

  /** Test for the HTable.append() method. */
  @Test
  def testAppend(): Unit = {
    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(HConstants.ALL_VERSIONS)  // retain all versions
    )
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = desc
    )

    val key = "row key"

    {
      val result = table.append(new Append(key).add("family", "qualifier", "value1"))
      Assert.assertEquals(1, result.size)
      Assert.assertEquals(1, result.getColumn("family", "qualifier").size)
      Assert.assertEquals(
          "value1",
          bytesToString(result.getColumnLatest("family", "qualifier").getValue))
    }
    {
      val result = table.append(new Append(key).add("family", "qualifier", "value2"))
      Assert.assertEquals(1, result.size)
      Assert.assertEquals(1, result.getColumn("family", "qualifier").size)
      Assert.assertEquals(
          "value1value2",
        bytesToString(result.getColumnLatest("family", "qualifier").getValue))
    }
    {
      val result = table.get(new Get(key)
          .setMaxVersions()
          .addColumn("family", "qualifier"))
      Assert.assertEquals(2, result.size)
      val kvs = result.getColumn("family", "qualifier")
      Assert.assertEquals(2, kvs.size)
      Assert.assertEquals("value1value2", bytesToString(kvs.get(0).getValue))
      Assert.assertEquals("value1", bytesToString(kvs.get(1).getValue))
    }
  }

  /** Makes sure the max TTL does not overflow due to 32 bits integer multiplication. */
  @Test
  def testMaxTTLOverflow(): Unit = {
    // The following TTL is nearly 25 days, in seconds,
    // and causes a 32 bits overflow when converted to ms:
    //     2147484 * 1000 = -2147483296
    // while:
    //     2147484 * 1000L = 2147484000
    //
    val ttl = 2147484

    val desc = new HTableDescriptor("table")
    desc.addFamily(new HColumnDescriptor("family")
        .setMaxVersions(HConstants.ALL_VERSIONS)  // retain all versions
        .setTimeToLive(2147484)  // retain cells for ~25 days
        .setMinVersions(0)  // no minimum number of versions to retain
    )
    val table = new FakeHTable(
        name = "table",
        conf = HBaseConfiguration.create(),
        desc = desc
    )

    // Writes a cell whose timestamp is now.
    // Since the TTL is ~25 days, the cell should not be discarded,
    // unless the 32 bits overflow occurs when converting the TTL to ms.
    // If the overflow occurs, the TTL becomes negative and all cells older than now + ~25 days
    // are discarded.
    val row = "row"
    val nowMS = System.currentTimeMillis
    table.put(new Put(row).add("family", "qualifier", nowMS, "value"))

    val result = table.get(new Get(row))
    Assert.assertEquals("value", Bytes.toString(result.getValue("family", "qualifier")))
  }

  // -----------------------------------------------------------------------------------------------

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
