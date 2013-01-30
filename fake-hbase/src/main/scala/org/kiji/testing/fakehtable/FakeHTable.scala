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

import java.io.PrintStream
import java.util.{ArrayList => JArrayList}
import java.util.Arrays
import java.util.{Iterator => JIterator}
import java.util.{List => JList}
import java.util.{Map => JMap}
import java.util.NavigableMap
import java.util.NavigableSet
import java.util.{TreeMap => JTreeMap}
import java.util.{TreeSet => JTreeSet}

import scala.Option.option2Iterable
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.Buffer
import scala.util.control.Breaks
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HRegionLocation
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.client.RowLock
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.WritableUtils
import org.kiji.testing.fakehtable.JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator
import org.slf4j.LoggerFactory

/**
 * Fake in-memory HTable.
 *
 * @param name Table name.
 * @param desc Table descriptor.
 * @param conf Table configuration.
 * @param autoFlush auto-flush flag.
 * @param enabled Whether the table is enabled.
 */
class FakeHTable(
    val name: String,
    val desc: HTableDescriptor,
    val conf: Configuration = HBaseConfiguration.create(),
    private var autoFlush: Boolean = false,
    private var writeBufferSize: Long = 1,
    var enabled: Boolean = true
) extends HTableInterface {
  private val Log = LoggerFactory.getLogger(getClass)
  require(conf != null)

  /** Whether the table has been closed. */
  private var closed: Boolean = false

  /** Region splits and locations. */
  private var regions: Seq[HRegionLocation] = Seq()

  /** Byte array shortcut. */
  type Bytes = Array[Byte]

  /** Time series in a column, ordered by decreasing time stamps. */
  type ColumnSeries = NavigableMap[Long, Bytes]

  /** Map column qualifiers to cell series. */
  type FamilyQualifiers = NavigableMap[Bytes, ColumnSeries]

  /** Map of column family names to qualifier maps. */
  type RowFamilies = NavigableMap[Bytes, FamilyQualifiers]

  /** Map of row keys. */
  type Table = NavigableMap[Bytes, RowFamilies]

  /** Map: row key -> family -> qualifier -> timestamp -> cell data. */
  private val rows: Table = new JTreeMap[Bytes, RowFamilies](Bytes.BYTES_COMPARATOR)

  override def getTableName(): Array[Byte] = {
    return name.getBytes
  }

  override def getConfiguration(): Configuration = {
    return conf
  }

  override def getTableDescriptor(): HTableDescriptor = {
    return desc
  }

  override def exists(get: Get): Boolean = {
    return !this.get(get).isEmpty()
  }

  override def batch(actions: JList[Row], results: Array[Object]): Unit = {
    require(results.size == actions.size)
    val array = batch(actions)
    System.arraycopy(array, 0, results, 0, results.length)
  }

  override def batch(actions: JList[Row]): Array[Object] = {
    val results = Buffer[Object]()
    actions.asScala.foreach { action =>
      action match {
        case put: Put => {
          this.put(put)
          results += new Object()
        }
        case get: Get => {
          results += this.get(get)
        }
        case delete: Delete => {
          this.delete(delete)
          results += new Object()
        }
        case increment: Increment => {
          results += this.increment(increment)
        }
      }
    }
    return results.toArray
  }

  override def get(get: Get): Result = {
    // get() could be built around scan(), to ensure consistent filters behavior.
    // For now, we use a shorcut:
    val filter: Filter = getFilter(get.getFilter)
    filter.reset()
    if (filter.filterAllRemaining()) {
      return new Result()
    }
    val rowKey = get.getRow
    if (filter.filterRowKey(rowKey, 0, rowKey.size)) {
      return new Result()
    }
    val row = rows.get(rowKey)
    if (row == null) {
      return new Result()
    }
    val result = makeResult(
        rowKey = rowKey,
        row = row,
        familyMap = get.getFamilyMap,
        timeRange = get.getTimeRange,
        maxVersions = get.getMaxVersions,
        filter = filter
    )
    if (filter.filterRow()) {
      return new Result()
    }
    return result
  }

  override def get(gets: JList[Get]): Array[Result] = {
    return gets.asScala.map(this.get(_)).toArray
  }

  @deprecated(message = "Deprecated method will not be implemented", since = "HBase 0.92")
  override def getRowOrBefore(row: Bytes, family: Bytes): Result = {
    sys.error("Deprecated method will not be implemented")
  }

  override def getScanner(scan: Scan): ResultScanner = {
    return new FakeResultScanner(scan)
  }

  override def getScanner(family: Bytes): ResultScanner = {
    return getScanner(new Scan().addFamily(family))
  }

  override def getScanner(family: Bytes, qualifier: Bytes): ResultScanner = {
    return getScanner(new Scan().addColumn(family, qualifier))
  }

  override def put(put: Put): Unit = {
    synchronized {
      val nowMS = System.currentTimeMillis

      val rowKey = put.getRow
      val rowFamilyMap = rows.asScala
          .getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](Bytes.BYTES_COMPARATOR))
      for ((family, kvs) <- put.getFamilyMap.asScala) {
        val rowQualifierMap = rowFamilyMap.asScala
            .getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](Bytes.BYTES_COMPARATOR))

        for (kv <- kvs.asScala) {
          require(family.toSeq == kv.getFamily.toSeq)

          val column = rowQualifierMap.asScala
              .getOrElseUpdate(kv.getQualifier, new JTreeMap[Long, Bytes](TimestampComparator))

          val timestamp = {
            if (kv.getTimestamp == HConstants.LATEST_TIMESTAMP) {
              nowMS
            } else {
              kv.getTimestamp
            }
          }

          column.put(timestamp, kv.getValue)
        }
      }
    }
  }

  override def put(put: JList[Put]): Unit = {
    put.asScala.foreach(this.put(_))
  }

  /**
   * Checks the value of a cell.
   *
   * @param row Row key.
   * @param family Family.
   * @param qualifier Qualifier.
   * @param value Value to compare against.
   *     Null means check that the specified cell does not exist.
   * @return whether the HBase cell check is successful, ie. whether the specified cell exists
   *     and contains the given value, or whether the cell does not exist.
   */
  private def checkCell(row: Bytes, family: Bytes, qualifier: Bytes, value: Bytes): Boolean = {
    if (value == null) {
      val fmap = rows.get(row)
      if (fmap == null) return true
      val qmap = fmap.get(family)
      if (qmap == null) return true
      val tmap = qmap.get(qualifier)
      return (tmap == null) || tmap.isEmpty
    } else {
      val fmap = rows.get(row)
      if (fmap == null) return false
      val qmap = fmap.get(family)
      if (qmap == null) return false
      val tmap = qmap.get(qualifier)
      if ((tmap == null) || tmap.isEmpty) return false
      return Arrays.equals(tmap.firstEntry.getValue, value)
    }
  }

  override def checkAndPut(
      row: Bytes,
      family: Bytes,
      qualifier: Bytes,
      value: Bytes,
      put: Put
  ): Boolean = {
    synchronized {
      if (checkCell(row = row, family = family, qualifier = qualifier, value = value)) {
        this.put(put)
        return true
      } else {
        return false
      }
    }
  }

  /**
   * Removes empty maps for a specified row, family and/or qualifier.
   *
   * @param rowKey Key of the row to clean up.
   * @param family Optional family to clean up. None means clean all families.
   * @param qualifier Optional qualifier to clean up. None means clean all qualifiers.
   */
  private def cleanupRow(rowKey: Bytes, family: Option[Bytes], qualifier: Option[Bytes]): Unit = {
    val row = rows.get(rowKey)
    if (row == null) { return }

    val families : Iterable[Bytes] = family match {
      case Some(_) => family
      case None => row.keySet.asScala
    }
    val emptyFamilies = Buffer[Bytes]()  // empty families to clean up
    for (family <- families) {
      val rowQualifierMap = row.get(family)
      if (rowQualifierMap != null) {
        val qualifiers : Iterable[Bytes] = qualifier match {
          case Some(_) => qualifier
          case None => rowQualifierMap.keySet.asScala
        }
        val emptyQualifiers = Buffer[Bytes]()
        for (qualifier <- qualifiers) {
          val timeSeries = rowQualifierMap.get(qualifier)
          if ((timeSeries != null)  && timeSeries.isEmpty) {
            emptyQualifiers += qualifier
          }
        }
        emptyQualifiers.foreach { qualifier => rowQualifierMap.remove(qualifier) }

        if (rowQualifierMap.isEmpty) {
          emptyFamilies += family
        }
      }
    }
    emptyFamilies.foreach { family => row.remove(family) }
    if (row.isEmpty) {
      rows.remove(rowKey)
    }
  }

  override def delete(delete: Delete): Unit = {
    synchronized {
      val rowKey = delete.getRow
      val row = rows.get(rowKey)
      if (row == null) { return }

      if (delete.getFamilyMap.isEmpty) {
        for ((family, qualifiers) <- row.asScala) {
          for ((qualifier, series) <- qualifiers.asScala) {
            series.subMap(delete.getTimeStamp, true, 0, true).clear()
          }
        }
        cleanupRow(rowKey = rowKey, family = None, qualifier = None)
        return
      }

      for ((requestedFamily, kvs) <- delete.getFamilyMap.asScala) {
        val rowQualifierMap = row.get(requestedFamily)
        if (rowQualifierMap != null) {
          for (kv <- kvs.asScala) {
            require(kv.isDelete)
            if (kv.isDeleteFamily) {
              // Removes versions of an entire family prior to the specified timestamp:
              for ((qualifier, series) <- rowQualifierMap.asScala) {
                series.subMap(kv.getTimestamp, true, 0, true).clear()
              }
            } else if (kv.isDeleteColumnOrFamily) {
              // Removes versions of a column prior to the specified timestamp:
              val series = rowQualifierMap.get(kv.getQualifier)
              if (series != null) {
                series.subMap(kv.getTimestamp, true, 0, true).clear()
              }
            } else {
              // Removes exactly one cell:
              val series = rowQualifierMap.get(kv.getQualifier)
              if (series != null) {
                val timestamp = {
                  if (kv.getTimestamp == HConstants.LATEST_TIMESTAMP) {
                    series.firstKey
                  } else {
                    kv.getTimestamp
                  }
                }
                series.remove(timestamp)
              }
            }
          }
        }
        cleanupRow(rowKey = rowKey, family = Some(requestedFamily), qualifier = None)
      }
    }
  }

  override def delete(deletes: JList[Delete]): Unit = {
    deletes.asScala.foreach(this.delete(_))
  }

  override def checkAndDelete(
      row: Bytes,
      family: Bytes,
      qualifier: Bytes,
      value: Bytes,
      delete: Delete
  ): Boolean = {
    synchronized {
      if (checkCell(row = row, family = family, qualifier = qualifier, value = value)) {
        this.delete(delete)
        return true
      } else {
        return false
      }
    }
  }

  override def increment(increment: Increment): Result = {
    synchronized {
      val nowMS = System.currentTimeMillis

      val rowKey = increment.getRow
      val row = rows.asScala
          .getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](Bytes.BYTES_COMPARATOR))
      val familyMap = new JTreeMap[Bytes, NavigableSet[Bytes]](Bytes.BYTES_COMPARATOR)

      for ((family, qualifierMap) <- increment.getFamilyMap.asScala) {
        val qualifierSet = familyMap.asScala
            .getOrElseUpdate(family, new JTreeSet[Bytes](Bytes.BYTES_COMPARATOR))
        val rowQualifierMap = row.asScala
            .getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](Bytes.BYTES_COMPARATOR))

        for ((qualifier, amount) <- qualifierMap.asScala) {
          qualifierSet.add(qualifier)
          val rowTimeSeries = rowQualifierMap.asScala
              .getOrElseUpdate(qualifier, new JTreeMap[Long, Bytes](TimestampComparator))
          val currentCounter = {
            if (rowTimeSeries.isEmpty) {
              0
            } else {
              Bytes.toLong(rowTimeSeries.firstEntry.getValue)
            }
          }
          val newCounter = currentCounter + amount
          Log.debug("Updating counter from %d to %d".format(currentCounter, newCounter))
          rowTimeSeries.put(nowMS, Bytes.toBytes(newCounter))
        }
      }

      return makeResult(
          rowKey = increment.getRow,
          row = row,
          familyMap = familyMap,
          timeRange = increment.getTimeRange,
          maxVersions = 1
      )
    }
  }

  override def incrementColumnValue(
      row: Bytes,
      family: Bytes,
      qualifier: Bytes,
      amount: Long
  ): Long = {
    return this.incrementColumnValue(row, family, qualifier, amount, writeToWAL = true)
  }

  override def incrementColumnValue(
      row: Bytes,
      family: Bytes,
      qualifier: Bytes,
      amount: Long,
      writeToWAL: Boolean
  ): Long = {
    val inc = new Increment(row)
        .addColumn(family, qualifier, amount)
    val result = this.increment(inc)
    require(!result.isEmpty)
    return Bytes.toLong(result.getValue(family, qualifier))
  }

  override def setAutoFlush(autoFlush: Boolean, clearBufferOnFail: Boolean): Unit = {
    this.autoFlush = autoFlush
    // Ignore clearBufferOnFail
  }

  override def setAutoFlush(autoFlush: Boolean): Unit = {
    this.autoFlush = autoFlush
  }

  override def isAutoFlush(): Boolean = {
    return autoFlush
  }

  override def flushCommits(): Unit = {
    // Do nothing
  }

  override def setWriteBufferSize(writeBufferSize: Long): Unit = {
    this.writeBufferSize = writeBufferSize
  }

  override def getWriteBufferSize(): Long = {
    return writeBufferSize
  }

  override def close(): Unit = {
    this.closed = true
  }

  override def lockRow(row: Bytes): RowLock = {
    sys.error("Not implemented")
  }

  override def unlockRow(lock: RowLock): Unit = {
    sys.error("Not implemented")
  }

  override def coprocessorProxy[T <: CoprocessorProtocol](
      protocol: Class[T],
      row: Bytes
  ): T = {
    sys.error("Coprocessor not implemented")
  }

  override def coprocessorExec[T <: CoprocessorProtocol, R](
      protocol: Class[T],
      startKey: Bytes,
      endKey: Bytes,
      callable: Batch.Call[T, R]
  ): JMap[Bytes, R] = {
    sys.error("Coprocessor not implemented")
  }

  override def coprocessorExec[T <: CoprocessorProtocol, R](
      protocol: Class[T],
      startKey: Bytes,
      endKey: Bytes,
      callable: Batch.Call[T, R],
      callback: Batch.Callback[R]
  ): Unit = {
    sys.error("Coprocessor not implemented")
  }

  // -----------------------------------------------------------------------------------------------

  private val BreakToNextColumn = new Breaks()
  private val BreakToNextRow = new Breaks()

  /**
   * Builds an HTable request Result instance.
   *
   * @param rowKey the row key.
   * @param row the actual row data.
   * @param familyMap the requested columns.
   * @param timeRange the timestamp range.
   * @param maxVersions maximum number of versions to return.
   * @param filter optional HBase filter to apply on the KeyValue entries.
   * @return a new Result instance with the specified cells (KeyValue entries).
   */
  private def makeResult(
      rowKey: Bytes,
      row: RowFamilies,
      familyMap: JMap[Bytes, NavigableSet[Bytes]],
      timeRange: TimeRange,
      maxVersions: Int,
      filter: Filter = PassThroughFilter
  ): Result = {
    val kvs: JList[KeyValue] = new JArrayList[KeyValue]()
    val requestedFamilies = if (!familyMap.isEmpty) familyMap.keySet else row.keySet

    BreakToNextRow.breakable {
      for (requestedFamily <- requestedFamilies.asScala) {
        /** Map: qualifier -> time stamp -> cell value */
        val rowQualifierMap = row.get(requestedFamily)
        if (rowQualifierMap != null) {
          val requestedQualifiers = {
            val qset = familyMap.get(requestedFamily)
            if ((qset != null) && !qset.isEmpty) qset else rowQualifierMap.keySet
          }
          for (requestedQualifier <- requestedQualifiers.asScala) {
            /** Map: time stamp -> cell value */
            val rowColumnSeries = rowQualifierMap.get(requestedQualifier)
            if (rowColumnSeries != null) {
              /** Map: time stamp -> cell value */
              val versionMap =
                  rowColumnSeries.subMap(timeRange.getMax, false, timeRange.getMin, true)
              BreakToNextColumn.breakable {
                var nversions = 0
                for ((timestamp, value) <- versionMap.asScalaIterator) {
                  val kv =
                      new KeyValue(rowKey, requestedFamily, requestedQualifier, timestamp, value)
                  filter.filterKeyValue(kv) match {
                  case Filter.ReturnCode.INCLUDE => {
                    kvs.add(filter.transform(kv))
                    nversions += 1
                    if (nversions >= maxVersions) {
                      BreakToNextColumn.break
                    }
                  }
                  case Filter.ReturnCode.SKIP => // Skip this key/value pair.
                  case Filter.ReturnCode.NEXT_COL => BreakToNextColumn.break
                  case Filter.ReturnCode.NEXT_ROW => BreakToNextRow.break
                  case Filter.ReturnCode.SEEK_NEXT_USING_HINT => sys.error("Not implemented")
                  }
                }
              }
            }
          }
        }
      }
    }
    if (filter.hasFilterRow()) {
      filter.filterRow(kvs)
    }
    return new Result(kvs.toArray(new Array[KeyValue](kvs.size)))
  }

  // -----------------------------------------------------------------------------------------------

  /** @return the regions info for this table. */
  private[fakehtable] def getRegions(): JList[HRegionInfo] = {
    val list = new java.util.ArrayList[HRegionInfo]()
    for (region <- regions) {
      list.add(region.getRegionInfo)
    }
    return list
  }

  /**
   * Converts a list of region boundaries (null excluded) into a stream of regions.
   *
   * @param split Region boundaries, first and last null/empty excluded.
   * @return a stream of (start, end) regions.
   */
  private def toRegions(split: Seq[Bytes]): Iterator[(Bytes, Bytes)] = {
    if (!split.isEmpty) {
      require((split.head != null) && !split.head.isEmpty)
      require((split.last != null) && !split.last.isEmpty)
    }
    val startKeys = Iterator(null) ++ split.iterator
    val endKeys = split.iterator ++ Iterator(null)
    return startKeys.zip(endKeys).toIterator
  }

  /**
   * Sets the region splits for this table.
   *
   * @param split Split boundaries (excluding the first and last null/empty).
   */
  private[fakehtable] def setSplit(split: Array[Bytes]): Unit = {
    val fakePort = 1234
    val tableName: Bytes = Bytes.toBytes(name)

    val newRegions = Buffer[HRegionLocation]()
    for ((start, end) <- toRegions(split)) {
      val fakeHost = "fake-location-%d".format(newRegions.size)
      val regionInfo = new HRegionInfo(tableName, start, end)
      newRegions += new HRegionLocation(regionInfo, fakeHost, fakePort)
    }
    this.regions = newRegions.toSeq
  }

  // -----------------------------------------------------------------------------------------------
  // HTable methods that are not part of HTableInterface, but required anyway:

  /** See HTable.getRegionLocation(). */
  def getRegionLocation(row: String): HRegionLocation = {
    return getRegionLocation(Bytes.toBytes(row))
  }

  /** See HTable.getRegionLocation(). */
  def getRegionLocation(row: Bytes): HRegionLocation = {
    return getRegionLocation(row, false)
  }

  /** See HTable.getRegionLocation(). */
  def getRegionLocation(row: Bytes, reload: Boolean): HRegionLocation = {
    for (region <- regions) {
      val start = region.getRegionInfo.getStartKey
      val end = region.getRegionInfo.getEndKey
      // start ≤ row < end:
      if ((Bytes.compareTo(start, row) <= 0)
          && (end.isEmpty || (Bytes.compareTo(row, end) < 0))) {
        return region
      }
    }
    sys.error("Invalid region split: last region must does not end with empty row key")
  }

  /** See HTable.getRegionLocations(). */
  def getRegionLocations(): NavigableMap[HRegionInfo, ServerName] = {
    val map = new JTreeMap[HRegionInfo, ServerName]()
    for (region <- regions) {
      map.put(region.getRegionInfo, new ServerName(region.getHostname, region.getPort, 0))
    }
    return map
  }

  // -----------------------------------------------------------------------------------------------

  def toHex(bytes: Bytes): String = {
    return bytes.iterator
        .map { byte => "%02x".format(byte) }
        .mkString(":")
  }

  def toString(bytes: Bytes): String = {
    return Bytes.toStringBinary(bytes)
  }

  /**
   * Dumps the content of the fake HTable.
   *
   * @param out Optional print stream to write to.
   */
  def dump(out: PrintStream = Console.out): Unit = {
    for ((rowKey, familyMap) <- rows.asScalaIterator) {
      for ((family, qualifierMap) <- familyMap.asScalaIterator) {
        for ((qualifier, timeSeries) <- qualifierMap.asScalaIterator) {
          for ((timestamp, value) <- timeSeries.asScalaIterator) {
            out.println("row=%s family=%s qualifier=%s timestamp=%d value=%s".format(
                toString(rowKey),
                toString(family),
                toString(qualifier),
                timestamp,
                toHex(value)))
          }
        }
      }
    }
  }

  /**
   * Instantiates the specified HBase filter.
   *
   * @param filterSpec HBase filter specification, or null for no filter.
   * @return a new instance of the specified filter.
   */
  private def getFilter(filterSpec: Filter): Filter = {
    Option(filterSpec) match {
      case Some(hfilter) => WritableUtils.clone(hfilter, conf)
      case None => PassThroughFilter
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * ResultScanner for a fake in-memory HTable.
   *
   * @param scan Scan options.
   */
  private class FakeResultScanner(
      val scan: Scan
  ) extends ResultScanner with JIterator[Result] {

    /** Key of the row to return on the next call to next(). Null means no more row. */
    private var key: Bytes = {
      if (rows.isEmpty) {
        null
      } else if (scan.getStartRow.isEmpty) {
        rows.firstKey
      } else {
        rows.floorKey(scan.getStartRow)
      }
    }
    if (!scan.getStopRow.isEmpty
       && (Bytes.BYTES_COMPARATOR.compare(key, scan.getStopRow) >= 0)) {
      key = null
    }

    /** HBase row/column filter. */
    val filter = getFilter(scan.getFilter)

    /** Next result to return. */
    private var nextResult: Result = getNextResult()

    override def hasNext(): Boolean = {
      return (nextResult != null)
    }

    override def next(): Result = {
      val result = nextResult
      nextResult = getNextResult()
      return result
    }

    /** @return the next non-empty result. */
    private def getNextResult(): Result = {
      while (true) {
        getResultForNextRow() match {
          case None => return null
          case Some(result) => {
            if (!result.isEmpty) {
              return result
            }
          }
        }
      }
      // next() returns when a non empty Result is found or when there are no more rows:
      sys.error("dead code")
    }

    /** @return the next row key, or null if there is no more row. */
    private def nextRowKey(): Bytes = {
      if (key == null) { return null }
      val rowKey = key
      key = rows.higherKey(rowKey)
      if ((key != null)
          && !scan.getStopRow.isEmpty
          && (Bytes.BYTES_COMPARATOR.compare(key, scan.getStopRow) >= 0)) {
        key = null
      }
      return rowKey
    }

    /** @return a Result, potentially empty, for the next row. */
    private def getResultForNextRow(): Option[Result] = {
      filter.reset()
      if (filter.filterAllRemaining) { return None }

      val rowKey = nextRowKey()
      if (rowKey == null) { return None }
      if (filter.filterRowKey(rowKey, 0, rowKey.size)) {
        // Row is filtered out based on its key, return an empty Result:
        return Some(new Result())
      }

      /** Map: family -> qualifier -> time stamp -> cell value */
      val row = rows.get(rowKey)
      require(row != null)

      val result = makeResult(
          rowKey = rowKey,
          row = row,
          familyMap = scan.getFamilyMap,
          timeRange = scan.getTimeRange,
          maxVersions = scan.getMaxVersions,
          filter = filter
      )
      if (filter.filterRow()) {
        // Filter finally decided to exclude the row, return an empty Result:
        return Some(new Result())
      }
      return Some(result)
    }

    override def next(nrows: Int): Array[Result] = {
      val results = Buffer[Result]()
      breakable {
        for (nrow <- 0 until nrows) {
          next() match {
            case null => break
            case row => results += row
          }
        }
      }
      return results.toArray
    }

    override def close(): Unit = {
      // Nothing to close
    }

    override def iterator(): JIterator[Result] = {
      return this
    }

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }
  }

  // -----------------------------------------------------------------------------------------------

}
