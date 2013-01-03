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

import java.util.{Iterator => JIterator}
import java.util.{Map => JMap}
import java.util.NavigableSet
import java.util.Arrays
import java.util.{List => JList}
import java.util.NavigableMap
import java.util.{TreeMap => JTreeMap}
import java.util.{TreeSet => JTreeSet}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.Buffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.coprocessor.Batch
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
import org.apache.hadoop.hbase.io.TimeRange
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.KeyValue
import org.slf4j.LoggerFactory
import org.kiji.testing.fakehtable.JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator;
import java.io.PrintStream

/**
 * Fake in-memory HTable.
 *
 * @param name Table name.
 * @param conf Table configuration.
 * @param desc Table descriptor.
 * @param autoFlush auto-flush flag.
 * @param enabled Whether the table is enabled.
 */
class FakeHTable(
    val name: String,
    val conf: Configuration,
    val desc: HTableDescriptor,
    private var autoFlush: Boolean = false,
    private var writeBufferSize: Long = 1,
    var enabled: Boolean = true
) extends HTableInterface {
  private val Log = LoggerFactory.getLogger(getClass)

  /** Whether the table has been closed. */
  private var closed: Boolean = false

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
    val rowKey = get.getRow
    val row = rows.get(rowKey)
    if (row == null) {
      return new Result()  // empty result
    } else {
      return makeResult(
          rowKey = rowKey,
          row = row,
          familyMap = get.getFamilyMap,
          timeRange = get.getTimeRange,
          maxVersions = get.getMaxVersions
      )
    }
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
    for (family <- families) {
      val rowQualifierMap = row.get(family)
      if (rowQualifierMap == null) { return }

      val qualifiers : Iterable[Bytes] = qualifier match {
        case Some(_) => qualifier
        case None => rowQualifierMap.keySet.asScala
      }
      for (qualifier <- qualifiers) {
        val timeSerie = rowQualifierMap.get(qualifier)
        if (timeSerie != null) {
          if (timeSerie.isEmpty) {
            rowQualifierMap.remove(qualifier)
          }
        }
      }

      if (rowQualifierMap.isEmpty) {
        rows.remove(rowKey)
      }
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

  /**
   * Builds an HTable request Result instance.
   *
   * @param rowKey the row key.
   * @param row the actual row data.
   * @param familyMap the requested columns.
   * @param timeRange the timestamp range.
   * @param maxVersions maximum number of versions to return.
   * @return a new Result instance with the specified cells (KeyValue entries).
   */
  private def makeResult(
      rowKey: Bytes,
      row: RowFamilies,
      familyMap: JMap[Bytes, NavigableSet[Bytes]],
      timeRange: TimeRange,
      maxVersions: Int
  ): Result = {
    val kvs = Buffer[KeyValue]()
    val requestedFamilies = if (!familyMap.isEmpty) familyMap.keySet else row.keySet
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
            val versionMap = rowColumnSeries.subMap(timeRange.getMax, false, timeRange.getMin, true)
            breakable {
              for ((timestamp, value) <- versionMap.asScalaIterator) {
                val kv = new KeyValue(rowKey, requestedFamily, requestedQualifier, timestamp, value)
                kvs += kv
                if (kvs.size >= maxVersions) {
                  break
                }
              }
            }
          }
        }
      }
    }
    return new Result(kvs.toArray)
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

    override def hasNext(): Boolean = {
      return (key != null)
    }

    override def next(): Result = {
      if (key == null) { return null }

      /** Map: family -> qualifier -> time stamp -> cell value */
      val rowKey = key
      val row = rows.get(key)
      require(row != null)

      // Advance to next key
      key = rows.higherKey(key)
      if ((key != null)
          && !scan.getStopRow.isEmpty
          && (Bytes.BYTES_COMPARATOR.compare(key, scan.getStopRow) >= 0)) {
        key = null
      }

      return makeResult(
          rowKey = rowKey,
          row = row,
          familyMap = scan.getFamilyMap,
          timeRange = scan.getTimeRange,
          maxVersions = scan.getMaxVersions
      )
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
