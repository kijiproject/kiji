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
import java.lang.{Boolean => JBoolean}
import java.lang.{Long => JLong}
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
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Row
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HRegionLocation
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.ServerName
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Append
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Durability
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.RowMutations
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel
import org.apache.hadoop.hbase.util.Bytes
import org.kiji.testing.fakehtable.JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator
import org.slf4j.LoggerFactory

/**
 * Fake in-memory HTable.
 *
 * @param name is the table name.
 * @param desc is the table HBase descriptor.
 *     Optional and may currently be null (the fake HTable infers descriptors as needed).
 * @param conf is the HBase configuration.
 * @param autoFlush is the initial value for the table auto-flush property.
 * @param enabled is the initial state of the table.
 * @param autoFillDesc indicates whether descriptors are required or automatically filled-in.
 * @param hconnection Fake HConnection for this HTable.
 */
class FakeHTable(
    val name: String,
    desc: HTableDescriptor,
    val conf: Configuration = HBaseConfiguration.create(),
    private var autoFlush: Boolean = false,
    private var writeBufferSize: Long = 1,
    var enabled: Boolean = true,
    autoFillDesc: Boolean = true,
    hconnection: FakeHConnection = new FakeHConnection(null)
) extends HTableInterface
    with FakeTypes {
  private val Log = LoggerFactory.getLogger(getClass)
  require(conf != null)

  /** Whether the table has been closed. */
  private var closed: Boolean = false

  /** A fake connection. */
  private val mFakeHConnection: FakeHConnection = hconnection
  private val mHConnection: HConnection =
      UntypedProxy.create(classOf[HConnection], mFakeHConnection)

  /** Region splits and locations. Protected by `this`. */
  private var regions: Seq[HRegionLocation] = Seq()

  /** Comparator for Bytes. */
  private final val BytesComparator: java.util.Comparator[Bytes] = Bytes.BYTES_COMPARATOR

  /** Map: row key -> family -> qualifier -> timestamp -> cell data. */
  private val rows: Table = new JTreeMap[Bytes, RowFamilies](BytesComparator)

  // -----------------------------------------------------------------------------------------------

  /** HBase descriptor of the table. */
  private val mDesc: HTableDescriptor = {
    desc match {
      case null => {
        require(autoFillDesc)
        new HTableDescriptor(name)
      }
      case desc => desc
    }
  }

  override def getTableName(): Array[Byte] = {
    return name.getBytes
  }

  override def getConfiguration(): Configuration = {
    return conf
  }

  override def getTableDescriptor(): HTableDescriptor = {
    return mDesc
  }

  override def exists(get: Get): Boolean = {
    return !this.get(get).isEmpty()
  }

  override def append(append: Append): Result = {
    /** Key values to return as a result. */
    val resultKVs = Buffer[KeyValue]()

    synchronized {
      val row = rows.get(append.getRow)

      /**
       * Gets the current value for the specified cell.
       *
       * @param kv contains the coordinate of the cell to report the current value of.
       * @return the current value of the specified cell, or null if the cell does not exist.
       */
      def getCurrentValue(kv: KeyValue): Bytes = {
        if (row == null) return null
        val family = row.get(kv.getFamily)
        if (family == null) return null
        val qualifier = family.get(kv.getQualifier)
        if (qualifier == null) return null
        val entry = qualifier.firstEntry
        if (entry == null) return null
        return entry.getValue
      }

      /** Build a put request with the appended cells. */
      val put = new Put(append.getRow)

      val timestamp = System.currentTimeMillis

      for ((family, kvs) <- append.getFamilyMap.asScala) {
        for (kv <- kvs.asScala) {
          val currentValue: Bytes = getCurrentValue(kv)
          val newValue: Bytes = {
            if (currentValue == null) kv.getValue else (currentValue ++ kv.getValue)
          }
          val appendedKV =
              new KeyValue(kv.getRow, kv.getFamily, kv.getQualifier, timestamp, newValue)
          put.add(appendedKV)

          if (append.isReturnResults) resultKVs += appendedKV.clone()
        }
      }
      this.put(put)
    }
    return new Result(resultKVs.toArray)
  }

  override def batch(actions: JList[_ <: Row], results: Array[Object]): Unit = {
    require(results.size == actions.size)
    val array = batch(actions)
    System.arraycopy(array, 0, results, 0, results.length)
  }

  override def batch(actions: JList[_ <: Row]): Array[Object] = {
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
        case append: Append => {
          results += this.append(append)
        }
        case increment: Increment => {
          results += this.increment(increment)
        }
        case mutations: RowMutations => {
          this.mutateRow(mutations)
        }
      }
    }
    return results.toArray
  }

  override def get(get: Get): Result = {
    // get() could be built around scan(), to ensure consistent filters behavior.
    // For now, we use a shortcut:
    synchronized {
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
      val result = ProcessRow.makeResult(
        table = this,
        rowKey = rowKey,
        row = row,
        familyMap = getFamilyMapRequest(get.getFamilyMap),
        timeRange = get.getTimeRange,
        maxVersions = get.getMaxVersions,
        filter = filter
      )
      if (filter.filterRow()) {
        return new Result()
      }
      return result
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
          .getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](BytesComparator))
      for ((family, kvs) <- put.getFamilyMap.asScala) {
        /** Map: qualifier -> time series. */
        val rowQualifierMap = rowFamilyMap.asScala
            .getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](BytesComparator))

        for (kv <- kvs.asScala) {
          require(Arrays.equals(family, kv.getFamily))

          /** Map: timestamp -> value. */
          val column = rowQualifierMap.asScala
              .getOrElseUpdate(kv.getQualifier, new JTreeMap[JLong, Bytes](TimestampComparator))

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
   * Checks the value of a cell. Caller <em>must</em> synchronize before calling.
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
   * Removes empty maps for a specified row, family and/or qualifier. Caller <em>must</em>
   * synchronize before calling.
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
          .getOrElseUpdate(rowKey, new JTreeMap[Bytes, FamilyQualifiers](BytesComparator))
      val familyMap = new JTreeMap[Bytes, NavigableSet[Bytes]](BytesComparator)

      for ((family: Array[Byte], qualifierMap: JList[Cell]) <- increment.getFamilyCellMap.asScala) {
        val qualifierSet = familyMap.asScala
            .getOrElseUpdate(family, new JTreeSet[Bytes](BytesComparator))
        val rowQualifierMap = row.asScala
            .getOrElseUpdate(family, new JTreeMap[Bytes, ColumnSeries](BytesComparator))

        for (cell: Cell <- qualifierMap.asScala) {
          val qualifier = CellUtil.cloneQualifier(cell)
          val amount = Bytes.toLong(CellUtil.cloneValue(cell))
          qualifierSet.add(qualifier)
          val rowTimeSeries = rowQualifierMap.asScala
              .getOrElseUpdate(qualifier, new JTreeMap[JLong, Bytes](TimestampComparator))
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

      return ProcessRow.makeResult(
          table = this,
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

  override def mutateRow(mutations: RowMutations): Unit = {
    synchronized {
      for (mutation <- mutations.getMutations.asScala) {
        mutation match {
          case put: Put => this.put(put)
          case delete: Delete => this.delete(delete)
          case _ => sys.error("Unexpected row mutation: " + mutation)
        }
      }
    }
  }

  override def setAutoFlush(autoFlush: Boolean, clearBufferOnFail: Boolean): Unit = {
    synchronized {
      this.autoFlush = autoFlush
      // Ignore clearBufferOnFail
    }
  }

  override def setAutoFlush(autoFlush: Boolean): Unit = {
    synchronized {
      this.autoFlush = autoFlush
    }
  }

  override def isAutoFlush(): Boolean = {
    synchronized {
      return autoFlush
    }
  }

  override def flushCommits(): Unit = {
    // Do nothing
  }

  override def setWriteBufferSize(writeBufferSize: Long): Unit = {
    synchronized {
      this.writeBufferSize = writeBufferSize
    }
  }

  override def getWriteBufferSize(): Long = {
    synchronized {
      return writeBufferSize
    }
  }

  override def close(): Unit = {
    synchronized {
      this.closed = true
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** @return the regions info for this table. */
  private[fakehtable] def getRegions(): JList[HRegionInfo] = {
    val list = new java.util.ArrayList[HRegionInfo]()
    synchronized {
      for (region <- regions) {
        list.add(region.getRegionInfo)
      }
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
      val regionInfo = new HRegionInfo(TableName.valueOf(tableName), start, end)
      val seqNum = System.currentTimeMillis()
      newRegions += new HRegionLocation(
          regionInfo,
          ServerName.valueOf(fakeHost, fakePort, /* startCode = */ 0),
          /* seqNum = */ 0
      )
    }
    synchronized {
      this.regions = newRegions.toSeq
    }
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
    synchronized {
      for (region <- regions) {
        val start = region.getRegionInfo.getStartKey
        val end = region.getRegionInfo.getEndKey
        // start ≤ row < end:
        if ((Bytes.compareTo(start, row) <= 0)
            && (end.isEmpty || (Bytes.compareTo(row, end) < 0))) {
          return region
        }
      }
    }
    sys.error("Invalid region split: last region must does not end with empty row key")
  }

  /** See HTable.getRegionLocations(). */
  def getRegionLocations(): NavigableMap[HRegionInfo, ServerName] = {
    val map = new JTreeMap[HRegionInfo, ServerName]()
    synchronized {
      for (region <- regions) {
        map.put(region.getRegionInfo, new ServerName(region.getHostname, region.getPort, 0))
      }
    }
    return map
  }

  /**
   * See HTable.getRegionsInRange(startKey, endKey).
   *
   * Adapted from org.apache.hadoop.hbase.client.HTable.
   * Note: if startKey == endKey, this returns the location for startKey.
   */
  def getRegionsInRange(startKey: Bytes, endKey: Bytes): JList[HRegionLocation] = {
    val endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW)
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: %s > %s".format(
          Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey)))
    }
    val regionList = new JArrayList[HRegionLocation]()
    var currentKey = startKey
    do {
      val regionLocation = getRegionLocation(currentKey, false)
      regionList.add(regionLocation)
      currentKey = regionLocation.getRegionInfo().getEndKey()
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
        && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0))
    return regionList
  }

  /** See HTable.getConnection(). */
  def getConnection(): HConnection = {
    mHConnection
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
    synchronized {
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
  }

  /**
   * Instantiates the specified HBase filter.
   *
   * @param filterSpec HBase filter specification, or null for no filter.
   * @return a new instance of the specified filter.
   */
  private def getFilter(filterSpec: Filter): Filter = {
    Option(filterSpec) match {
      case Some(hfilter) => ProtobufUtil.toFilter(ProtobufUtil.toFilter(hfilter))
      case None => PassThroughFilter
    }
  }

  /**
   * Ensures a family map from a Get/Scan is sorted.
   *
   * @param request Requested family/qualifier map.
   * @return The requested family/qualifiers, as a sorted map.
   */
  private def getFamilyMapRequest(
      request: JMap[Bytes, NavigableSet[Bytes]]
  ): NavigableMap[Bytes, NavigableSet[Bytes]] = {
    if (request.isInstanceOf[NavigableMap[_, _]]) {
      return request.asInstanceOf[NavigableMap[Bytes, NavigableSet[Bytes]]]
    }
    val map = new JTreeMap[Bytes, NavigableSet[Bytes]](BytesComparator)
    for ((family, qualifiers) <- request.asScala) {
      map.put(family, qualifiers)
    }
    return map
  }

  /**
   * Reports the HBase descriptor for a column family.
   *
   * @param family is the column family to report the descriptor of.
   * @return the descriptor of the specified column family.
   */
  private[fakehtable] def getFamilyDesc(family: Bytes): HColumnDescriptor = {
    val desc = getTableDescriptor()
    val familyDesc = desc.getFamily(family)
    if (familyDesc != null) {
      return familyDesc
    }
    require(autoFillDesc)
    val newFamilyDesc = new HColumnDescriptor(family)
    // Note on default parameters:
    //  - min versions is 0
    //  - max versions is 3
    //  - TTL is forever
    desc.addFamily(newFamilyDesc)
    return newFamilyDesc
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

    /** Requested family/qualifiers map. */
    private val requestedFamilyMap: NavigableMap[Bytes, NavigableSet[Bytes]] =
        getFamilyMapRequest(scan.getFamilyMap)

    /** Key of the row to return on the next call to next(). Null means no more row. */
    private var key: Bytes = {
      synchronized {
        if (rows.isEmpty) {
          null
        } else if (scan.getStartRow.isEmpty) {
          rows.firstKey
        } else {
          rows.ceilingKey(scan.getStartRow)
        }
      }
    }
    if (!scan.getStopRow.isEmpty
       && (key == null || BytesComparator.compare(key, scan.getStopRow) >= 0)) {
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

    /**
     * @return the next row key, or null if there is no more row. Caller <em>must</em> synchronize
     * on `FakeHTable.this`.
     */
    private def nextRowKey(): Bytes = {
      if (key == null) { return null }
      val rowKey = key
      key = rows.higherKey(rowKey)
      if ((key != null)
          && !scan.getStopRow.isEmpty
          && (BytesComparator.compare(key, scan.getStopRow) >= 0)) {
        key = null
      }
      return rowKey
    }

    /** @return a Result, potentially empty, for the next row. */
    private def getResultForNextRow(): Option[Result] = {
      FakeHTable.this.synchronized {
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

        val result = ProcessRow.makeResult(
          table = FakeHTable.this,
          rowKey = rowKey,
          row = row,
          familyMap = requestedFamilyMap,
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

  override def batchCallback[R](
      x$1: JList[_ <: Row],
      x$2: Batch.Callback[R]
  ): Array[Object] = {
    sys.error("Not implemented")
  }

  override def batchCallback[R](
      x$1: java.util.List[_ <: Row],
      x$2: Array[Object],
      x$3: Batch.Callback[R]
  ): Unit = { sys.error("Not implemented") }

  override def coprocessorService[T <: com.google.protobuf.Service, R](
    x$1: Class[T],
    x$2: Array[Byte],
    x$3: Array[Byte],
    x$4: Batch.Call[T, R],
    x$5: Batch.Callback[R]
  ):Unit = {
    sys.error("Not implemented")
  }

  override def coprocessorService[T <: com.google.protobuf.Service, R](
      x$1: Class[T],
      x$2: Array[Byte],
      x$3: Array[Byte],
      x$4: Batch.Call[T, R]
  ):java.util.Map[Array[Byte],R] = {
    sys.error("Not implemented")
  }

  override def coprocessorService(x$1: Array[Byte]): CoprocessorRpcChannel = {
    sys.error("Not implemented")
  }

  override def exists(gets: JList[Get]): Array[JBoolean] = {
    val exists: Array[JBoolean] = new Array[JBoolean](gets.size)
    synchronized {
      for (index <- 0 until gets.size) {
        exists(index) = this.exists(gets.get(index))
      }
    }
    exists
  }

  override def getName(): TableName = {
    TableName.valueOf(mDesc.getName)
  }

  override def incrementColumnValue(
      x$1: Array[Byte],
      x$2: Array[Byte],
      x$3: Array[Byte],
      x$4: Long,
      x$5: Durability
  ): Long = {
    sys.error("Not implemented")
  }

  override def setAutoFlushTo(x$1: Boolean):Unit = {
    sys.error("Not implemented")
  }

  // -----------------------------------------------------------------------------------------------

}
