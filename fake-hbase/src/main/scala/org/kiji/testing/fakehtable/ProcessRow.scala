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

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList}
import java.util.Arrays
import java.util.{List => JList}
import java.util.NavigableMap
import java.util.NavigableSet

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.io.TimeRange
import org.kiji.testing.fakehtable.JNavigableMapWithAsScalaIterator.javaNavigableMapAsScalaIterator
import org.slf4j.LoggerFactory

/**
 * Utility object providing the logic to prepare HBase Result instances.
 *
 * FakeHTable is the only intended object of this class.
 */
object ProcessRow extends FakeTypes {
  private final val Log = LoggerFactory.getLogger("ProcessRow")

  /** Comparator for KeyValue instances. */
  private final val KeyValueComparator = KeyValue.COMPARATOR

  /** Empty array of bytes. */
  final val EmptyBytes = Array[Byte]()

  private final val FamilyLoop = new Loop()
  private final val QualifierLoop = new Loop()
  private final val TimestampLoop = new Loop()

  /** Empty KeyValue (empty row, empty family, empty qualifier, latest timestamp, empty value. */
  private final val EmptyKeyValue =
      new KeyValue(EmptyBytes, EmptyBytes, EmptyBytes, HConstants.LATEST_TIMESTAMP, EmptyBytes)

  /**
   * Builds a Result instance from an actual row content and a get/scan request.
   *
   * @param table is the fake HTable.
   * @param rowKey is the row key.
   * @param row is the actual row data.
   * @param familyMap is the requested columns.
   * @param timeRange is the timestamp range.
   * @param maxVersions is the maximum number of versions to return.
   * @param filter is an optional HBase filter to apply on the KeyValue entries.
   * @return a new Result instance with the specified cells (KeyValue entries).
   */
  def makeResult(
      table: FakeHTable,
      rowKey: Bytes,
      row: RowFamilies,
      familyMap: NavigableMap[Bytes, NavigableSet[Bytes]],
      timeRange: TimeRange,
      maxVersions: Int,
      filter: Filter = PassThroughFilter
  ): Result = {

    val cells: JList[Cell] = new JArrayList[Cell]

    /** Current time, in milliseconds, to enforce TTL. */
    val nowMS = System.currentTimeMillis

    /**
     * Iteration start key.
     * Might be updated by filters (see SEEK_NEXT_USING_HINT)
     */
    var start: Cell = EmptyKeyValue

    /** Ordered set of families to iterate through. */
    val families: NavigableSet[Bytes] =
        if (!familyMap.isEmpty) familyMap.navigableKeySet else row.navigableKeySet

    /** Iterator over families. */
    var family: Bytes = families.ceiling(CellUtil.cloneFamily(start))

    FamilyLoop {
      if (family == null) FamilyLoop.break

      /** Map: qualifier -> time series. */
      val rowQMap = row.get(family)
      if (rowQMap == null) {
        family = families.higher(family)
        FamilyLoop.continue
      }

      // Apply table parameter (TTL, max/min versions):
      {
        val familyDesc: HColumnDescriptor = table.getFamilyDesc(family)

        val maxVersions = familyDesc.getMaxVersions
        val minVersions = familyDesc.getMinVersions
        val minTimestamp = nowMS - (familyDesc.getTimeToLive * 1000L)

        for ((qualifier, timeSeries) <- rowQMap.asScalaIterator) {
          while (timeSeries.size > maxVersions) {
            timeSeries.remove(timeSeries.lastKey)
          }
          if (familyDesc.getTimeToLive != HConstants.FOREVER) {
            while ((timeSeries.size > minVersions)
                && (timeSeries.lastKey < minTimestamp)) {
              timeSeries.remove(timeSeries.lastKey)
            }
          }
        }
      }

      /** Ordered set of qualifiers to iterate through. */
      val qualifiers: NavigableSet[Bytes] = {
        val reqQSet = familyMap.get(family)
        (if ((reqQSet == null) || reqQSet.isEmpty) rowQMap.navigableKeySet else reqQSet)
      }

      /** Iterator over the qualifiers. */
      var qualifier: Bytes = qualifiers.ceiling(CellUtil.cloneQualifier(start))
      QualifierLoop {
        if (qualifier == null) QualifierLoop.break

        /** NavigableMap: timestamp -> cell content (Bytes). */
        val series: ColumnSeries = rowQMap.get(qualifier)
        if (series == null) {
          qualifier = qualifiers.higher(qualifier)
          QualifierLoop.continue
        }

        /** Map: timestamp -> cell value */
        val versionMap = series.subMap(timeRange.getMax, false, timeRange.getMin, true)

        /** Ordered set of timestamps to iterate through. */
        val timestamps: NavigableSet[JLong] = versionMap.navigableKeySet

        /** Counter to enforce the max-versions per qualifier (post-filtering). */
        var nversions = 0

        /** Iterator over the timestamps (decreasing order, ie. most recent first). */
        var timestamp: JLong = timestamps.ceiling(start.getTimestamp.asInstanceOf[JLong])
        TimestampLoop {
          if (timestamp == null) TimestampLoop.break

          val value: Bytes = versionMap.get(timestamp)
          val kv: KeyValue = new KeyValue(rowKey, family, qualifier, timestamp, value)

          // Apply filter:
          filter.filterKeyValue(kv) match {
            case Filter.ReturnCode.INCLUDE => {
              cells.add(filter.transformCell(kv))

              // Max-version per qualifier happens after filtering:
              // Note that this doesn't take into account the transformed KeyValue.
              // It is not clear from the documentation whether filter.transform()
              // may alter the qualifier of the KeyValue.
              // Also, this doesn't take into account the final modifications resulting from
              // filter.filterRow(kvs).
              nversions += 1
              if (nversions >= maxVersions) {
                TimestampLoop.break
              }
            }
            case Filter.ReturnCode.INCLUDE_AND_NEXT_COL => {
              cells.add(filter.transformCell(kv))
              // No need to check the max-version per qualifier,
              // since this jumps to the next column directly.
              TimestampLoop.break
            }
            case Filter.ReturnCode.SKIP => // Skip this key/value pair.
            case Filter.ReturnCode.NEXT_COL => TimestampLoop.break
            case Filter.ReturnCode.NEXT_ROW => {
              // Semantically, NEXT_ROW apparently means NEXT_FAMILY
              QualifierLoop.break
            }
            case Filter.ReturnCode.SEEK_NEXT_USING_HINT => {
              Option(filter.getNextCellHint(kv)) match {
                case None => // No hint
                case Some(hint) => {
                  require(KeyValueComparator.compare(kv, hint) < 0,
                      "Filter hint cannot go backward from %s to %s".format(kv, hint))
                  if (!Arrays.equals(rowKey, hint.getRow)) {
                    // hint references another row.
                    //
                    // For now, this just moves to the next row and the remaining of the
                    // hint (family/qualifier) is ignored.
                    // Not sure it makes sense to jump at a specific family/qualifier in
                    // a different row from a column filter,
                    // but the documentation does not prevent it.
                    FamilyLoop.break
                  }
                  start = hint
                  if (!Arrays.equals(family, hint.getFamily)) {
                    // Restart the family iterator:
                    family = families.ceiling(hint.getFamily)
                    FamilyLoop.continue
                  } else if (!Arrays.equals(qualifier, hint.getQualifier)) {
                    qualifier = qualifiers.ceiling(hint.getQualifier)
                    QualifierLoop.continue
                  } else {
                    // hint jumps to an older timestamp for the current family/qualifier:
                    timestamp = timestamps.higher(hint.getTimestamp.asInstanceOf[JLong])
                    TimestampLoop.continue
                  }
                }
              }
            }
          }  // apply filter

          timestamp = timestamps.higher(timestamp).asInstanceOf[JLong]
        }  // TimestampLoop

        qualifier = qualifiers.higher(qualifier)
      }  // QualifierLoop

      family = families.higher(family)
    }  // FamilyLoop

    if (filter.hasFilterRow()) {
      filter.filterRowCells(cells)
    }
    return Result.create(cells)
  }

}