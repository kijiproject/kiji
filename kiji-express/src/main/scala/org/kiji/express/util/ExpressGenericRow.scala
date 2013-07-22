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

package org.kiji.express.util

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiCellDecoder
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiRowData
import org.kiji.schema.impl.HBaseKijiRowData

/**
 * Provides access to a row from Kiji using the provided decoders. ExpressGenericRow is constructed
 * by ExpressGenericTable, as a temporary way to provided generic access to a KijiRowData, with
 * GenericCellDecoders.  Eventually kiji-schema should be able to provide this directly from a
 * KijiRowData and we can eliminate this wrapper layer.
 *
 * @param rowData to wrap.
 * @param decoders is the map from column to the decoder to use for cells in that column.
 */
@ApiAudience.Private
private[express] final class ExpressGenericRow(
    rowData: KijiRowData,
    decoders: Map[KijiColumnName, KijiCellDecoder[_]]) {

  /** The raw bytes of this row, as gotten from the KijiRowData. */
  val row: Map[String, Map[String, Map[Long, Array[Byte]]]] =
      ExpressGenericRow.rowToScalaMap(rowData)

  /**
   * Gets an iterator over all cells for the specified column family, using the specified decoders.
   *
   * @param family to iterate over.
   * @return iterator of decoded KijiCells in this family.
   */
  def iterator[T](family: String): Iterator[KijiCell[T]] = {
    row(family).map {
      case (qualifier, tsMap) => {
        tsMap.map {
          case (ts, bytes) => {
            val decodedCell =
                decoders(new KijiColumnName(family))
                    .decodeCell(bytes)
                    .asInstanceOf[DecodedCell[T]]
            new KijiCell(family, qualifier, ts, decodedCell)
          }
        }
      // Sort by qualifier first, and reverse timestamp order second.
      }.toSeq.sortBy (cell => (cell.getQualifier, - cell.getTimestamp))
    }
        .reduce(_ ++ _)
        .toIterator
  }

  /**
   * Gets an iterator over all cells for the specified column, using generic API.
   *
   * @param family of the column to iterate over.
   * @param qualifier of the column to iterate over.
   * @return of KijiCells in this column.
   */
  def iterator[T](
      family: String,
      qualifier: String): Iterator[KijiCell[T]] = {
    row(family)(qualifier).map {
      case (ts, bytes) => {
        val decodedCell =
            decoders(new KijiColumnName(family, qualifier))
                .decodeCell(bytes)
                .asInstanceOf[DecodedCell[T]]
        new KijiCell(family, qualifier, ts, decodedCell)
      }
    }
        .toSeq.sortBy (cell => cell.getTimestamp).reverse
        .toIterator
  }
}

/**
 * Companion object containing utility methods for ExpressGenericRow.
 */
object ExpressGenericRow {
  /**
   * Converts a KijiRowData to a Scala Map.
   *
   * @param rowData to convert.
   * @return Scala map from family to qualifier to timestamp to bytes.
   */
  private[express] def rowToScalaMap(rowData: KijiRowData)
      : Map[String, Map[String, Map[Long, Array[Byte]]]] = {
    val rowMap = rowData.asInstanceOf[HBaseKijiRowData].getMap()

    // Convert family map
    rowMap.asScala.mapValues(
        // Convert qualifier map
        qualMap => qualMap.asScala.mapValues(
            // Convert timestamp map.
            tsMap => tsMap.asScala.map {
                case (ts, bytes) => (ts.longValue, bytes)
            }.toMap
        ).toMap
    ).toMap
  }
}
