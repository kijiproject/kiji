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

import org.apache.hadoop.conf.Configuration
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience;
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.GenericCellDecoderFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiCellDecoder
import org.kiji.schema.KijiCellDecoderFactory
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiSchemaTable
import org.kiji.schema.layout.CellSpec
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.util.ResourceUtils

/**
 * An Express construct that can decode rows from Kiji with a generic API.
 *
 * This is entirely in-memory. No paging will be supported.  Eventually we should patch kiji-schema
 * to allow decoding a generic cell from a rowdata.
 *
 * @param tableUri of the table to represent.
 * @param configuration the job configuration
 * @param columns in this table that requested rows will include.
 */
@ApiAudience.Private
private[express] final class ExpressGenericTable(tableUri: KijiURI, configuration: Configuration,
    columns: Seq[KijiColumnName]) {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ExpressGenericTable])

  logger.debug("ExpressGenericTable being initialized for table %s and columns %s".format(
      tableUri, columns))
  val kiji: Kiji = Kiji.Factory.open(tableUri, configuration)
  val schemaTable: KijiSchemaTable = kiji.getSchemaTable()

  /** Mapping from column name to cell spec. */
  val cellSpecMap: Map[KijiColumnName, CellSpec] =
      doAndRelease(kiji.openTable(tableUri.getTable())) {
        table: KijiTable => {
          val tableLayout: KijiTableLayout = table.getLayout
          // Add the CellSpec for each column to the column mapping.
          val cellSpecMap: Map[KijiColumnName, CellSpec] =
              columns.map {
                column => {
                  // Build the CellSpec with the necessary schemaTable
                  val cellSchema = tableLayout.getCellSchema(column)
                  // scalastyle:off null
                  val cellSpec = CellSpec
                      .fromCellSchema(cellSchema, schemaTable)
                      .setReaderSchema(null)
                  // scalastyle:on null
                  (column, cellSpec)
                }
              }.toMap
          cellSpecMap
        }
      }

  /** GenericCellDecoder factory for use in ExpressRowData. */
  val decoderFactory: KijiCellDecoderFactory = GenericCellDecoderFactory.get()

  /** A map from columns to their decoders. All the decoders should be generic. */
  val decoders: Map[KijiColumnName, KijiCellDecoder[_]] =
      cellSpecMap.map { case (colName, cellSpec) => (colName, decoderFactory.create(cellSpec)) }

  /**
   * Get a the equivalent of a Kiji row from this table using generic decoders.
   *
   * @param row to get a generic version of.
   * @return a row that decodes cells with generic decoders.
   */
  private[express] def getRow(row: KijiRowData): ExpressGenericRow =
      new ExpressGenericRow(row, decoders)

  def close(): Unit = {
    ResourceUtils.closeOrLog(schemaTable)
    ResourceUtils.releaseOrLog(kiji)
    logger.debug("Closed ExpressGenericTable.")
  }
}
