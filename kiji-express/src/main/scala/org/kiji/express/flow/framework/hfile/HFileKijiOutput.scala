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

package org.kiji.express.flow.framework.hfile

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.All
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.ColumnRequestOutput

/**
 * Factory methods for constructing [[org.kiji.express.flow.framework.hfile.HFileKijiSource]]s that
 * will be used as outputs of a KijiExpress flow. Two basic APIs are provided with differing
 * complexity. These are similar to the [[org.kiji.express.flow.framework.HFileKijiSource]] APIs
 * except that an extra parameter for the HFile output location is required.
 *
 * Simple:
 * {{{
 *   // Create an HFileKijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`. The resulting HFiles will be written to the "my_hfiles"
 *   // folder.
 *   HFileKijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       hFileOutput = "my_hfiles",
 *       timestampField = 'timestamps,
 *       'column1 -> "info:column1",
 *       'column2 -> "info:column2")
 * }}}
 *
 * Verbose:
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`. The resulting HFiles will be written to the "my_hfiles"
 *   // folder.
 *   HFileKijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       hFileOutput = "my_hfiles",
 *       timestampField = 'timestamps,
 *       columns = Map(
 *           // Enable paging for `info:column1`.
 *           'column1 -> QualifiedColumn("info", "column1").withPaging(cellsPerPage = 100),
 *           'column2 -> QualifiedColumn("info", "column2")))
 * }}}
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object HFileKijiOutput {

  val TEMP_HFILE_OUTPUT_KEY = "kiji.tempHFileOutput"

  /**
   * A factory method for instantiating [[org.kiji.express.flow.framework.hfile.HFileKijiSource]]s
   * used as sinks. This method permits specifying the full range of read options for each column.
   * Values written will be tagged with the current time at write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param hFileOutput is the location where the resulting HFiles will be placed.
   * @param columns is a mapping specifying what column to write each field value to.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      hFileOutput: String,
      columns: Map[Symbol, _ <: ColumnRequestOutput]
  ): HFileKijiSource = {
    new HFileKijiSource(
        tableAddress = tableUri,
        hFileOutput,
        timeRange = All,
        timestampField = None,
        loggingInterval = 1000,
        columns = columns)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.framework.hfile.HFileKijiSource]]s
   * used as sinks. This method permits specifying the full range of read options for each column.
   * Values written will be tagged with the current time at write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param hFileOutput is the location where the resulting HFiles will be placed.
   * @param columns is a mapping specifying what column to write each field value to.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      hFileOutput: String,
      timestampField: Symbol,
      columns: (Symbol, String)*
  ): HFileKijiSource = {

    val columnMap = columns
        .toMap
        .mapValues(ColumnRequestOutput(_))
    new HFileKijiSource(
        tableAddress = tableUri,
        hFileOutput = hFileOutput,
        timeRange = All,
        Some(timestampField),
        loggingInterval = 1000,
        columns = columnMap)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.framework.hfile.HFileKijiSource]]s
   * used as sinks. This method permits specifying the full range of read options for each column.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param hFileOutput is the location where the resulting HFiles will be placed.
   * @param columns is a mapping specifying what column to write each field value to.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      hFileOutput: String,
      timestampField: Symbol,
      columns: Map[Symbol, _ <: ColumnRequestOutput]
  ): HFileKijiSource = {
    require(timestampField != null)

    new HFileKijiSource(
        tableAddress = tableUri,
        hFileOutput,
        timeRange = All,
        timestampField = Some(timestampField),
        loggingInterval = 1000,
        columns = columns)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.framework.hfile.HFileKijiSource]]s
   * used as sinks. Values written will be tagged with the current time at write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param hFileOutput is the location where the resulting HFiles will be placed.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `"family:qualifier"`.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      hFileOutput: String,
      columns: (Symbol, String)*
  ): HFileKijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(ColumnRequestOutput(_))

    HFileKijiOutput(tableUri, hFileOutput, columnMap)
  }
}
