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

package org.kiji.express.flow

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * Factory methods for constructing [[org.kiji.express.flow.KijiSource]]s that will be used as
 * outputs of a KijiExpress flow. Two basic APIs are provided with differing complexity.
 *
 * Simple:
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       'column1 -> "info:column1",
 *       'column2 -> "info:column2")
 * }}}
 *
 * Verbose:
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable` putting timestamps in the
 *   // `'timestamps` field and writing the fields `'column1` and `'column2` to the columns
 *   // `info:column1` and `info:column2`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       columns = Map(
 *           // Enable paging for `info:column1`.
 *           'column1 -> QualifiedColumnOutputSpec("info", "column1"),
 *           'column2 -> QualifiedColumnOutputSpec("info", "column2")))
 * }}}
 *
 * The verbose methods allow you to instantiate explicity
 * [[org.kiji.express.flow.QualifiedColumnOutputSpec]] and
 * [[org.kiji.express.flow.ColumnFamilyOutputSpec]] objects.
 * Use the verbose method to specify options for the output columns, e.g.,
 * {{{
 *   // Create a KijiOutput that writes to the table named `mytable`, writing the value in field
 *   // `'column1` to the column `info:column1`, and writing the value in field `'column2` to a
 *   // column in family `family` using a qualifier that is the value of field `'qualifier`.
 *   KijiOutput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       timestampField = 'timestamps,
 *       columns = Map(
 *           'column1 -> QualifiedColumnOutputSpec("info", "column1",
 *               schemaSpec = SchemaSpec.Generic(myAvroSchema)),
 *           'column2 -> ColumnFamilyOutputSpec("family", 'qualifier)))
 * }}}
 */
@ApiAudience.Public
@ApiStability.Experimental
object KijiOutput {
  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. This
   * method permits specifying the full range of read options for each column. Values written will
   * be tagged with the current time at write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param columns is a mapping specifying what column to write each field value to.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      columns: Map[Symbol, _ <: ColumnOutputSpec]
  ): KijiSource = {
    new KijiSource(
        tableAddress = tableUri,
        timeRange = All,
        timestampField = None,
        outputColumns = columns)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. This
   * method permits specifying the full range of read options for each column.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param columns is a mapping specifying what column to write each field value to.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      timestampField: Symbol,
      columns: Map[Symbol, _ <: ColumnOutputSpec]
  ): KijiSource = {
    require(timestampField != null)

    new KijiSource(
        tableAddress = tableUri,
        timeRange = All,
        timestampField = Some(timestampField),
        outputColumns = columns)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. Values
   * written will be tagged with the current time at write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `"family:qualifier"`.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      columns: (Symbol, String)*
  ): KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(QualifiedColumnOutputSpec(_))

    KijiOutput(tableUri, columnMap)
  }

  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `"family:qualifier"`.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  def apply(
      tableUri: String,
      timestampField: Symbol,
      columns: (Symbol, String)*
  ): KijiSource = {
    require(timestampField != null)

    val columnMap = columns
        .toMap
        .mapValues(QualifiedColumnOutputSpec(_))

    KijiOutput(tableUri, timestampField, columnMap)
  }
}
