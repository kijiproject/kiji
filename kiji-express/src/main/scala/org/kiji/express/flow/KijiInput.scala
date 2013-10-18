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
 * inputs to a KijiExpress flow. Two basic APIs are provided with differing complexity.
 *
 * Simple:
 * {{{
 *   // Create a KijiInput that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiInput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       "info:column1" -> 'column1,
 *       "info:column2" -> 'column2)
 * }}}
 *
 * Verbose:
 * {{{
 *   // Create a KijiInput that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiInput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       columns = Map(
 *           QualifiedColumn("info", "column1") -> 'column1,
 *           QualifiedColumn("info", "column2") -> 'column2)
 * }}}
 */
@ApiAudience.Public
@ApiStability.Experimental
object KijiInput {
  /** Default time range for KijiSource */
  private val DEFAULT_TIME_RANGE: TimeRange = All

  /** Default logging interval for KijiSource */
  private val DEFAULT_LOGGING_INTERVAL: Long = 1000

  /**
   * An internal factory method for creating a [[org.kiji.express.flow.KijiSource]] for reading
   * cells from a Kiji table.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  private def applyAllArgsSym(
      tableUri: String,
      timeRange: TimeRange,
      loggingInterval: Long,
      columns: (String, Symbol)*): KijiSource = {

    val columnMap = columns
        .map {
          case (col, field) =>
          if (col.contains(":")) { // Group-type column
            (field, Column(col).ignoreMissing)
          } else { // Map-type column
            (field, MapFamily(col, qualifierMatches="", versions=latest).ignoreMissing)
          }
        }
        .toMap
    new KijiSource(tableUri, timeRange, None, loggingInterval, columnMap)
  }

  /**
   * An internal factory method for creating a [[org.kiji.express.flow.KijiSource]] for reading
   * cells from a Kiji table.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  private def applyAllArgsMap(
      tableUri: String,
      timeRange: TimeRange,
      loggingInterval: Long,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource = {
    val columnMap: Map[Symbol, ColumnRequest] = columns
        .map { entry: (ColumnRequest, Symbol) => entry.swap }
    new KijiSource(tableUri, timeRange, None, loggingInterval, columnMap)
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      columns: (String, Symbol)*): KijiSource =
    applyAllArgsSym(tableUri, DEFAULT_TIME_RANGE, DEFAULT_LOGGING_INTERVAL, columns: _*)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      columns: (String, Symbol)*): KijiSource =
    applyAllArgsSym(tableUri, timeRange, DEFAULT_LOGGING_INTERVAL, columns: _*)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      loggingInterval: Long,
      columns: (String, Symbol)*): KijiSource =
    applyAllArgsSym(tableUri, DEFAULT_TIME_RANGE, loggingInterval, columns: _*)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      loggingInterval: Long,
      columns: (String, Symbol)*): KijiSource =
    applyAllArgsSym(tableUri, timeRange, loggingInterval, columns: _*)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource =
    applyAllArgsMap(tableUri, DEFAULT_TIME_RANGE, DEFAULT_LOGGING_INTERVAL, columns)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource =
    applyAllArgsMap(tableUri, timeRange, DEFAULT_LOGGING_INTERVAL, columns)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      loggingInterval: Long,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource =
    applyAllArgsMap(tableUri, DEFAULT_TIME_RANGE, loggingInterval, columns)

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      loggingInterval: Long,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource =
    applyAllArgsMap(tableUri, timeRange, loggingInterval, columns)
}
