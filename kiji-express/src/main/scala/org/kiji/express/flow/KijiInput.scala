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
 *   // Create a KijiSource that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiInput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       "info:column1" -> 'column1,
 *       "info:column2" -> 'column2)
 * }}}
 *
 * Verbose:
 * {{{
 *   // Create a KijiSource that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiInput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       columns = Map(
 *           QualifiedColumnInputSpec("info", "column1") -> 'column1,
 *           QualifiedColumnInputSpec("info", "column2") -> 'column2)
 * }}}
 *
 * The verbose methods allow you to instantiate explicity
 * [[org.kiji.express.flow.QualifiedColumnInputSpec]] and
 * [[org.kiji.express.flow.ColumnInputSpec]] objects.
 * Use the verbose method to specify options for the input columns, e.g.,
 * {{{
 *   // Create a KijiSource that reads from the table named `mytable` reading the columns
 *   // `info:column1` and `info:column2` to the fields `'column1` and `'column2`.
 *   KijiInput(
 *       tableUri = "kiji://localhost:2181/default/mytable",
 *       columns = Map(
 *           QualifiedColumnInputSpec("info", "column1", maxVersions=5) -> 'column1,
 *           QualifiedColumnInputSpec("info", "column2", paging=Cells(10)) -> 'column2)
 * }}}
 */
@ApiAudience.Public
@ApiStability.Stable
object KijiInput {
  /** Default time range for KijiSource */
  private val defaultTimeRange: TimeRange = All

  /**
   * An internal factory method for creating a [[org.kiji.express.flow.KijiSource]] for reading
   * cells from a Kiji table.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  private def applyAllArgsSym(
      tableUri: String,
      timeRange: TimeRange,
      columns: (String, Symbol)*
  ): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, ColumnInputSpec(col)) }
        .toMap

    new KijiSource(
        tableUri,
        timeRange,
        None,
        inputColumns = columnMap
    )
  }

  /**
   * An internal factory method for creating a [[org.kiji.express.flow.KijiSource]] for
   * reading cells from a Kiji table.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with
   *     cell data from the requested columns and map-type column families.
   */
  private def applyAllArgsMap(
      tableUri: String,
      timeRange: TimeRange,
      columns: Map[_ <: ColumnInputSpec, Symbol]
  ): KijiSource = {
    val columnMap = columns
        .map { entry: (ColumnInputSpec, Symbol) => entry.swap }

    new KijiSource(
        tableUri,
        timeRange,
        None,
        inputColumns = columnMap
    )
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      columns: (String, Symbol)*
  ): KijiSource = {
    applyAllArgsSym(tableUri, defaultTimeRange, columns: _*)
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      columns: (String, Symbol)*
  ): KijiSource = {
    applyAllArgsSym(tableUri, timeRange, columns: _*)
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      columns: Map[_ <: ColumnInputSpec, Symbol]
  ): KijiSource = {
    applyAllArgsMap(tableUri, defaultTimeRange, columns)
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(
      tableUri: String,
      timeRange: TimeRange,
      columns: Map[_ <: ColumnInputSpec, Symbol]
  ): KijiSource = {
    applyAllArgsMap(tableUri, timeRange, columns)
  }
}
