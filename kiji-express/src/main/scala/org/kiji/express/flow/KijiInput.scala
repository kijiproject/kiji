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
 * KijiInput is a class that can be applied to a column -> field mapping in order to create a
 * `Source` for reading row data from a Kiji table.
 *
 * A Scalding `Source` can be used to process a distributed data set and views the entries in
 * the data set as tuples. This factory method can be used to obtain a `Source` which views
 * rows in a Kiji table as tuples, with cells from columns or map-type column families provided
 * as entries in the tuple.
 *
 * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
 * @param timeRange that cells must fall into to be retrieved.
 * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
 *     every 5000th skipped row will be logged.
 */
@ApiAudience.Public
@ApiStability.Experimental
class KijiInput(
    tableURI: String,
    timeRange: TimeRange,
    loggingInterval: Long) {
  /**
   * Applies this input specification to a column mapping, returning a scalding `Source`.
   *
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Columns are specified as "family:qualifier" or, in the case of a
   *     map-type column family, simply "family".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
                    .map { case (col, field) =>
      if (col.contains(":")) { // Group-type column
        (field, Column(col).ignoreMissing)
      } else { // Map-type column
        (field, MapFamily(col, qualifierMatches="", versions=latest).ignoreMissing)
      }
    }
                    .toMap
    new KijiSource(tableURI, timeRange, None, loggingInterval, columnMap)
  }

  /**
   * Applies this input specification to a column mapping, returning a scalding `Source`.
   *
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Use factory methods `MapFamily` and `Column` to create column
   *     requests.
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def apply(columns: Map[_ <: ColumnRequest, Symbol]): KijiSource = {
    val columnMap: Map[Symbol, ColumnRequest] = columns
        .map { entry: (ColumnRequest, Symbol) => entry.swap }
    new KijiSource(tableURI, timeRange, None, loggingInterval, columnMap)
  }
}

/**
 * Companion object for KijiInput that contains a factory method.
 */
object KijiInput {
  /**
   * A factory method for instantiating KijiInputs.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param loggingInterval to log skipped rows at. For example, if loggingInterval is 5000,
   *     every 5000th skipped row will be logged.
   * @return a KijiInput that can be applied to columns.
   */
  def apply(
      tableURI: String,
      timeRange: TimeRange = All,
      loggingInterval: Long = 1000): KijiInput = {
    new KijiInput(tableURI, timeRange, loggingInterval)
  }
}
