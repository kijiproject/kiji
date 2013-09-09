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
 * KijiOutput is a class that can be applied to a field -> column mapping in order to create a
 * `Source` for writing cells to a Kiji table.
 *
 * A Scalding `Source` can be used to output data in a collection of tuples to some data store.
 * This factory method can be used to obtain a `Source` which will write the value in a field
 * of a tuple as a cell at the current time to a column in a Kiji table. Tuples being written
 * must have a field named "entityId" which contains an entity id for the row in the Kiji table
 * that tuple fields should be written to.
 *
 * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
 * @param tsField is the name of a tuple field that contains a timestamp all cells should be
 *     written with. If unspecified, cells will be written with the current time.
 */
@ApiAudience.Public
@ApiStability.Experimental
class KijiOutput(tableURI: String, tsField: Symbol) {
  /**
   * Applies this output specification to a column mapping, returning a scalding `Source`.
   *
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `family:qualifier`.
   * @return a source that can write tuple fields to a cell in columns of a Kiji table.
   */
  def apply(columns: (Symbol, String)*): KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_).ignoreMissing)
    new KijiSource(tableURI, All, Option(tsField), 1000, columnMap)
  }

  /**
   * Applies this output specification to a column mapping, returning a scalding `Source`.
   *
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. Use
   * factory methods `MapFamily` and `Column` to name columns.
   * @return a source that can write tuple fields to a cell in columns of a Kiji table.
   */
  def apply(columns: Map[_ <: ColumnRequest, Symbol]): KijiSource = {
    val columnMap: Map[Symbol, ColumnRequest] = columns
        .map { entry: (ColumnRequest, Symbol) => entry.swap }
    new KijiSource(tableURI, All, Option(tsField), 1000, columnMap)
  }
}

/**
 * Companion object for KijiOutput that contains a factory method.
 */
@ApiAudience.Public
@ApiStability.Experimental
object KijiOutput {
  /**
   * A factory method for instantiating KijiOutputs.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   */
  def apply(
      tableURI: String,
      // scalastyle:off null
      tsField: Symbol = null): KijiOutput = {
    // scalastyle:on null
    new KijiOutput(tableURI, tsField)
  }
}

