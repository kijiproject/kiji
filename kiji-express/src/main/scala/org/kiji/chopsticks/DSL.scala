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

package org.kiji.chopsticks

import java.util.NavigableMap

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  /**
   * Used with [[#MapColumn(String, String, Int)]] and [[#Column(String, Int)]] to specify that all
   * versions of each column should be read.
   */
  val all = HConstants.ALL_VERSIONS

  /**
   * Used with [[#MapColumn(String, String, Int)]] and [[#Column(String, Int)]] to specify that
   * only the latest version of each column should be read.
   */
  val latest = 1

  /**
   * Factory method for Column that is a map-type column family.
   *
   * @param name of column in "family:qualifier" or "family" form.
   * @param qualifierMatches Regex for filtering qualifiers. Specify the empty string ("") to accept
   *     all qualifiers. Default value is "".
   * @param versions of column to get. Default value is 1.
   */
  def MapColumn(
      name: String,
      qualifierMatches: String = "",
      versions: Int = latest): ColumnFamily = {
    val filter: KijiColumnFilter = {
      if ("" == qualifierMatches) {
        null
      } else {
        new RegexQualifierColumnFilter(qualifierMatches)
      }
    }

    new ColumnFamily(name, new ColumnRequest.InputOptions(versions, filter))
  }

  /**
   * Factory method for Column that is a group-type column.
   *
   * @param name of column in "family:qualifier" form.
   * @param versions of column to get.
   */
  def Column(
      name: String,
      versions: Int = latest): QualifiedColumn = {
    name.split(":") match {
      case Array(family, qualifier) => {
        new QualifiedColumn(
            family,
            qualifier,
            new ColumnRequest.InputOptions(versions, null))
      }
      case Array(family) => {
        throw new KijiInvalidNameException(
            "Specify the fully qualified column name in the format "
                + "\"family:qualifier\".\n"
                + "If you want to specify a map-type column family only, "
                + "use MapColumn instead of Column.")
      }
      case _ => {
        throw new KijiInvalidNameException(
          "Specify the fully qualified column name in the format \"family:qualifier\".")
      }
    }
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param timeRange Range of timestamps to read from each column.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String,
      timeRange: TimeRange) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, timeRange, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param timeRange Range of timestamps to read from each column.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String,
      timeRange: TimeRange,
      columns: Map[ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, timeRange, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String) (
        columns: (Symbol, String)*): KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String,
      columns: Map[Symbol, ColumnRequest]): KijiSource = {
    new KijiSource(tableURI, TimeRange.All, columns)
  }

  /**
   * Get the first entry of a [[java.util.NavigableMap]]
   *
   * @param slice The map to get the first entry from.
   * @param T The type of value stored in the map.
   */
  // TODO: This can be removed once KijiSlice is in.
  def getMostRecent[T](slice: NavigableMap[Long, T]): T = slice.firstEntry().getValue()
}
