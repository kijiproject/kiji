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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  /**
   * Used with [[#MapColumn(String, String, Int)]] and [[#Column(String, Int)]] to specify that all
   * versions of each column should be read.
   */
  val all = Integer.MAX_VALUE

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
      versions: Int = latest): ColumnRequest = {
    require(name.split(":").length == 1)

    val filter: KijiColumnFilter = {
      if ("" == qualifierMatches) {
        null
      } else {
        new RegexQualifierColumnFilter(qualifierMatches)
      }
    }

    new ColumnRequest(name, new ColumnRequest.InputOptions(versions, filter))
  }

  /**
   * Factory method for Column that is a group-type column.
   *
   * @param name of column in "family:qualifier" form.
   * @param versions of column to get.
   */
  def Column(
      name: String,
      versions: Int = latest): ColumnRequest = {
    require(name.split(":").length == 2)

    new ColumnRequest(name, new ColumnRequest.InputOptions(versions, null))
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
      columns: Map[ColumnRequest, Symbol]): KijiSource = {
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
}
