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

/**
 * Module providing the main language constructs for reading from and writing to Kiji tables.
 *
 * KijiChopsticks users should import the members of this module to gain access to factory
 * methods that produce [[org.kiji.chopsticks.KijiSource]]s that can perform data processing
 * operations that read from or write to Kiji tables.
 * {{{
 *   import org.kiji.chopsticks.DSL._
 * }}}
 *
 * === Requesting columns and map-type column families. ===
 * When reading data from a Kiji table, users must specify the columns they wish to read and
 * optionally some parameters controlling how column cells are retrieved. Users should use the
 * factory method `Column` to create a request for cells from a particular column whose family
 * and qualifier are known. The number of most recent cells retrieved for a column can be
 * controlled. Here are some example requests for a column named `info:name`.
 * {{{
 *   // These three request the latest cell.
 *   var myColumnRequest = Column("info:name")
 *   myColumnRequest = Column("info:name", latest)
 *   myColumnRequest = Column("info:name", 1)
 *
 *   // This requests every cell.
 *   myColumnRequest = Column("info:name", all)
 *
 *   // This requests the 10 most recent cells.
 *   myColumnRequest = Column("info:name", 10)
 * }}}
 *
 * Cells can be requested for all columns in a map-type column family. Users should use the
 * factory method `MapFamily` to create a request for cells from all columns in a map-type column
 * family. In the same manner as with `Column`, the number of most recent cells retrieved for
 * each column in the family can be controlled. Optionally, users can also specify a regular
 * expression such that a column in the family will only be retrieved if its qualifier matches
 * the regular expression.
 * {{{
 *   // Gets the most recent cell for all columns in the map-type column family "searches".
 *   var myFamilyRequest = MapFamily("searches")
 *   // Gets all cells for all columns in the map-type column family "searches" whose
 *   // qualifiers contain the word "penguin".
 *   myFamilyRequest = MapFamily("searches", """.*penguin.*""", all)
 *   // Gets all cells for all columns in the map-type column family "searches".
 *   myFamilyRequest = MapFamily("searches", versions = all)
 * }}}
 *
 * === Getting input from a Kiji table. ===
 * The factory methods named `KijiInput` can be used to obtain a [[org.kiji.chopsticks.KijiSource]]
 * to process rows from the table (represented as tuples) using various operations. When using
 * `KijiInput`, users specify a table (using a Kiji URI) and use column requests and other options
 * to control how data is read from Kiji into tuple fields. Here are some examples.
 * {{{
 *   // Read the most recent cells from columns "info:id" and "info:name" into tuple fields "id"
 *   // and "name".
 *   var myKijiSource =
 *       KijiInput("kiji://.env/default/newsgroup_users")("info:id" -> 'id, "info:name" -> 'name)
 *   // Read only cells from "info:id" that occurred before Unix time 100000.
 *   myKijiSource =
 *       KijiInput("kiji://.env/default/newsgroup_users", Before(100000))("info:id" -> 'id)
 *   // Read all versions from "info:posts"
 *   myKijiSource =
 *       KijiInput("kiji://.env/default/newsgroup_users", Map(Column("info:id", all) -> 'id))
 * }}}
 *
 * See [[org.kiji.chopsticks.TimeRange]] for more information on how to create and use time
 * ranges for requesting data.
 *
 * === Writing to a Kiji table. ===
 * Data from any Cascading `Source` can be written to a Kiji table. Tuples to be written to a
 * Kiji table must have a field named "entityId" which contains an entity id for a row in a Kiji
 * table. The contents of a tuple field can be written as a cell at the most current timestamp to
 * a column in a Kiji table. To do so, you specify a mapping from tuple field names to qualified
 * Kiji table column names.
 * {{{
 *   // Write from the tuple field "average" to the column "stats:average" of the Kiji table
 *   // "newsgroup_users".
 *   mySource.write("kiji://.env/default/newsgroup_users")('average -> "stats:average")
 * }}}
 */
@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  /** Used with a column request to indicate that all cells of a column should be retrieved. */
  val all = HConstants.ALL_VERSIONS

  /**
   * Used with a column request to indicate that only the latest cell of a column should be
   * retrieved.
   */
  val latest = 1

  /**
   * Creates a request for the cells in columns of a map-type column family in a Kiji table.
   *
   * @param name of the map-type column family being requested.
   * @param qualifierMatches is a regular expression that the qualifiers of columns in the map-type
   *     column family must match for their cells to be retrieved. By default no filtering is
   *     performed.
   * @param versions is the maximum number of cells (starting with the most recent) that will be
   *     retrieved from columns in the map-type column family. By default only the most recent
   *     cell from columns in the map-type column family will be retrieved.
   * @return a request for the map-type column family configured with the specified options.
   */
  def MapFamily(
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

    new ColumnFamily(name, new ColumnRequestOptions(versions, filter))
  }

  /**
   * Creates a request for the cells in a column of a Kiji table.
   *
   * @param name of the column in the Kiji table, in the form `family:qualifier`.
   * @param versions is the maximum number of cells (starting from the most recent) that will be
   *     retrieved from the column. By default only the most recent cell from the column will be
   *     retrieved.
   * @return a request for the column configured with the specified options.
   */
  def Column(
      name: String,
      versions: Int = latest): QualifiedColumn = {
    name.split(":") match {
      case Array(family, qualifier) => {
        new QualifiedColumn(
            family,
            qualifier,
            new ColumnRequestOptions(versions, null))
      }
      case Array(family) => {
        throw new KijiInvalidNameException(
            "Specify the fully qualified column name in the format "
                + "\"family:qualifier\".\n"
                + "If you want to specify a map-type column family only, "
                + "use MapFamily instead of Column.")
      }
      case _ => {
        throw new KijiInvalidNameException(
          "Specify the fully qualified column name in the format \"family:qualifier\".")
      }
    }
  }

  /**
   * Creates a `Source` for reading row data from a Kiji table.
   *
   * A Scalding `Source` can be used to process a distributed data set and views the entries in
   * the data set as tuples. This factory method can be used to obtain a `Source` which views
   * rows in a Kiji table as tuples, with cells from columns or map-type column families provided
   * as entries in the tuple.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param columns are a series of tuples mapping column (or map-type column family) names to
   *     tuple field names. When naming a column, use the format "column:qualifier".
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def KijiInput
      (tableURI: String)
      (columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Creates a `Source` for reading row data from a Kiji table.
   *
   * A Scalding `Source` can be used to process a distributed data set and views the entries in
   * the data set as tuples. This factory method can be used to obtain a `Source` which views
   * rows in a Kiji table as tuples, with cells from columns or map-type column families provided
   * as entries in the tuple.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column (or map-type column family) names to
   *     tuple field names. When naming a column, use the format `column:qualifier`.
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def KijiInput(
      tableURI: String,
      timeRange: TimeRange)
      (columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, timeRange, columnMap)
  }

  /**
   * Creates a `Source` for reading row data from a Kiji table.
   *
   * A Scalding `Source` can be used to process a distributed data set and views the entries in
   * the data set as tuples. This factory method can be used to obtain a `Source` which views
   * rows in a Kiji table as tuples, with cells from columns or map-type column families provided
   * as entries in the tuple.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Use factory methods `MapFamily` and `Column` to create column
   *     requests.
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  def KijiInput(
      tableURI: String,
      columns: Map[_ <: ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Creates a `Source` for reading row data from a Kiji table.
   *
   * A Scalding `Source` can be used to process a distributed data set and views the entries in
   * the data set as tuples. This factory method can be used to obtain a `Source` which views
   * rows in a Kiji table as tuples, with cells from columns or map-type column families provided
   * as entries in the tuple.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column (or map-type column family) requests to
   *     tuple field names. Use factory methods `MapFamily` and `Column` to create column
   *     requests.
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
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
   * Creates a `Source` for writing cells to a Kiji table.
   *
   * A Scalding `Source` can be used to output data in a collection of tuples to some data store.
   * This factory method can be used to obtain a `Source` which will write the value in a field
   * of a tuple as a cell at the current time to a column in a Kiji table. Tuples being written
   * must have a field named "entityId" which contains an entity id for the row in the Kiji table
   * that tuple fields should be written to.
   *
   * @param tableURI is a Kiji URI that addresses a table in a Kiji instance.
   * @param columns are a series of pairs mapping tuple field names to Kiji column names. When
   *     naming columns, use the format `family:qualifier`.
   * @return a source that can write tuple fields to a cell in columns of a Kiji table.
   */
  def KijiOutput(
      tableURI: String)
      (columns: (Symbol, String)*): KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Gets the value of the first entry in a map sorted by key.

   * @param aMap to retrieve the value from.
   * @tparam T is the type of value stored in the map.
   */
  def getMostRecent[T](aMap: NavigableMap[_, T]): T = aMap.firstEntry().getValue()
}
