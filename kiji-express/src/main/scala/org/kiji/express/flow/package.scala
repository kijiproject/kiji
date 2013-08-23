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

package org.kiji.express

import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KConstants
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * Module providing the ability to write Scalding flows against data stored in Kiji tables.
 *
 * KijiExpress users should import the members of this module to gain access to factory
 * methods that produce [[org.kiji.express.flow.KijiSource]]s that can perform data processing
 * operations that read from or write to Kiji tables.
 * {{{
 *   import org.kiji.express.flow._
 * }}}
 *
 * === Requesting columns and map-type column families. ===
 * When reading data from a Kiji table, users must specify the columns they wish to read and
 * optionally some parameters controlling how column cells are retrieved. Users should use the
 * factory `Column` to create a request for cells from a particular column whose family
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
 * factory `MapFamily` to create a request for cells from all columns in a map-type column
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
 * When writing to a family, you must name which field in the tuple will specify the qualifier to
 * write to.
 * {{{
 *  MapFamily("searches")('terms)
 * }}}
 *
 * === Getting input from a Kiji table. ===
 * The factory `KijiInput` can be used to obtain a
 * [[org.kiji.express.flow.KijiSource]] to process rows from the table (represented as tuples)
 * using various operations. When using `KijiInput`, users specify a table (using a Kiji URI) and
 * use column requests and other options to control how data is read from Kiji into tuple fields.
 * Here are some examples.
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
 *       KijiInput("kiji://.env/default/newsgroup_users")(Map(Column("info:id", all) -> 'id))
 * }}}
 *
 * See below for more information on how to create and use time ranges for requesting data.
 *
 * === Writing to a Kiji table. ===
 * Data from any Cascading `Source` can be written to a Kiji table. Tuples to be written to a
 * Kiji table must have a field named `entityId` which contains an entity id for a row in a Kiji
 * table. The contents of a tuple field can be written as a cell at the most current timestamp to
 * a column in a Kiji table. To do so, you specify a mapping from tuple field names to qualified
 * Kiji table column names.
 * {{{
 *   // Write from the tuple field "average" to the column "stats:average" of the Kiji table
 *   // "newsgroup_users".
 *   mySource.write("kiji://.env/default/newsgroup_users")('average -> "stats:average")
 * }}}
 *
 * === Specifying ranges of time. ===
 * Instances of [[org.kiji.express.flow.TimeRange]] are used to specify a range of timestamps
 * that should be retrieved when reading data from Kiji. There are five implementations of
 * `TimeRange` that can be used when requesting data.
 *
 * <ul>
 *   <li>All</li>
 *   <li>At(timestamp: Long)</li>
 *   <li>After(begin: Long)</li>
 *   <li>Before(end: Long)</li>
 *   <li>Between(begin: Long, end: Long)</li>
 * </ul>
 *
 * These implementations can be used with [[org.kiji.express.flow.KijiInput]] to specify a range
 * that a Kiji cell's timestamp must be in to be retrieved. For example,
 * to read cells from the column `info:word` that have timestamps between `0L` and `10L`,
 * you can do the following.
 *
 * @example {{{
 *     KijiInput("kiji://.env/default/words", timeRange=Between(0L, 10L))("info:word" -> 'word)
 * }}}
 */
package object flow {

  /** Used with a column request to indicate that all cells of a column should be retrieved. */
  val all = HConstants.ALL_VERSIONS

  /**
   * Used with a column request to indicate that only the latest cell of a column should be
   * retrieved.
   */
  val latest = 1

  /**
   * A factory for requests for map-type column families.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  object MapFamily {
    /**
     * Creates a request for the cells in columns of a map-type column family in a Kiji table.
     *
     * @param name of the map-type column family being requested.
     * @param qualifierMatches is a regular expression that the qualifiers of columns in the
     *     map-type column family must match for their cells to be retrieved. By default no
     *     filtering is performed.
     * @param versions is the maximum number of cells (starting with the most recent) that will be
     *     retrieved from columns in the map-type column family. By default only the most recent
     *     cell from columns in the map-type column family will be retrieved.
     * @return a request for the map-type column family configured with the specified options.
     * @throws a KijiInvalidNameException if the map-type column name contains a qualifier.
     */
    def apply(
        name: String,
        qualifierMatches: String = "",
        versions: Int = latest): ColumnFamily = {
      val filter: Option[KijiColumnFilter] = {
        if ("" == qualifierMatches) {
          None
        } else {
          Some(new RegexQualifierColumnFilter(qualifierMatches))
        }
      }

      if (name.contains(":")) {
        throw new KijiInvalidNameException("Cannot create a map-type column request with a " +
            "qualifier in the column name \'" + name + "\'.")
      }
      new ColumnFamily(name, None, new ColumnRequestOptions(versions, filter))
    }

    /**
     * Creates an output specification for a map-type column family in a Kiji table.
     *
     * @param name of the map-type column family being requested.
     * @param qualifierSelector is the name of the field in the tuple that will contain the
     *     qualifier in this column family to write to.
     * @return a request for the map-type column family configured with the specified options.
     * @throws a KijiInvalidNameException if the map-type column name contains a qualifier.
     */
    def apply(name: String)(qualifierSelector: Symbol): ColumnFamily = {
      if (name.contains(":")) {
        throw new KijiInvalidNameException("Cannot create a map-type column output specification " +
            "with a qualifier in the column name \'" + name + "\'.")
      }
      new ColumnFamily(name, Some(qualifierSelector.name))
    }
  }

  /**
   * A factory for requests for group-type column families.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  object Column {
    /**
     * Creates a request for the cells in a column of a Kiji table.
     *
     * @param name of the column in the Kiji table, in the form `family:qualifier`.
     * @param versions is the maximum number of cells (starting from the most recent) that will be
     *     retrieved from the column. By default only the most recent cell from the column will be
     *     retrieved.
     * @return a request for the column configured with the specified options.
     * @throws a KijiInvalidNameException if the group-type column name is not fully qualified.
     */
    def apply(
        name: String,
        versions: Int = latest): QualifiedColumn = {
      name.split(":") match {
        case Array(family, qualifier) => {
          new QualifiedColumn(
              family,
              qualifier,
              new ColumnRequestOptions(versions))
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
  }

  /**
   * A trait implemented by classes that specify time ranges when reading data from Kiji tables.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  sealed trait TimeRange extends Serializable {
    /** Earliest timestamp of the TimeRange, inclusive. */
    def begin: Long

    /** Latest timestamp of the TimeRange, exclusive. */
    def end: Long
  }

  /**
   * Specifies that all timestamps should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case object All extends TimeRange {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Specifies that only the specified timestamp should be requested.
   *
   * @param timestamp to request.
   */
  final case class At(timestamp: Long) extends TimeRange {
    override val begin: Long = timestamp
    override val end: Long = timestamp
  }

  /**
   * Specifies that all timestamps after the specified begin timestamp should be requested.
   *
   * @param begin is the earliest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class After(override val begin: Long) extends TimeRange {
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Specifies that all timestamps before the specified end timestamp should be requested.
   *
   * @param end is the latest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class Before(override val end: Long) extends TimeRange {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
  }

  /**
   * Specifies that all timestamps between the specified begin and end timestamps should be
   * requested.
   *
   * @param begin is the earliest timestamp that should be requested.
   * @param end is the latest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class Between(
      override val begin: Long,
      override val end: Long) extends TimeRange {
    // Ensure that the timerange bounds are sensible.
    require(begin <= end, "Invalid time range specified: (%d, %d)".format(begin, end))
  }
}
