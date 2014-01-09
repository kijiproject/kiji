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
 * Module providing the ability to write Scalding using data stored in Kiji tables.
 *
 * KijiExpress users should import the members of this module to gain access to factory
 * methods that produce [[org.kiji.express.flow.KijiSource]]s that can perform data processing
 * operations that read from or write to Kiji tables.
 * {{{
 *   import org.kiji.express.flow._
 * }}}
 *
 * === Reading from columns and map-type column families. ===
 * Specify columns to read from a Kiji table using instances of the
 * [[org.kiji.express.flow.QualifiedColumnInputSpec]] and
 * [[org.kiji.express.flow.ColumnFamilyInputSpec]] classes, which contain fields for specifying
 * the names of the columns to read, as well as what data to read back (e.g., only the latest
 * version of a cell, or a certain number of recent versions) and how it is read back (e.g., using
 * paging to limit the amount of data in memory).
 *
 * Specify a fully-qualified column with an instance of `QualifiedColumnInputSpec`.  Below are
 * several examples for specifying the column `info:name`:
 * {{{
 *   // Request the latest cell.
 *   val myInputColumn = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "name")
 *       .build
 *   val myInputColumn = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "name")
 *       .withMaxVersions(latest)
 *       .build
 *   val myInputColumn = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "name")
 *       .withMaxVersions(1)
 *       .build
 *
 *   // Request every cell.
 *   val myInputColumn = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "name")
 *       .withMaxVersions(all)
 *       .build
 *
 *   // Request the 10 most recent cells.
 *   val myInputColumn = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "name")
 *       .withMaxVersions(10)
 *       .build
 * }}}
 *
 * To request cells from all of the columns in a family, use the `ColumnFamilyInputSpec`
 * class, which, like `QualifiedColumnInputSpec`, provides options on the input spec such as
 * the maximum number of cell versions to return, filters to use, etc.  A user can
 * specify a filter, for example, to specify a regular expression such that a column in the family
 * will only be retrieved if its qualifier matches the regular expression:
 * {{{
 *   // Gets the most recent cell for all columns in the column family "searches".
 *   var myFamilyInput = ColumnFamilyInputSpec.builder.withFamily("searches").build
 *
 *   // Gets all cells for all columns in the column family "searches" whose
 *   // qualifiers contain the word "penguin".
 *   myFamilyInput = ColumnFamilyInputSpec.builder
 *       .withFamily("searches")
 *       .withFilterSpec(ColumnFilterSpec.Regex(""".*penguin.*"""))
 *       .withMaxVersions(all)
 *       .build
 *
 *   // Gets all cells for all columns in the column family "searches".
 *   myFamilyInput = ColumnFamilyInputSpec.builder
 *       .withFamily("searches")
 *       .withMaxVersions(all)
 *       .build
 * }}}
 *
 * See [[org.kiji.express.flow.QualifiedColumnInputSpec]] and
 * [[org.kiji.express.flow.ColumnFamilyInputSpec]] for a full list of options for column input
 * specs.
 *
 * When specifying a column for writing, the user can likewise use the
 * `QualifiedColumnOutputSpec` and `ColumnFamilyOutputSpec` classes to indicate the name of
 * the column and any options.  The following, for example, specifies a column to use for writes
 * with the default reader schema:
 * {{{
 *   // Create a column output spec for writing to "info:name" using the default reader schema
 *   var myWriteReq = QualifiedColumnOutputSpec.builder
 *       .withColumn("info", "name")
 *       .withSchemaSpec(SchemaSpec.DefaultReader)
 *       .build
 * }}}
 *
 *
 * When writing to a family, you specify a Scalding field that contains the name of the qualifier to
 * use for your write.  For example, to use the value in the Scalding field ``'terms`` as the name
 * of the column qualifier, use the following:
 * {{{
 *   var myOutputFamily = ColumnFamilyOutputSpec.builder
 *       .withFamily("searches")
 *       .withQualifierSelector('terms)
 *       .build
 * }}}
 *
 * See [[org.kiji.express.flow.QualifiedColumnOutputSpec]] and
 * [[org.kiji.express.flow.ColumnFamilyOutputSpec]] for a full list of options for column output
 * specs.
 *
 * === Getting input from a Kiji table. ===
 * The factory `KijiInput` can be used to obtain a
 * [[org.kiji.express.flow.KijiSource]] to process rows from the table (represented as tuples)
 * using various operations. When using `KijiInput`, users specify a table (using a Kiji URI) and
 * use column specs and other options to control how data is read from Kiji into tuple fields.
 * ``KijiInput`` contains different factories that allow for abbreviated column specifications,
 * as illustrated in the examples below:
 * {{{
 *   // Read the most recent cells from columns "info:id" and "info:name" into tuple fields "id"
 *   // and "name" (don't explicitly instantiate a QualifiedColumnInputSpec).
 *   var myKijiSource = KijiInput.builder
 *       .withTableURI("kiji://.env/default/newsgroup_users")
 *       .withColumns("info:id" -> 'id, "info:name" -> 'name)
 *       .build
 *
 *   // Read only cells from "info:id" that occurred before Unix time 100000.
 *   // (Don't explicitly instantiate a QualifiedColumnInputSpec)
 *   myKijiSource = KijiInput.builder
 *       .withTableURI("kiji://.env/default/newsgroup_users")
 *       .withTimeRangeSpec(Before(100000))
 *       .withColumns("info:id" -> 'id)
 *       .build
 *
 *   // Read all versions from "info:posts"
 *   myKijiSource = KijiInput.builder
 *       .withTableURI("kiji://.env/default/newsgroup_users")
 *       .withColumnSpecs(
 *           QualifiedColumnOutputSpec.builder
 *               .withColumn("info", "id")
 *               .withMaxVersions(all)
 *               .build -> 'id
 *       )
 *       .build
 * }}}
 *
 * See [[org.kiji.express.flow.KijiInput]] and [[org.kiji.express.flow.ColumnInputSpec]] for more
 * information on how to create and use time ranges for requesting data.
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
 *   mySource.write(
 *       KijiOutput.builder
 *           .withTableURI("kiji://.env/default/newsgroup_users")
 *           .withColumns('average -> "stats:average")
 *           .build
 *   )
 *
 *   // Create a KijiSource to write the data in tuple field "results" to column family
 *   // "searches" with the string in tuple field "terms" as the column qualifier.
 *   myOutput = KijiOutput.builder
 *       .withTableURI("kiji://.env/default/searchstuff")
 *       .withColumnSpecs('results -> ColumnFamilyOutputSpec.builder
 *           .withFamily("searches")
 *           .withQualifierSelector('terms)
 *           .build
 *       )
 *       .build
 * }}}
 *
 * === Specifying ranges of time. ===
 * Instances of [[org.kiji.express.flow.TimeRangeSpec]] are used to specify a range of timestamps
 * that should be retrieved when reading data from Kiji. There are five implementations of
 * `TimeRange` that can be used when requesting data.
 *
 * <ul>
 *   <li>All</li>
 *   <li>At(timestamp: Long)</li>
 *   <li>From(begin: Long)</li>
 *   <li>Before(end: Long)</li>
 *   <li>Between(begin: Long, end: Long)</li>
 * </ul>
 *
 * These implementations can be used with [[org.kiji.express.flow.KijiInput]] to specify a range
 * that a Kiji cell's timestamp must be in to be retrieved. For example,
 * to read cells from the column `info:word` that have timestamps between `0L` and `10L`,
 * you can do the following.
 *
 * @example
 *     {{{
 *       KijiInput.builder
 *           .withTableURI("kiji://.env/default/words")
 *           .withTimeRangeSpec(Between(0L, 10L))
 *           .withColumns("info:word" -> 'word)
 *           .build
 *     }}}
 */
package object flow {

  /** Used with a column input spec to indicate that all cells of a column should be retrieved. */
  val all = HConstants.ALL_VERSIONS

  /**
   * Used with a column input spec to indicate that only the latest cell of a column should be
   * retrieved.
   */
  val latest = 1
}
