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

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException

/**
 * A request for data from a Kiji table. Provides access to options common to all types of column
 * input specs. There are two types of column input specs:
 * <ul>
 *   <li>
 *     [[org.kiji.express.flow.QualifiedColumnInputSpec]] - Requests versions of cells from an
 *     fully-qualified column.
 *   </li>
 *   <li>
 *     [[org.kiji.express.flow.ColumnFamilyInputSpec]] - Requests versions of cells from columns in
 *     a column family.
 *   </li>
 * </ul>
 *
 * Requested data will be represented as a sequence of flow cells (`Seq[FlowCell[T] ]`).
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 *
 * Note: Subclasses of `ColumnInputSpec` are case classes that override its abstract methods
 * (e.g., `schemaSpec`) with `val`s.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait ColumnInputSpec {
  /**
   * Maximum number of cells to retrieve starting from the most recent cell. By default, only the
   * most recent cell is retrieved.
   *
   * @return the maximum number of cells to retrieve.
   */
  def maxVersions: Int

  /**
   * Filter that a cell must pass in order to be retrieved. If NoFilterSpec, no filter is used.
   *
   * @return the column filter specification
   */
  def filterSpec: ColumnFilterSpec

  /**
   * Specifies the maximum number of cells to maintain in memory when paging through a column.
   *
   * @return the paging specification for this column.
   */
  def pagingSpec: PagingSpec

  /**
   * Specifies the schema that should be applied to the requested data.
   *
   * @return the schema that should be used for reading.
   */
  def schemaSpec: SchemaSpec

  /**
   * Column family of the requested data.
   *
   * @return the column family of the requested data.
   */
  def family: String

  /**
   * The [[org.kiji.schema.KijiColumnName]] of the requested data.
   *
   * @return the column name of the requested data.
   */
  def columnName: KijiColumnName

}

/**
 * Provides convenience factory methods for creating [[org.kiji.express.flow.ColumnInputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object ColumnInputSpec {
  /** Constants for default parameters. */
  val DEFAULT_MAX_VERSIONS = latest
  val DEFAULT_PAGING_SPEC = PagingSpec.Off
  val DEFAULT_SCHEMA_SPEC = SchemaSpec.Writer
  val DEFAULT_COLUMN_FILTER = ColumnFilterSpec.NoColumnFilterSpec

  /**
   * A request for data from a Kiji table column. The input spec will be for a qualified column if
   * the column parameter contains a ':', otherwise the input will assumed to be for a column family
   * (column family names cannot contain ';' characters).
   *
   * @param column name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is `None`).
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji
   *        per page.
   * @param schemaSpec specifies the schema to use when reading cells. Defaults to
   *     [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return a new column input spec with supplied options.
   */
  def apply(
      column: String,
      maxVersions: Int = DEFAULT_MAX_VERSIONS,
      filterSpec: ColumnFilterSpec = DEFAULT_COLUMN_FILTER,
      pagingSpec: PagingSpec = DEFAULT_PAGING_SPEC,
      schemaSpec: SchemaSpec = DEFAULT_SCHEMA_SPEC
  ): ColumnInputSpec = {
    column.split(':') match {
      case Array(family, qualifier) =>
          QualifiedColumnInputSpec(
              family,
              qualifier,
              maxVersions,
              filterSpec,
              pagingSpec,
              schemaSpec
          )
      case Array(family) =>
          ColumnFamilyInputSpec(
              family,
              maxVersions,
              filterSpec,
              pagingSpec,
              schemaSpec
          )
      case _ => throw new IllegalArgumentException("column name must contain 'family:qualifier'" +
        " for a group-type, or 'family' for a map-type column.")
    }
  }

  /**
   * A request for data from a Kiji table column. The input spec will be for a qualified column if
   * the column parameter contains a ':', otherwise the input will assumed to be for a column family
   * (column family names cannot contain ';' characters). Data will be read back as the specified
   * avro class.
   *
   * @param column name of the requested data.
   * @param specificRecord class to read from the column.
   * @return a new column input spec with supplied options.
   */
  def apply(
      column: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnInputSpec = {
    ColumnInputSpec(column, schemaSpec = SchemaSpec.Specific(specificRecord))
  }

  /**
   * A request for data from a Kiji table column. The input spec will be for a qualified column if
   * the column parameter contains a ':', otherwise the input will assumed to be for a column family
   * (column family names cannot contain ';' characters). Data will be read back applying the
   * specified avro schema.
   *
   * @param column name of the requested data.
   * @param schema to apply to the data.
   * @return a new column input spec with supplied options.
   */
  def apply(
      column: String,
      schema: Schema
  ): ColumnInputSpec = {
    ColumnInputSpec(column, schemaSpec = SchemaSpec.Generic(schema))
  }
}

/**
 * Specifies a request for versions of cells from a fully-qualified column.
 *
 * Basic example that reads data into generic records using the schema with which they were written:
 * {{{
 *   // Request the latest version of data stored in the "info:name" column.
 *   val myColumnSpec: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec(
 *           family = "info",
 *           qualifier = "name",
 *           maxVersions = 1
 *       )
 * }}}
 *
 * Paging can be enabled on a column input specification causing blocks of cells to be retrieved
 * from Kiji at a time:
 * {{{
 *   // Request cells from the "info:status" column retrieving 1000 cells per block.
 *   val myPagedColumn: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec(
 *           family = "info",
 *           qualifier = "status",
 *           maxVersions = Int.MaxValue,
 *           pagingSpec = PagingSpec.Cells(1000)
 *       )
 * }}}
 *
 * If compiled avro classes are being used, a compiled record class can be specified. Data read from
 * this column will be of the specified type:
 * {{{
 *   // Request cells from the "info:user" column containing User records.
 *   val myColumnSpec: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec(
 *           family = "info",
 *           qualifier = "user",
 *           maxVersions = 1,
 *           schemaSpec = SchemaSpec.Specific(classOf[User])
 *       )
 * }}}
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 *
 * @param family of columns the requested data belongs to.
 * @param qualifier of the column the requested data belongs to.
 * @param maxVersions to read back from the requested column (default is only most recent).
 * @param filterSpec to use when reading back cells (default is `None`).
 * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji per page.
 * @param schemaSpec specifies the schema to use when reading cells. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class QualifiedColumnInputSpec(
    family: String,
    qualifier: String,
    maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
    filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER,
    pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
    schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnInputSpec {
  override val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.QualifiedColumnInputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object QualifiedColumnInputSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.QualifiedColumnInputSpec]] with
   * a specific Avro record type.
   *
   * @param family of columns the requested data belongs to.
   * @param qualifier of the column the requested data belongs to.
   * @param specificRecord class to read from the column.
   * @return a new column input spec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): QualifiedColumnInputSpec = {
    QualifiedColumnInputSpec(family, qualifier, schemaSpec = SchemaSpec.Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.QualifiedColumnInputSpec]] with
   * a generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family of columns the requested data belongs to.
   * @param qualifier of the column the requested data belongs to.
   * @param schema of generic Avro type to read from the column.
   * @return a new column input spec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
  ): QualifiedColumnInputSpec = {
    QualifiedColumnInputSpec(family, qualifier, schemaSpec = SchemaSpec.Generic(schema))
  }

  /**
   * A request for data from a fully qualified Kiji table column.
   * This construct method is used by Java builders for ColumnInputSpec.
   * Scala users ought to use the natural apply method.
   *
   * @param column is the fully qualified column name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is `None`).
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji
   *        per page.
   * @param schemaSpec specifies the schema to use when reading cells. Defaults to
   *     [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return a new column input spec with supplied options.
   */
  private[express] def construct(
      column: KijiColumnName,
      maxVersions: java.lang.Integer,
      filterSpec: ColumnFilterSpec,
      pagingSpec: PagingSpec,
      schemaSpec: SchemaSpec
  ): ColumnInputSpec = {
    // Construct QualifiedColumnInputSpec
    QualifiedColumnInputSpec(
        column.getFamily(),
        column.getQualifier(),
        Option(maxVersions) match {
          case None => ColumnInputSpec.DEFAULT_MAX_VERSIONS
          case _ => maxVersions
        },
        Option(filterSpec).getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER),
        Option(pagingSpec).getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
        Option(schemaSpec).getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )
  }
}

/**
 * Specifies a request for versions of cells from a column family.
 *
 * Basic column family example:
 * {{{
 *   // Request the latest version of data stored in the "matrix" column family.
 *   val myColumnFamilySpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec(
 *           family = "matrix",
 *           maxVersions = 1
 *       )
 * }}}
 *
 * Filters can be applied to the column qualifier of cells in a column family.
 * {{{
 *   // Request cells from the "hits" column that are from columns with qualifiers that begin with
 *   // the string "http://www.wibidata.com/".
 *   val myFilteredColumnSpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec(
 *           family = "hits",
 *           maxVersions = Int.MaxValue,
 *           filterSpec = RegexQualifierFilterSpec("http://www\.wibidata\.com/.*")
 *       )
 * }}}
 *
 * Paging can be enabled on a column input specification causing blocks of cells to be retrieved
 * from Kiji at a time:
 * {{{
 *   // Request cells from the "metadata" column family retrieving 1000 cells per block.
 *   val myPagedColumn: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec(
 *           family = "metadata",
 *           maxVersions = Int.MaxValue,
 *           pagingSpec = PagingSpec.Cells(1000)
 *       )
 * }}}
 *
 * If compiled avro classes are being used, a class that data should be read as can be specified:
 * {{{
 *   // Request cells from the "users" column family containing User records.
 *   val myColumnSpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec(
 *           family = "users",
 *           maxVersions = 1,
 *           schemaSpec = SchemaSpec.Specific(classOf[User])
 *       )
 * }}}
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 *
 * @param family of columns the requested data belongs to.
 * @param maxVersions to read back from the requested column family (default is only most recent).
 * @param filterSpec to use when reading back cells (default is `None`).
 * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji per page.
 * @param schemaSpec specifies the schema to use when reading cells. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ColumnFamilyInputSpec(
    family: String,
    maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
    filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER,
    pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
    schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnInputSpec {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a ':' in family name for column family request")
  }
  override val columnName: KijiColumnName = new KijiColumnName(family)
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.ColumnFamilyInputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object ColumnFamilyInputSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnFamilyInputSpec]] with a
   * specific Avro record type.
   *
   * @param family of columns the requested data belongs to.
   * @param specificRecord class to read from the column.
   * @return a new column input spec with supplied options.
   */
  def apply(
      family: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnFamilyInputSpec = {
    ColumnFamilyInputSpec(family, schemaSpec = SchemaSpec.Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnFamilyInputSpec]] with a
   * generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family of columns the requested data belongs to.
   * @param schema of Avro type to read from the column.
   * @return a new column input spec with supplied options.
   */
  def apply(
      family: String,
      schema: Schema
  ): ColumnFamilyInputSpec = {
    ColumnFamilyInputSpec(family, schemaSpec = SchemaSpec.Generic(schema))
  }

  /**
   * A request for data from a Kiji table column family.
   * This construct method is used by Java builders for ColumnInputSpec.
   * Scala users ought to use the natural apply method.
   *
   * @param column family name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is `None`).
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji
   *        per page.
   * @param schemaSpec specifies the schema to use when reading cells. Defaults to
   *     [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return a new column input spec with supplied options.
   */
  private[express] def construct(
      column: KijiColumnName,
      maxVersions: java.lang.Integer,
      filterSpec: ColumnFilterSpec,
      pagingSpec: PagingSpec,
      schemaSpec: SchemaSpec
  ): ColumnInputSpec = {
    // Construct QualifiedColumnInputSpec
    ColumnFamilyInputSpec(
        column.getFamily(),
        Option(maxVersions) match {
          case None => ColumnInputSpec.DEFAULT_MAX_VERSIONS
          case _ => maxVersions
        },
        Option(filterSpec).getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER),
        Option(pagingSpec).getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
        Option(schemaSpec).getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )
  }
}
