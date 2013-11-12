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
 * Interface for all column input specification objects. ColumnInputSpec implementations
 * specify how to read Kiji columns or column families into individual fields in an Express flow.
 *
 * Use the [[org.kiji.express.flow.QualifiedColumnInputSpec]] to retrieve an individual Kiji
 * column into a single field in a flow.
 *
 * Use the [[org.kiji.express.flow.ColumnFamilyInputSpec]] to retrieve an entire column family
 * into a field of a flow.  Each row in the KijiTable will be a new tuple, with each field in the
 * tuple containing a stream of [[org.kiji.express.flow.Cell]]s.
 *
 * Note: Subclasses of ColumnInputSpec are case classes that override ColumnInputSpec's
 * abstract methods (e.g., `schema`) with `val`s.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
sealed trait ColumnInputSpec {
  /**
   * Specifies the maximum number of cells (from the most recent) to retrieve from a column.
   *
   * By default, only the most recent cell is retrieved.
   *
   * @return the maximum number of cells (from the most recent) to retrieve from a column.
   */
  def maxVersions: Int

  /**
   * Specifies a filter that a cell must pass in order to be retrieved.
   *
   * If `None`, no filter is used.
   *
   * @return `Some(filter)` or `None`.
   */
  def filter: Option[ColumnFilterSpec]

  /**
   * Specifies the maximum number of cells to maintain in memory when paging through a column.
   *
   * @return the paging specification for this column.
   */
  def paging: PagingSpec

  /**
   * Specifies the schema of data to be read from the column.
   *
   * @return schema specification for reading column.
   */
  def schemaSpec: SchemaSpec

  /**
   * Column family which this [[org.kiji.express.flow.ColumnInputSpec]] belongs to.
   *
   * @return family name of column
   */
  def family: String

  /**
   * The [[org.kiji.schema.KijiColumnName]] of the column.
   *
   * @return the name of this column.
   */
  def columnName: KijiColumnName
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.ColumnInputSpec]] instances.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
object ColumnInputSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnInputSpec]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family.
   *
   * @param column The requested column name.
   * @param maxVersions The maximum number of versions to read back (default is only most recent).
   * @param filter Filter to use when reading back cells (default is `None`).
   * @param paging Maximum number of cells to retrieve from HBase per page.
   * @param schemaSpec Reader schema specification.  Defaults to
   *     [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return ColumnInputSpec with supplied options.
   */
  def apply(
      column: String,
      maxVersions: Int = latest,
      filter: Option[ColumnFilterSpec] = None,
      paging: PagingSpec = PagingSpec.Off,
      schemaSpec: SchemaSpec = SchemaSpec.Writer
  ): ColumnInputSpec = {
    column.split(':') match {
      case Array(family, qualifier) =>
          QualifiedColumnInputSpec(
              family,
              qualifier,
              maxVersions,
              filter,
              paging,
              schemaSpec
          )
      case Array(family) =>
          ColumnFamilyInputSpec(
              family,
              maxVersions,
              filter,
              paging,
              schemaSpec
          )
      case _ => throw new IllegalArgumentException("column name must contain 'family:qualifier'" +
        " for a group-type, or 'family' for a map-type column.")
    }
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnInputSpec]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family. The column will be read with the
   * schema of the provided specific Avro record.
   *
   * @param column The requested column name.
   * @param specificRecord class to read from the column.
   * @return ColumnInputSpec with supplied options.
   */
  def apply(
      column: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnInputSpec = {
    ColumnInputSpec(column, schemaSpec = SchemaSpec.Specific(specificRecord))
  }

  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnInputSpec]].  The input
   * spec will be for a qualified column if the column parameter contains a ':',
   * otherwise the input will assumed to be for a column family.  The column will be read with the
   * provided generic Avro schema.
   *
   * @param column The requested column name.
   * @param schema of generic Avro type to read from the column.
   * @return ColumnInputSpec with supplied options.
   */
  def apply(
      column: String,
      schema: Schema
  ): ColumnInputSpec = {
    ColumnInputSpec(column, schemaSpec = SchemaSpec.Generic(schema))
  }
}

/**
 * Specification for reading from a fully qualified column in a Kiji table.
 *
 * @param family The requested column family name.
 * @param qualifier The requested column qualifier name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is `None`).
 * @param paging Maximum number of cells to retrieve from HBase per page.
 * @param schemaSpec Reader schema specification. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
final case class QualifiedColumnInputSpec(
    family: String,
    qualifier: String,
    maxVersions: Int = latest,
    filter: Option[ColumnFilterSpec] = None,
    paging: PagingSpec = PagingSpec.Off,
    schemaSpec: SchemaSpec = SchemaSpec.Writer
) extends ColumnInputSpec {
  override val columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.QualifiedColumnInputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
object QualifiedColumnInputSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.QualifiedColumnInputSpec]] with
   * a specific Avro record type.
   *
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param specificRecord class to read from the column.
   * @return QualifiedColumnInputSpec with supplied options.
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
   * @param family The requested column family name.
   * @param qualifier The requested column qualifier name.
   * @param schema of generic Avro type to read from the column.
   * @return QualifiedColumnInputSpec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
  ): QualifiedColumnInputSpec = {
    QualifiedColumnInputSpec(family, qualifier, schemaSpec = SchemaSpec.Generic(schema))
  }
}

/**
 * Specification for reading from a column family in a Kiji table.
 *
 * @param family The requested column family name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is `None`).
 * @param paging Maximum number of cells to retrieve from HBase per RPC.
 * @param schemaSpec Reader schema specification. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
final case class ColumnFamilyInputSpec(
    family: String,
    maxVersions: Int = latest,
    filter: Option[ColumnFilterSpec] = None,
    paging: PagingSpec = PagingSpec.Off,
    schemaSpec: SchemaSpec = SchemaSpec.Writer
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
@ApiStability.Stable
@Inheritance.Sealed
object ColumnFamilyInputSpec {
  /**
   * Convenience function for creating a [[org.kiji.express.flow.ColumnFamilyInputSpec]] with a
   * specific Avro record type.
   *
   * @param family The requested column family name.
   * @param specificRecord class to read from the column.
   * @return ColumnFamilyInputSpec with supplied options.
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
   * @param family The requested column family name.
   * @param schema of Avro type to read from the column.
   * @return ColumnFamilyInputSpec with supplied options.
   */
  def apply(
      family: String,
      schema: Schema
  ): ColumnFamilyInputSpec = {
    ColumnFamilyInputSpec(family, schemaSpec = SchemaSpec.Generic(schema))
  }
}
