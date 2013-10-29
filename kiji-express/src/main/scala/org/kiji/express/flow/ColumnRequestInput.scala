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

import java.io.Serializable

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.hbase.HConstants

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Cell
import org.kiji.express.KijiSlice
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * Shared interface for all (group- and map-type) ColumnRequestInput objects.  Useful for any code
 * that performs operations on collections for [[org.kiji.express.flow.QualifiedColumnRequestInput]]
 * and [[org.kiji.express.flow.ColumnFamilyRequestInput]] objects.
 *
 * The entire API is read-only (outside of [[org.kiji.express]]).  We assume that users will specify
 * output requests using the factory methods `object ColumnRequestInput.`
 *
 */
private[express] trait ColumnRequestInput {

  // Note that the subclasses of ColumnRequestInput are case classes, and so they override
  // ColumnRequestInput's abstract methods (e.g., `maxVersions`) with vals.

  /**
   * Specifies the maximum number of cells (from the most recent) to retrieve from a column.
   *
   * By default, only the most recent cell is retrieved.
   */
  private[express] def maxVersions: Int

  /**
   * Specifies a filter that a cell must pass for this request to retrieve it.
   *
   * By default, there is no filter.
   */
  private[express] def filter: Option[KijiColumnFilter]

  /**
   * Specifies a default value to use for missing cells during a read.
   *
   * By default (or if this field is set to `None`), rows with missing values are ignored.
   */
  private[express] def default: Option[KijiSlice[_]]

  /**
   * Specifies the maximum number of cells to maintain in memory when paging through a column.
   *
   * By default (or if this field is set to
   * [[org.kiji.express.flow.ColumnRequestInput.PageSizePagingOff]]), paging is disabled.
   */
  private[express] def pageSize: Option[Int]

  /**
   * Specifies the class of data returned by a read, if forcing the results to be a SpecificRecord.
   *
   * By default (or if this field is set to None), results are not forced to be a SpecificRecord.
   */
  private[express] def avroClass: Option[Class[_ <: SpecificRecord]]

  /**
   * Creates a copy of this column request with `maxVersions` set appropriately for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: ColumnRequestInput

  /**
   * Returns the standard KijiColumnName representation of the name of the column this
   * ColumnRequests is for.
   *
   * @return the name of the column this ColumnRequestInput specifies.
   */
  private[express] def getColumnName: KijiColumnName

}

/**
 * Specification for reading from a fully qualified column in a Kiji table.
 *
 * @param family The requested column family name.
 * @param qualifier The requested column qualifier name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is `None`)
 * @param pageSize The maximum number of cells to keep in memory when paging (default is `None`,
 *     paging disabled)
 * @param avroClass Avro class to use for data being read (default is `None`).
 */
final case class QualifiedColumnRequestInput (
    val family: String,
    val qualifier: String,
    val maxVersions: Int = latest,
    val filter: Option[KijiColumnFilter] = None,
    val default: Option[KijiSlice[_]] = None,
    val pageSize: Option[Int] = None,
    val avroClass: Option[Class[_ <: SpecificRecord]] = None
) extends ColumnRequestInput {

  /** Returns name (`family:qualifier`) for requested column as KijiColumnName. */
  override def getColumnName: KijiColumnName = new KijiColumnName(family, qualifier)

  /**
   * Legacy methods for easily specifying a default value for missing data.
   *
   * We shall remove these methods and replace their functionality with better factory methods for
   *     `KijiSlice.`
   */
  private[express] def replaceMissingWith[T](datum: T): QualifiedColumnRequestInput = {
    val default = new KijiSlice(List(Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum)))
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWithVersioned[T](
      version: Long, datum: T): QualifiedColumnRequestInput = {
    val default = new KijiSlice(List(Cell( family, qualifier, version, datum)))
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWith[T](data: Seq[T]): QualifiedColumnRequestInput = {
    val default = new KijiSlice(data.map { datum: T =>
        Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum) })
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWithVersioned[T](
      versionedData: Seq[(Long, T)]): QualifiedColumnRequestInput = {
    val default = new KijiSlice(versionedData.map { case (version: Long, datum: Any) =>
        Cell(family, qualifier, version, datum) })
    copy(default=Some(default))
  }

  /**
   * Creates a copy of this column request with `maxVersions` set appropriately for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: QualifiedColumnRequestInput =
      copy(maxVersions = Integer.MAX_VALUE)
}

/**
 * Specification for reading from a map-type column family in a Kiji table.
 *
 * The constructor for this class is private.  Please instantiate objects with the constructor in
 *     `object ColumnRequestInput`
 *
 * @param family The requested column family name.
 * @param maxVersions The maximum number of versions to read back (default is only most recent).
 * @param filter Filter to use when reading back cells (default is `None`)
 * @param pageSize The maximum number of cells to keep in memory when paging (default is `None`,
 *     paging disabled)
 * @param avroClass Avro class to use for data being read (default is `None`).
 */
final case class ColumnFamilyRequestInput (
    val family: String,
    val maxVersions: Int = latest,
    val filter: Option[KijiColumnFilter] = None,
    val default: Option[KijiSlice[_]] = None,
    val pageSize: Option[Int] = None,
    val avroClass: Option[Class[_ <: SpecificRecord]] = None
) extends ColumnRequestInput {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot hae a : in family name for map-type request")
  }

  /** Returns name (`family`) for requested column as KijiColumnName. */
  override def getColumnName: KijiColumnName = new KijiColumnName(family)

  /**
   * Legacy methods for easily specifying a default value for missing data.
   *
   * We shall remove these methods and replace their functionality with better factory methods for
   *     `KijiSlice.`
   */
  private[express] def replaceMissingWith[T](
      qualifier: String, datum: T): ColumnFamilyRequestInput = {
    val default = new KijiSlice(List(Cell( family, qualifier, HConstants.LATEST_TIMESTAMP, datum)))
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWithVersioned[T](
      qualifier: String, version: Long, datum: T): ColumnFamilyRequestInput = {
    val default = new KijiSlice(List(Cell( family, qualifier, version, datum)))
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWith[T](data: Seq[(String, T)]): ColumnFamilyRequestInput = {
    val default = new KijiSlice(data.map { case (qualifier: String, datum: Any) =>
                Cell(family, qualifier, HConstants.LATEST_TIMESTAMP, datum) })
    copy(default=Some(default))
  }

  /** See previous comment. */
  private[express] def replaceMissingWithVersioned[T](
      versionedData: Seq[(String, Long, T)]): ColumnFamilyRequestInput = {
    val default = new KijiSlice(versionedData.map {
      case (qualifier: String, version: Long, datum: Any)
        => Cell(family, qualifier, version, datum) })
      copy(default=Some(default))
  }

  /**
   * Creates a copy of this column request with `maxVersions` set appropriately for testing.
   *
   * Used within Express for testing infrastructure.
   */
  private[express] def newGetAllData: ColumnFamilyRequestInput =
      copy(maxVersions = Integer.MAX_VALUE)
}

/** Factory object for creating group- and map-type column input requests. */
private[express] object ColumnRequestInput {

  /**
   * Factory method for `QualifiedColumnRequestInput` and `ColumnFamilyRequestInput`.
   *
   * For internal use only, not part of the public API.
   *
   * Will choose to create a group or map-type request appropriately depending on whether the
   * specified `columnName` contains a `:` or not.
   *
   * @param columnName name of the requested column.  Requests for group-type columns should be of
   *     the form `family:qualifier`.  Requests for map-type columns should be of the form
   *     `family`.
   * @param maxVersions The maximum number of versions to read back (default is only most recent).
   * @param filter Filter to use when reading back cells (default is `None`)
   * @param pageSize The maximum number of cells to keep in memory when paging (default is `None`,
   *     paging disabled)
   * @param avroClass Avro class to use for data being read (default is `None`).
   * @return A `QualifiedColumnRequestInput` or `ColumnFamilyRequestInput` object (depending
   *     on the presence of a `:` in `columnName`) with its fields populated per the parameters.
   */
  private[express] def apply(
    columnName: String,
    maxVersions: Int = latest,
    filter: Option[KijiColumnFilter] = None,
    default: Option[KijiSlice[_]] = None,
    pageSize: Option[Int] = None,
    avroClass: Option[Class[_ <: SpecificRecord]] = None
  ): ColumnRequestInput = {
    val kijiColumn = new KijiColumnName(columnName)
    if (kijiColumn.isFullyQualified) {
      new QualifiedColumnRequestInput(
        family = kijiColumn.getFamily(),
        qualifier = kijiColumn.getQualifier(),
        maxVersions = maxVersions,
        filter = filter,
        default = default,
        pageSize = pageSize,
        avroClass = avroClass
      )
    } else {
      new ColumnFamilyRequestInput(
        family = kijiColumn.getFamily(),
        maxVersions = maxVersions,
        filter = filter,
        default = default,
        pageSize = pageSize,
        avroClass = avroClass
      )
    }
  }
}
