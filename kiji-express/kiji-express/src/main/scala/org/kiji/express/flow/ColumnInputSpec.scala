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

import com.google.common.base.Objects

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
  val DEFAULT_COLUMN_FILTER_SPEC = ColumnFilterSpec.NoColumnFilterSpec

  /**
   * A request for data from a Kiji table column. The input spec will be for a qualified column if
   * the column parameter contains a ':', otherwise the input will assumed to be for a column family
   * (column family names cannot contain ';' characters).
   *
   * @param column name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji
   *        per page.
   * @param schemaSpec specifies the schema to use when reading cells. Defaults to
   *     [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return a new column input spec with supplied options.
   */
  private[express] def apply(
      column: String,
      maxVersions: Int = DEFAULT_MAX_VERSIONS,
      filterSpec: ColumnFilterSpec = DEFAULT_COLUMN_FILTER_SPEC,
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
}

/**
 * Specifies a request for versions of cells from a fully-qualified column.
 *
 * Basic example that reads data into generic records using the schema with which they were written:
 * {{{
 *   // Request the latest version of data stored in the "info:name" column.
 *   val myColumnSpec: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec.builder
 *           .withColumn("info", "name")
 *           .withMaxVersions(1)
 *           .build
 * }}}
 *
 * Paging can be enabled on a column input specification causing blocks of cells to be retrieved
 * from Kiji at a time:
 * {{{
 *   // Request cells from the "info:status" column retrieving 1000 cells per block.
 *   val myPagedColumn: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec.builder
 *           .withColumn("info", "status")
 *           .withMaxVersions(flow.all)
 *           .withPagingSpec(PagingSpec.Cells(1000))
 *           .build
 * }}}
 *
 * If compiled avro classes are being used, a compiled record class can be specified. Data read from
 * this column will be of the specified type:
 * {{{
 *   // Request cells from the "info:user" column containing User records.
 *   val myColumnSpec: QualifiedColumnInputSpec =
 *       QualifiedColumnInputSpec.builder
 *           .withColumn("info", "user")
 *           .withMaxVersions(1)
 *           .withSchemaSpec(SchemaSpec.Specific(classOf[User]))
 *           .build
 * }}}
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 *
 * @param family of columns the requested data belongs to.
 * @param qualifier of the column the requested data belongs to.
 * @param maxVersions to read back from the requested column (default is only most recent).
 * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
 * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji per page.
 * @param schemaSpec specifies the schema to use when reading cells. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
final class QualifiedColumnInputSpec private(
    val family: String,
    val qualifier: String,
    val maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
    val filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC,
    val pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
    val schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnInputSpec with Serializable {
  override def columnName: KijiColumnName = new KijiColumnName(family, qualifier)

  override def toString: String = Objects.toStringHelper(classOf[QualifiedColumnInputSpec])
      .add("family", family)
      .add("qualifier", qualifier)
      .add("max_versions", maxVersions)
      .add("filter_spec", filterSpec)
      .add("paging_spec", pagingSpec)
      .add("schema_spec", schemaSpec)
      .toString

  override def hashCode: Int =
      Objects.hashCode(
          family,
          qualifier,
          maxVersions: java.lang.Integer,
          filterSpec,
          pagingSpec,
          schemaSpec)

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[QualifiedColumnInputSpec]) {
      false
    } else {
      val other = obj.asInstanceOf[QualifiedColumnInputSpec]
      family == other.family &&
          qualifier == other.qualifier &&
          maxVersions == other.maxVersions &&
          filterSpec == other.filterSpec &&
          pagingSpec == other.pagingSpec &&
          schemaSpec == other.schemaSpec
    }
  }
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
   * a generic Avro type specified by a [[org.apache.avro.Schema]].
   *
   * @param family of columns the requested data belongs to.
   * @param qualifier of the column the requested data belongs to.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
   * @param pagingSpec options specifying the maximum number of cells to retrieve
   *        from Kiji per page.
   * @param schemaSpec specification with which to read data.
   * @return a new column input spec with supplied options.
   */
  private[express] def apply(
      family: String,
      qualifier: String,
      maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
      filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC,
      pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
      schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
  ): QualifiedColumnInputSpec = {
    new QualifiedColumnInputSpec(
        family,
        qualifier,
        maxVersions,
        filterSpec,
        pagingSpec,
        schemaSpec)
  }

  /**
   * Decompose the given object into its constituent parts if it is an instance of
   * QualifiedColumnInputSpec.
   *
   * @param target object to decompose if it is a QualifiedColumnInputSpec.
   * @return the fields used to construct the target.
   *     (family, qualifier, maxVersions, filterSpec, pagingSpec, schemaSpec)
   */
  private[express] def unapply(
      target: Any
  ): Option[(
      String,
      String,
      Int,
      ColumnFilterSpec,
      PagingSpec,
      SchemaSpec)] = PartialFunction.condOpt(target) {
    case qcis: QualifiedColumnInputSpec => (
        qcis.family,
        qcis.qualifier,
        qcis.maxVersions,
        qcis.filterSpec,
        qcis.pagingSpec,
        qcis.schemaSpec)
  }

  /**
   * A request for data from a fully qualified Kiji table column.
   * This construct method is used by Java builders for ColumnInputSpec.
   * Scala users ought to use the Builder APIs.
   *
   * @param column is the fully qualified column name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells. Defaults to
   *        [[org.kiji.express.flow.ColumnFilterSpec.NoColumnFilterSpec]].
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji
   *        per page. Defaults to [[org.kiji.express.flow.PagingSpec.Off]].
   * @param schemaSpec specifies the schema to use when reading cells. Defaults to
   *        [[org.kiji.express.flow.SchemaSpec.Writer]].
   * @return a new column input spec with supplied options.
   */
  private[express] def construct(
      column: KijiColumnName,
      maxVersions: java.lang.Integer,
      filterSpec: ColumnFilterSpec,
      pagingSpec: PagingSpec,
      schemaSpec: SchemaSpec
  ): QualifiedColumnInputSpec = {
    // Construct QualifiedColumnInputSpec
    new QualifiedColumnInputSpec(
        column.getFamily(),
        column.getQualifier(),
        Option(maxVersions) match {
          case None => ColumnInputSpec.DEFAULT_MAX_VERSIONS
          case _ => maxVersions
        },
        Option(filterSpec).getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC),
        Option(pagingSpec).getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
        Option(schemaSpec).getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )
  }

  /**
   * Create a new QualifiedColumnInputSpec.Builder.
   *
   * @return a new QualifiedColumnInputSpec.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new QualifiedColumnInputSpec.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new QualifiedColumnInputSpec.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for QualifiedColumnInputSpec.
   *
   * @param constructorFamily optional family with which to initialize this builder.
   * @param constructorQualifier optional qualifier with which to initialize this builder.
   * @param constructorMaxVersions optional maxVersions with which to initialize this builder.
   * @param constructorFilterSpec optional FilterSpec with which to initialize this builder.
   * @param constructorPagingSpec optional PagingSpec with which to initialize this builder.
   * @param constructorSchemaSpec optional SchemaSpec with which to initialize this builder.
   */
  final class Builder private(
      constructorFamily: Option[String],
      constructorQualifier: Option[String],
      constructorMaxVersions: Option[Int],
      constructorFilterSpec: Option[ColumnFilterSpec],
      constructorPagingSpec: Option[PagingSpec],
      constructorSchemaSpec: Option[SchemaSpec]
  ) {
    private[this] val monitor = new AnyRef
    private var mFamily: Option[String] = constructorFamily
    private var mQualifier: Option[String] = constructorQualifier
    private var mMaxVersions: Option[Int] = constructorMaxVersions
    private var mFilterSpec: Option[ColumnFilterSpec] = constructorFilterSpec
    private var mPagingSpec: Option[PagingSpec] = constructorPagingSpec
    private var mSchemaSpec: Option[SchemaSpec] = constructorSchemaSpec

    /**
     * Configure the input spec to read the given Kiji column.
     *
     * @param column into which to read the values
     * @return this builder.
     */
    def withColumn(column: KijiColumnName): Builder = monitor.synchronized {
      require(column.isFullyQualified(), "Column must be fully qualified.")
      require(None == mFamily, "Family already set to: " + mFamily.get)
      require(None == mQualifier, "Qualifier already set to: " + mQualifier.get)
      mFamily = Some(column.getFamily)
      mQualifier = Some(column.getQualifier)
      this
    }

    /**
     * Configure the input spec to read the given Kiji column.
     *
     * @param family of the column from which to read.
     * @param qualifier of the column from which to read.
     * @return this builder.
     */
    def withColumn(family: String, qualifier: String): Builder = monitor.synchronized {
      require(None == mFamily, "Family already set to: " + mFamily.get)
      require(None == mQualifier, "Qualifier already set to: " + mQualifier.get)
      mFamily = Some(family)
      mQualifier = Some(qualifier)
      this
    }

    /**
     * Configure the input spec to read from the given Kiji column family. Must also call
     * [[org.kiji.express.flow.QualifiedColumnInputSpec.Builder.withQualifier()]] before calling
     * [[org.kiji.express.flow.QualifiedColumnInputSpec.Builder.build]].
     *
     * @param family of the column from which to read.
     * @return this builder.
     */
    def withFamily(family: String): Builder = monitor.synchronized {
      require(None == mFamily, "Family already set to: " + mFamily.get)
      mFamily = Some(family)
      this
    }

    /**
     * Configure the input spec to read from the given Kiji column qualifier. Must also call
     * [[org.kiji.express.flow.QualifiedColumnInputSpec.Builder.withFamily()]] before calling
     * [[org.kiji.express.flow.QualifiedColumnInputSpec.Builder.build]].
     *
     * @param qualifier of the column from which to read.
     * @return this builder.
     */
    def withQualifier(qualifier: String): Builder = monitor.synchronized {
      require(None == mQualifier, "Qualifier already set to: " + mQualifier.get)
      mQualifier = Some(qualifier)
      this
    }

    /**
     * Name of the Kiji column family from which to read.
     *
     * @return the name of the Kiji column family from which to read.
     */
    def family: Option[String] = mFamily

    /**
     * Name of the Kiji column qualifier from which to read.
     *
     * @return the name of the Kiji column qualifier from which to read.
     */
    def qualifier: Option[String] = mQualifier

    /**
     * Configure the input spec to read the specified maximum versions.
     *
     * @param maxVersions to read back from the requested column (default is only most recent).
     * @return this builder.
     */
    def withMaxVersions(maxVersions: Int): Builder = monitor.synchronized {
      require(None == mMaxVersions, "Max versions already set to: " + mMaxVersions.get)
      require(0 < maxVersions, "Max versions must be strictly positive, instead got " + maxVersions)
      mMaxVersions = Some(maxVersions)
      this
    }

    /**
     * The maximum number of versions requested for reading.
     *
     * @return the maximum versions to read back from requested column.
     */
    def maxVersions: Option[Int] = mMaxVersions

    /**
     * Configure the input spec to read using the given FilterSpec.
     *
     * @param filterSpec defining the filter which will be used to read this column.
     * @return this builder.
     */
    def withFilterSpec(filterSpec: ColumnFilterSpec): Builder = monitor.synchronized {
      require(None == mFilterSpec, "Filter spec already set to: " + mFilterSpec.get)
      mFilterSpec = Some(filterSpec)
      this
    }

    /**
     * Specification of the filter to use when reading this column.
     *
     * @return a specification of the filter to use when reading this column.
     */
    def filterSpec: Option[ColumnFilterSpec] = mFilterSpec

    /**
     * Configure the input spec to page the read data according to the given specification.
     *
     * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji.
     * @return this builder.
     */
    def withPagingSpec(pagingSpec: PagingSpec): Builder = monitor.synchronized {
      require(None == mPagingSpec, "Paging spec already set to: " + mPagingSpec.get)
      mPagingSpec = Some(pagingSpec)
      this
    }

    /**
     * Paging specification containing the maximum number of cells to retrieve from Kiji.
     *
     * @return paging specification containing the maximum number of cells to retrieve from Kiji.
     */
    def pagingSpec: Option[PagingSpec] = mPagingSpec

    /**
     * Configure the input spec to read using the given SchemaSpec.
     *
     * @param schemaSpec defining the Schema which will be used to read this column.
     * @return this builder.
     */
    def withSchemaSpec(schemaSpec: SchemaSpec): Builder = monitor.synchronized {
      require(None == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec.get)
      mSchemaSpec = Some(schemaSpec)
      this
    }

    /**
     * Specification of the Schema to use when reading this column.
     *
     * @return a specification of the Schema to use when reading this column.
     */
    def schemaSpec: Option[SchemaSpec] = mSchemaSpec

    /**
     * Build a new QualifiedColumnInputSpec from the values stored in this builder.
     *
     * @return a new QualifiedColumnInputSpec from the values stored in this builder.
     */
    def build: QualifiedColumnInputSpec = new QualifiedColumnInputSpec(
      mFamily.getOrElse(throw new IllegalArgumentException("family must be specified.")),
      mQualifier.getOrElse(throw new IllegalArgumentException("qualifier must be specified.")),
      mMaxVersions.getOrElse(ColumnInputSpec.DEFAULT_MAX_VERSIONS),
      mFilterSpec.getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC),
      mPagingSpec.getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
      mSchemaSpec.getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )

    override def toString: String = Objects.toStringHelper(classOf[Builder])
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("max_versions", mMaxVersions)
        .add("filter_spec", mFilterSpec)
        .add("paging_spec", mPagingSpec)
        .add("schema_spec", mSchemaSpec)
        .toString

    override def hashCode: Int =
        Objects.hashCode(mFamily, mQualifier, mMaxVersions, mFilterSpec, mPagingSpec, mSchemaSpec)

    override def equals(target: Any): Boolean = {
      if (!target.isInstanceOf[Builder]) {
        false
      } else {
        val other: Builder = target.asInstanceOf[Builder]
        family == other.family &&
            qualifier == other.qualifier &&
            maxVersions == other.maxVersions &&
            filterSpec == other.filterSpec &&
            pagingSpec == other.pagingSpec &&
            schemaSpec == other.schemaSpec
      }
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.QualifiedColumnInputSpec.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  object Builder {

    /**
     * Create a new empty QualifiedColumnInputSpec.Builder.
     *
     * @return a new empty QualifiedColumnInputSpec.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None, None, None, None)

    /**
     * Create a new QualifiedColumnInputSpec.Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new QualifiedColumnInputSpec.Builder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = {
      new Builder(other.family,
          other.qualifier,
          other.maxVersions,
          other.filterSpec,
          other.pagingSpec,
          other.schemaSpec)
    }
  }
}

/**
 * Specifies a request for versions of cells from a column family.
 *
 * Basic column family example:
 * {{{
 *   // Request the latest version of data stored in the "matrix" column family.
 *   val myColumnFamilySpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec.builder
 *           .withFamily("matrix")
 *           .withMaxVersions(1)
 *           .build
 * }}}
 *
 * Filters can be applied to the column qualifier of cells in a column family.
 * {{{
 *   // Request cells from the "hits" column that are from columns with qualifiers that begin with
 *   // the string "http://www.wibidata.com/".
 *   val myFilteredColumnSpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec.builder
 *           .withFamily("hits")
 *           .withMaxVersions(flow.all)
 *           .withFilterSpec(RegexQualifierFilterSpec("http://www\.wibidata\.com/.*")
 *           .build
 * }}}
 *
 * Paging can be enabled on a column input specification causing blocks of cells to be retrieved
 * from Kiji at a time:
 * {{{
 *   // Request cells from the "metadata" column family retrieving 1000 cells per block.
 *   val myPagedColumn: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec.builder
 *           .withFamily("metadata")
 *           .withMaxVersions(flow.all)
 *           .withPagingSpec(PagingSpec.Cells(1000))
 *           .build
 * }}}
 *
 * If compiled avro classes are being used, a class that data should be read as can be specified:
 * {{{
 *   // Request cells from the "users" column family containing User records.
 *   val myColumnSpec: ColumnFamilyInputSpec =
 *       ColumnFamilyInputSpec.builder
 *           .withFamily("users")
 *           .withMaxVersions(1)
 *           .withSchemaSpec(SchemaSpec.Specific(classOf[User]))
 *           .build
 * }}}
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 *
 * @param family of columns the requested data belongs to.
 * @param maxVersions to read back from the requested column family (default is only most recent).
 * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
 * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji per page.
 * @param schemaSpec specifies the schema to use when reading cells. Defaults to
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ColumnFamilyInputSpec private(
    val family: String,
    val maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
    val filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC,
    val pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
    val schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnInputSpec with Serializable {
  if (family.contains(':')) {
    throw new KijiInvalidNameException("Cannot have a ':' in family name for column family request")
  }
  override def columnName: KijiColumnName = new KijiColumnName(family)

  override def toString: String = Objects.toStringHelper(classOf[ColumnFamilyInputSpec])
      .add("family", family)
      .add("max_versions", maxVersions)
      .add("filter_spec", filterSpec)
      .add("paging_spec", pagingSpec)
      .add("schema_spec", schemaSpec)
      .toString

  override def hashCode: Int =
      Objects.hashCode(
          family,
          maxVersions: java.lang.Integer,
          filterSpec,
          pagingSpec,
          schemaSpec)

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ColumnFamilyInputSpec]) {
      false
    } else {
      val other = obj.asInstanceOf[ColumnFamilyInputSpec]
      family == other.family &&
          maxVersions == other.maxVersions &&
          filterSpec == other.filterSpec &&
          pagingSpec == other.pagingSpec &&
          schemaSpec == other.schemaSpec
    }
  }
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
   * Create a new ColumnFamilyInputSpec from the given parameters.
   *
   * @param family of columns the requested data belongs to.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
   * @param pagingSpec options specifying the maximum number of cells to retrieve
   *        from Kiji per page.
   * @param schemaSpec specification with which to read data.
   * @return a new column input spec with supplied options.
   */
  private[express] def apply(
      family: String,
      maxVersions: Int = ColumnInputSpec.DEFAULT_MAX_VERSIONS,
      filterSpec: ColumnFilterSpec = ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC,
      pagingSpec: PagingSpec = ColumnInputSpec.DEFAULT_PAGING_SPEC,
      schemaSpec: SchemaSpec = ColumnInputSpec.DEFAULT_SCHEMA_SPEC
  ): ColumnFamilyInputSpec = {
    new ColumnFamilyInputSpec(
        family,
        maxVersions,
        filterSpec,
        pagingSpec,
        schemaSpec)
  }

  /**
   * Decompose the given object into its constituent parts if it is an instance of
   * ColumnFamilyInputSpec.
   *
   * @param target object to decompose if it is a ColumnFamilyInputSpec.
   * @return the fields used to construct the target.
   *     (family, maxVersions, filterSpec, pagingSpec, schemaSpec)
   */
  private[express] def unapply(
      target: Any
  ): Option[(
      String,
      Int,
      ColumnFilterSpec,
      PagingSpec,
      SchemaSpec)] = PartialFunction.condOpt(target) {
    case qcis: ColumnFamilyInputSpec => (
        qcis.family,
        qcis.maxVersions,
        qcis.filterSpec,
        qcis.pagingSpec,
        qcis.schemaSpec)
  }

  /**
   * A request for data from a Kiji table column family.
   * This construct method is used by Java builders for ColumnInputSpec.
   * Scala users ought to use the Builder APIs.
   *
   * @param column family name of the requested data.
   * @param maxVersions to read back from the requested column (default is only most recent).
   * @param filterSpec to use when reading back cells (default is NoColumnFilterSpec).
   * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji per
   *     page. Defaults to [[org.kiji.express.flow.PagingSpec.Off]].
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
  ): ColumnFamilyInputSpec = {
    // Construct ColumnFamilyInputSpec
    ColumnFamilyInputSpec(
        column.getFamily(),
        Option(maxVersions) match {
          case None => ColumnInputSpec.DEFAULT_MAX_VERSIONS
          case _ => maxVersions
        },
        Option(filterSpec).getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC),
        Option(pagingSpec).getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
        Option(schemaSpec).getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )
  }

  /**
   * Create a new ColumnFamilyInputSpec.Builder.
   *
   * @return a new ColumnFamilyInputSpec.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new ColumnFamilyInputSpec.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new ColumnFamilyInputSpec.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for ColumnFamilyInputSpec.
   *
   * @param constructorFamily optional family with which to initialize this builder.
   * @param constructorMaxVersions optional maxVersions with which to initialize this builder.
   * @param constructorFilterSpec optional FilterSpec with which to initialize this builder.
   * @param constructorPagingSpec optional PagingSpec with which to initialize this builder.
   * @param constructorSchemaSpec optional SchemaSpec with which to initialize this builder.
   */
  final class Builder private(
      constructorFamily: Option[String],
      constructorMaxVersions: Option[Int],
      constructorFilterSpec: Option[ColumnFilterSpec],
      constructorPagingSpec: Option[PagingSpec],
      constructorSchemaSpec: Option[SchemaSpec]
  ) {
    private[this] val monitor = new AnyRef
    private var mFamily: Option[String] = constructorFamily
    private var mMaxVersions: Option[Int] = constructorMaxVersions
    private var mFilterSpec: Option[ColumnFilterSpec] = constructorFilterSpec
    private var mPagingSpec: Option[PagingSpec] = constructorPagingSpec
    private var mSchemaSpec: Option[SchemaSpec] = constructorSchemaSpec

    /**
     * Configure the input spec to read the given Kiji column family.
     *
     * @param column family into which to read the values
     * @return this builder.
     */
    def withColumn(column: KijiColumnName): Builder = monitor.synchronized {
      require(!column.isFullyQualified(), "Column family may not be fully qualified.")
      require(None == mFamily, "Family already set to: " + mFamily.get)
      mFamily = Some(column.getFamily)
      this
    }

    /**
     * Configure the input spec to read from the given Kiji column family.
     *
     * @param family of the column from which to read.
     * @return this builder.
     */
    def withFamily(family: String): Builder = monitor.synchronized {
      require(None == mFamily, "Family already set to: " + mFamily.get)
      mFamily = Some(family)
      this
    }

    /**
     * Name of the Kiji column family from which to read.
     *
     * @return the name of the Kiji column family from which to read.
     */
    def family: Option[String] = mFamily

    /**
     * Configure the input spec to read the specified maximum versions.
     *
     * @param maxVersions to read back from the requested column (default is only most recent).
     * @return this builder.
     */
    def withMaxVersions(maxVersions: Int): Builder = monitor.synchronized {
      require(None == mMaxVersions, "Max versions already set to: " + mMaxVersions.get)
      require(0 < maxVersions, "Max versions must be strictly positive, instead got " + maxVersions)
      mMaxVersions = Some(maxVersions)
      this
    }

    /**
     * The maximum number of versions requested for reading.
     *
     * @return the maximum versions to read back from requested column.
     */
    def maxVersions: Option[Int] = mMaxVersions

    /**
     * Configure the input spec to read using the given FilterSpec.
     *
     * @param filterSpec defining the filter which will be used to read this column.
     * @return this builder.
     */
    def withFilterSpec(filterSpec: ColumnFilterSpec): Builder = monitor.synchronized {
      require(None == mFilterSpec, "Filter spec already set to: " + mFilterSpec.get)
      mFilterSpec = Some(filterSpec)
      this
    }

    /**
     * Specification of the filter to use when reading this column.
     *
     * @return a specification of the filter to use when reading this column.
     */
    def filterSpec: Option[ColumnFilterSpec] = mFilterSpec

    /**
     * Configure the input spec to page the read data according to the given specification.
     *
     * @param pagingSpec options specifying the maximum number of cells to retrieve from Kiji.
     * @return this builder.
     */
    def withPagingSpec(pagingSpec: PagingSpec): Builder = monitor.synchronized {
      require(None == mPagingSpec, "Paging spec already set to: " + mPagingSpec.get)
      mPagingSpec = Some(pagingSpec)
      this
    }

    /**
     * Paging specification containing the maximum number of cells to retrieve from Kiji.
     *
     * @return paging specification containing the maximum number of cells to retrieve from Kiji.
     */
    def pagingSpec: Option[PagingSpec] = mPagingSpec

    /**
     * Configure the input spec to read using the given SchemaSpec.
     *
     * @param schemaSpec defining the Schema which will be used to read this column.
     * @return this builder.
     */
    def withSchemaSpec(schemaSpec: SchemaSpec): Builder = monitor.synchronized {
      require(None == mSchemaSpec, "Schema spec already set to: " + mSchemaSpec.get)
      mSchemaSpec = Some(schemaSpec)
      this
    }

    /**
     * Specification of the Schema to use when reading this column.
     *
     * @return a specification of the Schema to use when reading this column.
     */
    def schemaSpec: Option[SchemaSpec] = mSchemaSpec

    /**
     * Build a new ColumnFamilyInputSpec from the values stored in this builder.
     *
     * @return a new ColumnFamilyInputSpec from the values stored in this builder.
     */
    def build: ColumnFamilyInputSpec = new ColumnFamilyInputSpec(
      mFamily.getOrElse(throw new IllegalArgumentException("family must be specified.")),
      mMaxVersions.getOrElse(ColumnInputSpec.DEFAULT_MAX_VERSIONS),
      mFilterSpec.getOrElse(ColumnInputSpec.DEFAULT_COLUMN_FILTER_SPEC),
      mPagingSpec.getOrElse(ColumnInputSpec.DEFAULT_PAGING_SPEC),
      mSchemaSpec.getOrElse(ColumnInputSpec.DEFAULT_SCHEMA_SPEC)
    )

    override def toString: String = Objects.toStringHelper(classOf[Builder])
        .add("family", mFamily)
        .add("max_versions", mMaxVersions)
        .add("filter_spec", mFilterSpec)
        .add("paging_spec", mPagingSpec)
        .add("schema_spec", mSchemaSpec)
        .toString

    override def hashCode: Int =
        Objects.hashCode(mFamily, mMaxVersions, mFilterSpec, mPagingSpec, mSchemaSpec)

    override def equals(target: Any): Boolean = {
      if (!target.isInstanceOf[Builder]) {
        false
      } else {
        val other: Builder = target.asInstanceOf[Builder]
        family == other.family &&
            maxVersions == other.maxVersions &&
            filterSpec == other.filterSpec &&
            pagingSpec == other.pagingSpec &&
            schemaSpec == other.schemaSpec
      }
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.ColumnFamilyInputSpec.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  object Builder {

    /**
     * Create a new empty ColumnFamilyInputSpec.Builder.
     *
     * @return a new empty ColumnFamilyInputSpec.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None, None, None)

    /**
     * Create a new ColumnFamilyInputSpec.Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new ColumnFamilyInputSpec.Builder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = {
      new Builder(other.family,
          other.maxVersions,
          other.filterSpec,
          other.pagingSpec,
          other.schemaSpec)
    }
  }
}
