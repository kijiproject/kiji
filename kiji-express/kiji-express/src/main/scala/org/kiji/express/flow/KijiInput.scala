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
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiURI

/**
 * Factory methods for constructing [[org.kiji.express.flow.KijiSource]]s that will be used as
 * inputs to a KijiExpress flow.
 *
 * Example usage:
 *
 * {{{
 *   val column3 = QualifiedColumnInputSpec.builder
 *       .withColumn("info", "column3")
 *       .withSchemaSpec(DefaultReader)
 *       .build
 *
 *   KijiInput.builder
 *       .withTableURI("kiji://localhost:2181/default/mytable")
 *       .withTimeRangeSpec(TimeRangeSpec.Between(5, 10))
 *       .withColumns("info:column1" -> 'column1, "info:column2" -> 'column2)
 *       .addColumnSpecs(column3 -> 'column3)
 *       // Selects a 30% sample of data between startEid and endEid.
 *       .withRowRangeSpec(RowRangeSpec.Between(startEid, endEid)
 *       .withRowFilterSpec(RowFilterSpec.Random(0.3F))
 *       .build
 * }}}
 *
 * Note: Columns containing no values will be replaced with an empty sequence unless all requested
 *     columns are empty in which case the entire row will be skipped.
 */
@ApiAudience.Public
@ApiStability.Stable
object KijiInput {
  /** Default time range for KijiSource */
  private val DEFAULT_TIME_RANGE: TimeRangeSpec = TimeRangeSpec.All

  /**
   * Create a new empty KijiInput.Builder.
   *
   * @return a new empty KijiInput.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new KijiInput.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new KijiInput.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for [[org.kiji.express.flow.KijiSource]]s to be used as inputs.
   *
   * @param mTableURI string of the table from which to read.
   * @param mTimeRange from which to read values.
   * @param mColumnSpecs specification of columns from which to read.
   * @param mRowRangeSpec rows from which to read.
   * @param mRowFilterSpec filters used to read.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mTableURI: Option[String],
      private[this] var mTimeRange: Option[TimeRangeSpec],
      private[this] var mColumnSpecs: Option[Map[_ <: ColumnInputSpec, Symbol]],
      private[this] var mRowRangeSpec: Option[RowRangeSpec],
      private[this] var mRowFilterSpec: Option[RowFilterSpec]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Get the Kiji URI of the table from which to read from this Builder.
     *
     * @return the Kiji URI of the table from which to read from this Builder.
     */
    def tableURI: Option[String] = monitor.synchronized(mTableURI)

    /**
     * Get the input time range specification from this Builder.
     *
     * @return the input time range specification from this Builder.
     */
    def timeRange: Option[TimeRangeSpec] = monitor.synchronized(mTimeRange)

    /**
     * Get the input specifications from this Builder.
     *
     * @return the input specifications from this Builder.
     */
    def columnSpecs: Option[Map[_ <: ColumnInputSpec, Symbol]] = monitor.synchronized(mColumnSpecs)

    /**
     * Get the input row range specification from this Builder.
     *
     * @return the input row range specification from this Builder.
     */
    def rowRangeSpec: Option[RowRangeSpec] = monitor.synchronized(mRowRangeSpec)

    /**
     * Get the input row filter specification from this Builder.
     *
     * @return the input row filter specification from this Builder.
     */
    def rowFilterSpec: Option[RowFilterSpec] = monitor.synchronized(mRowFilterSpec)

    /**
     * Configure the KijiSource to read values from the table with the given Kiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this builder.
     */
    def withTableURI(tableURI: String): Builder = monitor.synchronized {
      require(tableURI != null, "Table URI may not be null.")
      require(mTableURI.isEmpty, "Table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Configure the KijiSource to read values from the table with the given Kiji URI.
     *
     * @param tableURI of the table from which to read.
     * @return this builder.
     */
    def withTableURI(tableURI: KijiURI): Builder = withTableURI(tableURI.toString)

    /**
     * Configure the KijiSource to read values from the given range of input times.
     *
     * @param timeRangeSpec specification of times from which to read.
     * @return this builder.
     */
    def withTimeRangeSpec(timeRangeSpec: TimeRangeSpec): Builder = monitor.synchronized {
      require(timeRangeSpec != null, "Time range may not be null.")
      require(mTimeRange.isEmpty, "Time range already set to: " + mTimeRange.get)
      mTimeRange = Some(timeRangeSpec)
      this
    }

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumns(columns: (String, Symbol)*): Builder = withColumns(columns.toMap)

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumns(columns: Map[String, Symbol]): Builder =
        withColumnSpecs(columns.map { Builder.columnToSpec })

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumns(columns: (String, Symbol)*): Builder = addColumns(columns.toMap)

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columns mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumns(columns: Map[String, Symbol]): Builder =
        addColumnSpecs(columns.map { Builder.columnToSpec })

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: (_ <: ColumnInputSpec, Symbol)*): Builder =
        withColumnSpecs(columnSpecs.toMap[ColumnInputSpec, Symbol])

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: (_ <: ColumnInputSpec, Symbol)*): Builder =
        addColumnSpecs(columnSpecs.toMap[ColumnInputSpec, Symbol])

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: Map[_ <: ColumnInputSpec, Symbol]): Builder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.values.toSet.size,
        "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        require(mColumnSpecs.isEmpty, "Column input specs already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
      }
      this
    }

    /**
     * Configure the KijiSource to read values from the given columns into the corresponding fields.
     *
     * @param columnSpecs mapping from column inputs to fields which will hold the values from those
     *     columns.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: Map[_ <: ColumnInputSpec, Symbol]): Builder = {
      require(columnSpecs != null, "Column input specs may not be null.")
      require(columnSpecs.size == columnSpecs.values.toSet.size,
        "Column input specs may not include duplicate Fields. found: " + columnSpecs)
      monitor.synchronized {
        mColumnSpecs match {
          case Some(cs) => {
            val symbols: List[Symbol] = columnSpecs.values.toList
            val duplicateField: Boolean = cs.toIterable.exists { entry: (ColumnInputSpec, Symbol) =>
              val (_, field) = entry
              symbols.contains(field)
            }
            require(!duplicateField, ("Column input specs already set to: %s May not add duplicate "
                + "Fields.").format(mColumnSpecs.get))
            mColumnSpecs = Some(cs ++ columnSpecs)
          }
          case None => mColumnSpecs = Some(columnSpecs)
        }
      }
      this
    }

    /**
     * Configure the KijiSource to traverse rows within the requested row range specification.
     *
     * @param rowRangeSpec requested range for rows.
     * @return this builder.
     */
    def withRowRangeSpec(rowRangeSpec: RowRangeSpec): Builder = monitor.synchronized {
      require(rowRangeSpec != null, "Row range spec may not be null.")
      require(mRowRangeSpec.isEmpty, "Row range spec already set to: " + mRowRangeSpec.get)
      mRowRangeSpec = Some(rowRangeSpec)
      this
    }

    /**
     * Configure the KijiSource to traverse rows with the requested row filter specification.
     *
     * @param rowFilterSpec requested row filter.
     * @return this builder.
     */
    def withRowFilterSpec(rowFilterSpec: RowFilterSpec): Builder = monitor.synchronized {
      require(rowFilterSpec != null, "Row filter spec may not be null.")
      require(mRowFilterSpec.isEmpty, "Row filter spec already set to: " + mRowFilterSpec.get)
      mRowFilterSpec = Some(rowFilterSpec)
      this
    }

    /**
     * Build a new KijiSource configured for input from the values stored in this Builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new KijiSource configured for input from the values stored in this Builder.
     */
    def build: KijiSource = monitor.synchronized {
      KijiInput(
          tableURI.getOrElse(throw new IllegalStateException("Table URI must be specified.")),
          timeRange.getOrElse(DEFAULT_TIME_RANGE),
          columnSpecs.getOrElse(
              throw new IllegalStateException("Column input specs must be specified.")),
          rowRangeSpec.getOrElse(RowRangeSpec.All),
          rowFilterSpec.getOrElse(RowFilterSpec.NoFilter))
    }
  }

  /**
   * Companion object providing utility methods and factory methods for creating new instances of
   * [[org.kiji.express.flow.KijiInput.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {

    /**
     * Create a new empty Builder.
     *
     * @return a new empty Builder.
     */
    def apply(): Builder = new Builder(None, None, None, None, None)

    /**
     * Create a new Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new Builder as a copy of the given Builder.
     */
    def apply(other: Builder): Builder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new Builder(
          other.tableURI,
          other.timeRange,
          other.columnSpecs,
          other.rowRangeSpec,
          other.rowFilterSpec)
    }

    /**
     * Converts a column -> Field mapping to a ColumnInputSpec -> Field mapping.
     *
     * @param pair column to Field binding.
     * @return ColumnInputSpec to Field binding.
     */
    private def columnToSpec(pair: (String, Symbol)): (_ <: ColumnInputSpec, Symbol) = {
      val (column, field) = pair
      val colName: KijiColumnName = new KijiColumnName(column)
      if (colName.isFullyQualified) {
        (QualifiedColumnInputSpec(colName.getFamily, colName.getQualifier), field)
      } else {
        (ColumnFamilyInputSpec(colName.getFamily), field)
      }
    }
  }

  /**
   * A factory method for creating a KijiSource.
   *
   * @param tableUri addressing a table in a Kiji instance.
   * @param timeRange that cells must fall into to be retrieved.
   * @param columns are a series of pairs mapping column input specs to tuple field names.
   *     Columns are specified as "family:qualifier" or, in the case of a column family input spec,
   *     simply "family".
   * @param rowRangeSpec the specification for which row interval to scan
   * @param rowFilterSpec the specification for which filter to apply.
   * @return a source for data in the Kiji table, whose row tuples will contain fields with cell
   *     data from the requested columns and map-type column families.
   */
  private[express] def apply(
      tableUri: String,
      timeRange: TimeRangeSpec,
      columns: Map[_ <: ColumnInputSpec, Symbol],
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec
  ): KijiSource = {
    new KijiSource(
      tableUri,
      timeRange,
      None,
      inputColumns = columns.map { entry: (ColumnInputSpec, Symbol) => entry.swap },
      rowRangeSpec = rowRangeSpec,
      rowFilterSpec = rowFilterSpec
    )
  }
}
