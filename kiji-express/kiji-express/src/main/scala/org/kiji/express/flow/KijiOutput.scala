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
import org.kiji.schema.InternalKijiError

/**
 * Factory methods for constructing [[org.kiji.express.flow.KijiSource]]s that will be used as
 * outputs of a KijiExpress flow.
 *
 * Example usage:
 *
 * {{{
 *   KijiOutput.builder
 *       .withTableURI("kiji://localhost:2181/default/mytable")
 *       .withTimestampField('timestamp)
 *       .withColumns('column1 -> "info:column1", 'column2 -> "info:column2")
 *       .addColumnSpecs('column3 -> QualifiedColumnOutputSpec.builder
 *           .withColumn("info", "column3")
 *           .build)
 *       .build
 * }}}
 */
@ApiAudience.Public
@ApiStability.Stable
object KijiOutput {

  val DEFAULT_COLUMN_OUTPUT_SPECS: Map[Symbol, _ <: ColumnOutputSpec] = Map()

  /**
   * A factory method for instantiating [[org.kiji.express.flow.KijiSource]]s used as sinks. This
   * method permits specifying the full range of read options for each column. If timestampField is
   * undefined, the current time will be used at the time of the write.
   *
   * @param tableUri that addresses a table in a Kiji instance.
   * @param timestampField is the name of a tuple field that will contain cell timestamps when the
   *     source is used for writing.
   * @param columns is a mapping specifying what column to write each field value to.
   * @return a source that can write tuple field values to columns of a Kiji table.
   */
  private[express] def apply(
      tableUri: String,
      timestampField: Option[Symbol],
      columns: Map[Symbol, _ <: ColumnOutputSpec]
  ): KijiSource = {
    new KijiSource(
        tableAddress = tableUri,
        timeRange = TimeRangeSpec.All,
        timestampField = timestampField,
        outputColumns = columns)
  }

  /**
   * Create a new empty KijiOutput.Builder.
   *
   * @return a new empty KijiOutput.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new KijiOutput.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new KijiOutput.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for [[org.kiji.express.flow.KijiSource]]s to be used as outputs.
   *
   * @param constructorTableURI string of the table to which to write.
   * @param constructorTimestampField flow Field from which to read the timestamp.
   * @param constructorColumnSpecs mapping from Field to output specification.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      val constructorTableURI: Option[String],
      val constructorTimestampField: Option[Symbol],
      val constructorColumnSpecs: Option[Map[Symbol, _ <: ColumnOutputSpec]]
  ) {
    private[this] val monitor = new AnyRef

    private var mTableURI: Option[String] = constructorTableURI
    private var mTimestampField: Option[Symbol] = constructorTimestampField
    private var mColumnSpecs: Option[Map[Symbol, _ <: ColumnOutputSpec]] = constructorColumnSpecs

    /**
     * Configure the KijiSource to write to the table with the given URI.
     *
     * @param tableURI string of the table to which to write.
     * @return this builder.
     */
    def withTableURI(tableURI: String): Builder = monitor.synchronized {
      require(None == mTableURI, "Table URI already set to: " + mTableURI.get)
      mTableURI = Some(tableURI)
      this
    }

    /**
     * Get the output table URI from this builder.
     *
     * @return the output table URI from this builder.
     */
    def tableURI: Option[String] = mTableURI

    /**
     * Configure the KijiSource to write with the timestamp found in the given Field.
     *
     * @param timestampField whose value will be used as a timestamp when writing.
     * @return this builder.
     */
    def withTimestampField(timestampField: Symbol): Builder = monitor.synchronized {
      require(None == mTimestampField, "Timestamp Field already set to: " + mTimestampField)
      mTimestampField = Some(timestampField)
      this
    }

    /**
     * Get the Field whose value will be used as a timestamp when writing.
     *
     * @return the Field whose value will be used as a timestamp when writing.
     */
    def timestampField: Option[Symbol] = mTimestampField

    /**
     * Configure the KijiSource to write values of the given Fields to the corresponding columns.
     *
     * @param columns mapping from Field to Kiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def withColumns(columns: Map[Symbol, String]): Builder = monitor.synchronized {
      require(columns.size == columns.values.toSet.size,
          "Column output specifications may not contain duplicate columns, found: " + columns)
      require(None == mColumnSpecs,
          "Column output specifications already set to: " + mColumnSpecs)
      mColumnSpecs = Some(columns.mapValues { QualifiedColumnOutputSpec.fromColumnName })
      this
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Kiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def withColumns(columns: (Symbol, String)*): Builder = {
      withColumns(columns.toMap)
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Kiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def addColumns(columns: Map[Symbol, String]): Builder = monitor.synchronized {
      require(columns.size == columns.values.toSet.size,
        "Column output specifications may not contain duplicate columns, found: " + columns)
      mColumnSpecs match {
        case Some(cs) => {
          val colsList: List[String] = columns.values.toList
          val duplicateFieldOrColumn: Boolean = cs.exists {
            case (field, column) => columns.contains(field) || colsList.contains(column)
          }
          require(!duplicateFieldOrColumn, ("Column output specifications already set to: %s May "
              + "not add duplicate Fields or columns.").format(mColumnSpecs.get))
          mColumnSpecs = Some(cs ++ columns.mapValues {
            QualifiedColumnOutputSpec.fromColumnName
          })
        }
        case None => {
          mColumnSpecs = Some(columns.mapValues { QualifiedColumnOutputSpec.fromColumnName })
        }
      }
      this
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columns mapping from Field to Kiji column where the value of the field will be
     *     written.
     * @return this builder.
     */
    def addColumns(columns: (Symbol, String)*): Builder = {
      addColumns(columns.toMap)
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: Map[Symbol, _ <: ColumnOutputSpec]): Builder = {
      monitor.synchronized {
        val (qualified, families) = columnSpecs.partition {
          case (_, spec) => spec match {
            case qcos: QualifiedColumnOutputSpec => true
            case cfos: ColumnFamilyOutputSpec => false
            case unknown => throw new InternalKijiError("Unknown ColumnOutputSpec type: " + unknown)
          }
        }
        require(qualified.size == qualified.values.map { _.columnName }.toSet.size,
            "Column output specifications may not contain duplicate columns, found: " + columnSpecs)
        require(families.size == families.values.map {
              case ColumnFamilyOutputSpec(family, qualifierSelector, _) =>
                (family, qualifierSelector)
            }.toSet.size,
            "Column output specifications may not contain duplicate columns. Column family output "
            + "specifications are considered duplicate if the family and qualifier selector both "
            + "match, found: " + columnSpecs)
        require(None == mColumnSpecs,
          "Column output specifications already set to: " + mColumnSpecs.get)
        mColumnSpecs = Some(columnSpecs)
        this
      }
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def withColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder = {
      withColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: Map[Symbol, _ <: ColumnOutputSpec]): Builder = {
      monitor.synchronized {
        val (qualified, families) = columnSpecs.partition {
          case (_, spec) => spec match {
            case qcos: QualifiedColumnOutputSpec => true
            case cfos: ColumnFamilyOutputSpec => false
            case unknown => throw new InternalKijiError("Unknown ColumnOutputSpec type: " + unknown)
          }
        }
        require(qualified.size == qualified.values.map { _.columnName }.toSet.size,
          "Column output specifications may not contain duplicate columns, found: " + columnSpecs)
        require(families.size == families.values.map {
          case ColumnFamilyOutputSpec(family, qualifierSelector, _) =>
            (family, qualifierSelector)
        }.toSet.size,
          "Column output specifications may not contain duplicate columns. Column family output "
              + "specifications are considered duplicate if the family and qualifier selector both "
              + "match, found: " + columnSpecs)
        mColumnSpecs match {
          case Some(cs) => {
            val colsList: List[KijiColumnName] = columnSpecs.values.toList.map { _.columnName }
            val duplicateFieldOrColumn = cs.exists { case (field, spec) =>
                columnSpecs.contains(field) || colsList.contains(spec.columnName)
            }
            require(!duplicateFieldOrColumn, ("Column output specifications already set to: %s May "
                + "not add duplicate Fields or columns.").format(mColumnSpecs.get))
            mColumnSpecs = Some(cs ++ columnSpecs)
          }
          case None => mColumnSpecs = Some(columnSpecs)
        }
      }
      this
    }

    /**
     * Configure the KijiSource to write values of the given fields to the corresponding columns.
     *
     * @param columnSpecs mapping from Field to output specifications for the value stored in that
     *     Field.
     * @return this builder.
     */
    def addColumnSpecs(columnSpecs: (Symbol, _ <: ColumnOutputSpec)*): Builder = {
      addColumnSpecs(columnSpecs.toMap[Symbol, ColumnOutputSpec])
    }

    /**
     * Get the output specifications from this Builder.
     *
     * @return the output specifications from this Builder.
     */
    def columnSpecs: Option[Map[Symbol, _ <: ColumnOutputSpec]] = mColumnSpecs

    /**
     * Build a new KijiSource configured for output from the values stored in this Builder.
     *
     * @return a new KijiSource configured for output from the values stored in this Builder.
     */
    def build: KijiSource = monitor.synchronized {
      KijiOutput(
        tableURI.getOrElse(throw new IllegalArgumentException("Table URI must be specified.")),
        timestampField,
        columnSpecs.getOrElse(DEFAULT_COLUMN_OUTPUT_SPECS)
      )
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.KijiOutput.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {
    /**
     * Create a new empty KijiOutput.Builder.
     *
     * @return a new empty KijiOutput.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None)

    /**
     * Create a new KijiOutputBuilder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new KijiOutputBuilder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder =
        new Builder(other.tableURI, other.timestampField, other.columnSpecs)
  }
}
