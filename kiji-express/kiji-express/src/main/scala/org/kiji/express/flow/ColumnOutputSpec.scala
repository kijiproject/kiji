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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.AvroUtil
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiInvalidNameException

/**
 * A specification describing how to write data from a scalding tuple field to a Kiji table.
 * Provides access to options common to all types of column output specs. There are two types of
 * column output specs:
 * <ul>
 *   <li>
 *     [[org.kiji.express.flow.QualifiedColumnOutputSpec]] - Describes how to write data from a
 *     scalding tuple to a column in a Kiji table.
 *   </li>
 *   <li>
 *     [[org.kiji.express.flow.ColumnFamilyOutputSpec]] - Describes how to write data from a
 *     scalding tuple to a column family in a Kiji table.
 *   </li>
 * </ul>
 *
 * To see more information about writing data to a Kiji table, see
 * [[org.kiji.express.flow.KijiOutput]].
 *
 * Note: Subclasses of `ColumnOutputSpec` are case classes that override its abstract methods
 * (e.g., `schemaSpec`) with `val`s.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait ColumnOutputSpec {
  /**
   * The [[org.kiji.schema.KijiColumnName]] to write data to.
   *
   * @return the name of the column or column family to write data to.
   */
  def columnName: KijiColumnName

  /**
   * Specifies the schema that should be used to write data.
   *
   * @return the schema specification that should be used for writing.
   */
  def schemaSpec: SchemaSpec

  /**
   * Make a best effort attempt to encode a provided value to a type that will be compatible with
   * the column. If no such conversion can be made, the original value will be returned.
   */
  private[express] def encode: Any => Any = {
    schemaSpec.schema.map(AvroUtil.avroEncoder).getOrElse(identity)
  }
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.ColumnOutputSpec]] instances.
 */
@ApiAudience.Public
@ApiStability.Stable
object ColumnOutputSpec {
  /** Constant for default schema spec parameter. */
  val DEFAULT_SCHEMA_SPEC = SchemaSpec.Writer
}

/**
 * A specification describing how to write data from a scalding tuple field to a column in a Kiji
 * table.
 *
 * Example usages:
 *
 * {{{
 *   // Use the default SchemaSpec which infers the Schema from the written object.
 *   QualifiedColumnOutputSpec.builder
 *       .withColumn("family", "qualifier")
 *       .build
 *
 *   // Use an explicit Schema from an Avro record class.
 *   QualifiedColumnOutputSpec.builder
 *       .withFamily("family")
 *       .withQualifier("qualifier")
 *       .withSchemaSpec(SchemaSpec.Specific(classOf[MyRecord]))
 *       .build
 *
 *   // Write to a column in a map type family using the default reader schema for that family.
 *   ColumnFamilyOutputSpec.builder
 *       .withFamily("mapFamily")
 *       .withQualifierSelector('qualifier)
 *       .withSchemaSpec(SchemaSpec.DefaultReader)
 *       .build
 * }}}
 *
 * To see more information about writing data to a Kiji table, see
 * [[org.kiji.express.flow.KijiOutput]].
 *
 * @param family of the column to write to.
 * @param qualifier of the column to write to.
 * @param schemaSpec The schema specification with which to write values. By default uses
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Stable
final class QualifiedColumnOutputSpec private(
    val family: String,
    val qualifier: String,
    val schemaSpec: SchemaSpec = ColumnOutputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnOutputSpec with Serializable {
  override def columnName: KijiColumnName = new KijiColumnName(family, qualifier)

  override def toString: String = Objects.toStringHelper(classOf[QualifiedColumnOutputSpec])
      .add("family", family)
      .add("qualifier", qualifier)
      .add("schema_spec", schemaSpec)
      .toString

  override def hashCode: Int = Objects.hashCode(family, qualifier, schemaSpec)

  override def equals(obj: Any): Boolean = obj match {
    case other: QualifiedColumnOutputSpec => {
      family == other.family &&
          qualifier == other.qualifier &&
          schemaSpec == other.schemaSpec
    }
    case _ => false
  }
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.QualifiedColumnOutputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Stable
object QualifiedColumnOutputSpec {

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with a generic Avro writer schema. This constructor takes a column string which must
   * contain the column family and qualifier in the form 'family:qualifier'.
   *
   * @param family of the column to write to.
   * @param qualifier of the column to write to.
   * @param schemaSpec specification with which to write data.
   * @return a new column output spec with the supplied options.
   */
  private[express] def apply(
      family: String,
      qualifier: String,
      schemaSpec: SchemaSpec = ColumnOutputSpec.DEFAULT_SCHEMA_SPEC
  ): QualifiedColumnOutputSpec = {
    new QualifiedColumnOutputSpec(family, qualifier, schemaSpec)
  }

  /**
   * Extract the fields of a QualifiedColumnOutputSpec for pattern matching.
   *
   * @param target object to decompose if it is a QualifiedColumnOutputSpec.
   * @return the fields of the decomposed QualifiedColumnOutputSpec or None if target is not a
   *     QualifiedColumnOutputSpec.
   *     (family, qualifier, schemaSpec)
   */
  private[express] def unapply(
      target: Any
  ): Option[(String, String, SchemaSpec)] = PartialFunction.condOpt(target) {
    case qcos: QualifiedColumnOutputSpec => (qcos.family, qcos.qualifier, qcos.schemaSpec)
  }

  /**
   * Convenience method for creating a QualifiedColumnOutputSpec from a qualified column name using
   * the default SchemaSpec.
   *
   * @param column name of the column to which to output.
   * @return a new QualifiedColumnOutputSpec for the given column with the default SchemaSpec.
   */
  private[express] def fromColumnName(column: String): QualifiedColumnOutputSpec =
      builder.withColumn(new KijiColumnName(column)).build

  /**
   * Create a new QualifiedColumnOutputSpec.Builder.
   *
   * @return a new QualifiedColumnOutputSpec.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new QualifiedColumnOutputSpec.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new QualifiedColumnOutputSpec.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for QualifiedColumnOutputSpec.
   *
   * @param mFamily optional family with which to initialize this builder.
   * @param mQualifier optional qualifier with which to initialize this builder.
   * @param mSchemaSpec optional SchemaSpec with which to initialize this builder.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mFamily: Option[String],
      private[this] var mQualifier: Option[String],
      private[this] var mSchemaSpec: Option[SchemaSpec]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Configure the output spec to write to the given Kiji column.
     *
     * @param column into which to write values.
     * @return this builder.
     */
    def withColumn(column: KijiColumnName): Builder = monitor.synchronized {
      require(column != null, "Output column may not be null.")
      require(column.isFullyQualified, "Output column must be fully qualified.")
      require(mFamily.isEmpty, "Output column family already set to: " + mFamily.get)
      require(mQualifier.isEmpty, "Output column qualifier already set to: " + mQualifier.get)
      mFamily = Some(column.getFamily)
      mQualifier = Some(column.getQualifier)
      this
    }

    /**
     * Configure the output spec to write to the given Kiji column.
     *
     * @param family of the column into which to write.
     * @param qualifier of the column into which to write.
     * @return this builder.
     */
    def withColumn(family: String, qualifier: String): Builder = monitor.synchronized {
      require(family != null, "Output column family may not be null.")
      require(qualifier != null, "Output column qualifier may not be null.")
      require(mFamily.isEmpty, "Output column family already set to: " + mFamily.get)
      require(mQualifier.isEmpty, "Output column qualifier already set to: " + mQualifier.get)
      mFamily = Some(family)
      mQualifier = Some(qualifier)
      this
    }

    /**
     * Configure the output spec to write to the given Kiji column family. Must also call
     * [[org.kiji.express.flow.QualifiedColumnOutputSpec.Builder.withQualifier()]] before calling
     * [[org.kiji.express.flow.QualifiedColumnOutputSpec.Builder.build]].
     *
     * @param family of the column into which to write.
     * @return this builder.
     */
    def withFamily(family: String): Builder = monitor.synchronized {
      require(family != null, "Output column family may not be null.")
      require(mFamily.isEmpty, "Output column family already set to: " + mFamily.get)
      mFamily = Some(family)
      this
    }

    /**
     * Configure the output spec to write tot he given Kiji column qualifier. Must also call
     * [[org.kiji.express.flow.QualifiedColumnOutputSpec.Builder.withFamily()]] before calling
     * [[org.kiji.express.flow.QualifiedColumnOutputSpec.Builder.build]].
     *
     * @param qualifier of the column into which to write.
     * @return this builder.
     */
    def withQualifier(qualifier: String): Builder = monitor.synchronized {
      require(qualifier != null, "Output column qualifier may not be null.")
      require(mQualifier.isEmpty, "Output column qualifier already set to: " + mQualifier.get)
      mQualifier = Some(qualifier)
      this
    }

    /**
     * Name of the Kiji column family into which to write.
     *
     * @return the name of the Kiji column family into which to write.
     */
    def family: Option[String] = monitor.synchronized(mFamily)

    /**
     * Name of the Kiji column qualifier into which to write.
     *
     * @return the name of the Kiji column qualifier into which to write.
     */
    def qualifier: Option[String] = monitor.synchronized(mQualifier)

    /**
     * Configure the output spec to write using the given SchemaSpec.
     *
     * @param schemaSpec defining the Schema which will be used to write this column.
     * @return this builder.
     */
    def withSchemaSpec(schemaSpec: SchemaSpec): Builder = monitor.synchronized {
      require(schemaSpec != null, "Schema spec may not be null.")
      require(mSchemaSpec.isEmpty, "Schema spec already set to: " + mSchemaSpec.get)
      mSchemaSpec = Option(schemaSpec)
      this
    }

    /**
     * Specification of the Schema to use when writing this column.
     *
     * @return a specification of the Schema to use when writing this column.
     */
    def schemaSpec: Option[SchemaSpec] = monitor.synchronized(mSchemaSpec)

    /**
     * Build a new QualifiedColumnOutputSpec from the values stored in this builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new QualifiedColumnOutputSpec from the values stored in this builder.
     */
    def build: QualifiedColumnOutputSpec = monitor.synchronized {
      QualifiedColumnOutputSpec(
          mFamily.getOrElse(
              throw new IllegalStateException("Output column family must be specified.")),
          mQualifier.getOrElse(
              throw new IllegalStateException("Output column qualifier must be specified.")),
          mSchemaSpec.getOrElse(ColumnOutputSpec.DEFAULT_SCHEMA_SPEC)
      )
    }

    override def toString: String = monitor.synchronized {
      Objects.toStringHelper(classOf[Builder])
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("schema_spec", mSchemaSpec)
        .toString
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.QualifiedColumnOutputSpec.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {

    /**
     * Create a new empty QualifiedColumnOutputSpec.Builder.
     *
     * @return a new empty QualifiedColumnOutputSpec.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None)

    /**
     * Create a new QualifiedColumnOutputSpec.Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new QualifiedcolumnOutputSpec.Builder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = {
      // explicitly synchronize to get a consistent snapshot
      other.monitor.synchronized(new Builder(other.family, other.qualifier, other.schemaSpec))
    }
  }
}

/**
 * A specification describing how to write data from a scalding tuple field to a family of columns
 * in a Kiji table.
 *
 * Basic column family example:
 * {{{
 *   // Write data versioned with the current timestamp to a column in the "matrix" column family.
 *   // The 'column field will contain the column name each tuple should be written to.
 *   val myColumnFamilySpec: ColumnFamilyOutputSpec =
 *       ColumnFamilyOutputSpec.builder
 *           .withFamily("matrix")
 *           .withQualifierSelector('column)
 *           .build
 * }}}
 *
 * If compiled avro classes are being used, the class of the data that will be written can be
 * specified:
 * {{{
 *   // Write instances of User records versioned with the current timestamp to a column in the
 *   // "users" column family.
 *   val myColumnSpec: ColumnFamilyOutputSpec =
 *       ColumnFamilyOutputSpec.builder
 *           .withFamily("users")
 *           .withQualifierSelector('name)
 *           .withSchemaSpec(SchemaSpec.Specific(classOf[User])
 *           .build
 * }}}
 *
 * To see more information about writing data to a Kiji table, see
 * [[org.kiji.express.flow.KijiOutput]].
 *
 * @param family of the column to write to.
 * @param qualifierSelector is the tuple field used to specify the qualifier of the column to write
 *     to. If an attempt is made to write a tuple that is missing the qualifierSelector field, an
 *     error will be thrown.
 * @param schemaSpec to use when writing data. By default uses
 *     [[org.kiji.express.flow.SchemaSpec.Writer]].
 */
@ApiAudience.Public
@ApiStability.Stable
final class ColumnFamilyOutputSpec private(
    val family: String,
    val qualifierSelector: Symbol,
    val schemaSpec: SchemaSpec = ColumnOutputSpec.DEFAULT_SCHEMA_SPEC
) extends ColumnOutputSpec {
  if (family.contains(':')) {
    throw new KijiInvalidNameException(
        "family name in ColumnFamilyOutputSpec may not contain a ':'")
  }
  override def columnName: KijiColumnName = new KijiColumnName(family)

  override def toString: String = Objects.toStringHelper(classOf[QualifiedColumnOutputSpec])
      .add("family", family)
      .add("qualifier_selector", qualifierSelector.name)
      .add("schema_spec", schemaSpec)
      .toString

  override def hashCode: Int = Objects.hashCode(family, qualifierSelector, schemaSpec)

  override def equals(obj: Any): Boolean = obj match {
    case other: ColumnFamilyOutputSpec => {
      family.equals(other.family) &&
          qualifierSelector.equals(other.qualifierSelector) &&
          schemaSpec.equals(other.schemaSpec)
    }
    case _ => false
  }
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.ColumnFamilyOutputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Stable
object ColumnFamilyOutputSpec {

  /**
   * A specification describing how to write data from a scalding tuple field to a family of columns
   * in a Kiji table with a [[org.kiji.express.flow.SchemaSpec.Generic]] Avro writer schema.
   *
   * @param family of the column to write to.
   * @param qualifierSelector is the tuple field used to specify the qualifier of the column to
   *     write to. If an attempt is made to write a tuple that is missing the qualifierSelector
   *     field, an error will be thrown.
   * @param schemaSpec specification of the schema to use for writing values.
   * @return a new column output spec with supplied options.
   */
  private[express] def apply(
      family: String,
      qualifierSelector: Symbol,
      schemaSpec: SchemaSpec = ColumnOutputSpec.DEFAULT_SCHEMA_SPEC
  ): ColumnFamilyOutputSpec = {
    new ColumnFamilyOutputSpec(family, qualifierSelector, schemaSpec)
  }

  /**
   * Extract the fields of a ColumnFamilyOutputSpec for pattern matching.
   *
   * @param target object to decompose if it is a ColumnFamilyOutputSpec.
   * @return the fields of the decomposed ColumnFamilyOutputSpec or None if target is not a
   *     ColumnFamilyOutputSpec.
   *     (family, qualifierSelector, schemaSpec)
   */
  private[express] def unapply(
      target: Any
  ): Option[(String, Symbol, SchemaSpec)] = PartialFunction.condOpt(target) {
    case cfos: ColumnFamilyOutputSpec => (cfos.family, cfos.qualifierSelector, cfos.schemaSpec)
  }

  /**
   * A request for data from a Kiji table column family.
   * This construct method is used by Java builders for ColumnInputSpec.
   * Scala users ought to use the Builder APIs.
   *
   * @param family name of the requested data.
   * @param qualifierSelector is the string tuple field name used to specify the
   *     qualifier of the column to write to.
   *     If an attempt is made to write a tuple that is missing the qualifierSelector
   *     field, an error will be thrown.
   * @return a new column output spec with supplied options.
   */
  private[express] def construct(
      family: String,
      qualifierSelector: String,
      schemaSpec: SchemaSpec
  ): ColumnFamilyOutputSpec = {
    // Construct ColumnFamilyOutputSpec
    ColumnFamilyOutputSpec(
        family,
        Symbol(qualifierSelector),
        Option(schemaSpec).getOrElse(ColumnOutputSpec.DEFAULT_SCHEMA_SPEC)
    )
  }

  /**
   * Create a new ColumnFamilyOutputSpec.Builder.
   *
   * @return a new ColumnFamilyOutputSpec.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new ColumnFamilyOutputSpec.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new ColumnFamilyOutputSpec.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for ColumnFamilyOutputSpec.
   *
   * @param mFamily optional family with which to initialize this builder.
   * @param mQualifierSelector optional qualifier selector with which to initialize this builder.
   * @param mSchemaSpec optional SchemaSpec with which to intialize this builder.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final class Builder private(
      private[this] var mFamily: Option[String],
      private[this] var mQualifierSelector: Option[Symbol],
      private[this] var mSchemaSpec: Option[SchemaSpec]
  ) {
    /** protects read and write access to private var fields. */
    private val monitor = new AnyRef

    /**
     * Configure the output spec to write into the given column family.
     *
     * @param family into which to write.
     * @return this builder.
     */
    def withFamily(family: String): Builder = monitor.synchronized {
      require(family != null, "Output column family may not be null.")
      require(mFamily.isEmpty, "Output column family already set to: " + mFamily.get)
      require(!family.contains(":"), "Output column family may not contain ':'.")
      mFamily = Some(family)
      this
    }

    /**
     * Column family configured in this builder.
     *
     * @return the column family configured in this builder.
     */
    def family: Option[String] = monitor.synchronized(mFamily)

    /**
     * Configure the output spec to read the column qualifier from the given Field.
     *
     * @param qualifierSelector Field name from which to read the output qualifier.
     * @return this builder.
     */
    def withQualifierSelector(qualifierSelector: Symbol): Builder = monitor.synchronized {
      require(qualifierSelector != null, "Output column qualifier selector may not be null.")
      require(mQualifierSelector.isEmpty,
        "Output column qualifier selector already set to: " + mQualifierSelector.get)
      mQualifierSelector = Some(qualifierSelector)
      this
    }

    /**
     * Field from which the output column qualifier will be read.
     *
     * @return the field from which the output column qualifier will be read.
     */
    def qualifierSelector: Option[Symbol] = monitor.synchronized(mQualifierSelector)

    /**
     * Configure the output spec to write using the given SchemaSpec.
     *
     * @param schemaSpec defining the Schema which will be used to write this column.
     * @return this builder.
     */
    def withSchemaSpec(schemaSpec: SchemaSpec): Builder = monitor.synchronized {
      require(schemaSpec != null, "Schema spec may not be null.")
      require(mSchemaSpec.isEmpty, "Schema spec already set to: " + mSchemaSpec.get)
      mSchemaSpec = Some(schemaSpec)
      this
    }

    /**
     * Specification of the Schema to use when writing this column.
     *
     * @return a specification of the Schema to use when writing this column.
     */
    def schemaSpec: Option[SchemaSpec] = monitor.synchronized(mSchemaSpec)

    /**
     * Create a new ColumnFamilyOutputSpec from the values stored in this builder.
     *
     * @throws IllegalStateException if the builder is not in a valid state to be built.
     * @return a new ColumnFamilyOutputSpec from the values stored in this builder.
     */
    def build: ColumnFamilyOutputSpec = monitor.synchronized {
      ColumnFamilyOutputSpec(
          mFamily.getOrElse(
            throw new IllegalStateException("Output column family must be specified.")),
          mQualifierSelector.getOrElse(
            throw new IllegalStateException("Output column qualifier selector must be specified.")),
          mSchemaSpec.getOrElse(ColumnOutputSpec.DEFAULT_SCHEMA_SPEC)
      )
    }

    override def toString: String = monitor.synchronized {
      Objects.toStringHelper(classOf[Builder])
        .add("family", mFamily)
        .add("qualifier_selector", mQualifierSelector)
        .add("schema_spec", mSchemaSpec)
        .toString
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.ColumnFamilyOutputSpec.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Stable
  object Builder {

    /**
     * Create a new ColumnFamilyOutputSpec.Builder.
     *
     * @return a new ColumnFamilyOutputSpec.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None)

    /**
     * Create a new ColumnFamilyOutputSpec.Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new ColumnFamilyOutputSpec.Builder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = other.monitor.synchronized {
      // synchronize to get consistent snapshot of other
      new Builder(other.family, other.qualifierSelector, other.schemaSpec)
    }
  }
}
