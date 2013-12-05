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
import org.kiji.express.flow.SchemaSpec.Generic
import org.kiji.express.flow.SchemaSpec.Specific
import org.kiji.express.flow.SchemaSpec.Writer
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
@ApiStability.Experimental
@Inheritance.Sealed
trait ColumnOutputSpec {
  /**
   * Column family to write data to.
   *
   * @return family of columns to write to.
   */
  def family: String

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
 * A specification describing how to write data from a scalding tuple field to a column in a Kiji
 * table.
 *
 * Basic example that infers the schema of data being written to Kiji (this will not work when
 * writing maps, arrays):
 * {{{
 *   // Write tuple fields to the "info:name" column.
 *   val myColumnSpec: QualifiedColumnOutputSpec =
 *       QualifiedColumnOutputSpec(
 *           family = "info",
 *           qualifier = "name"
 *       )
 * }}}
 *
 * If generic avro records are being used, an avro schema that data should be written using can be
 * specified:
 * {{{
 *   // Write tuple fields to the "info:user" column using a provided avro schema.
 *   val myColumnSpec: QualifiedColumnOutputSpec =
 *       QualifiedColumnOutputSpec(
 *           family = "info",
 *           qualifier = "user",
 *           schemaSpec = SchemaSpec.Generic(myAvroSchema)
 *       )
 * }}}
 *
 * If compiled avro classes are being used, a compiled record class can be specified. Data written
 * to this column should be of the specified type:
 * {{{
 *   // Write tuple fields to the "info:user" column using a compiled avro class.
 *   val myColumnSpec: QualifiedColumnOutputSpec =
 *       QualifiedColumnOutputSpec(
 *           family = "info",
 *           qualifier = "user",
 *           schemaSpec = SchemaSpec.Specific(classOf[User])
 *       )
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
@ApiStability.Experimental
@Inheritance.Sealed
final case class QualifiedColumnOutputSpec(
    family: String,
    qualifier: String,
    schemaSpec: SchemaSpec = Writer
) extends ColumnOutputSpec {
  override def columnName: KijiColumnName = new KijiColumnName(family, qualifier)
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.QualifiedColumnOutputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object QualifiedColumnOutputSpec {
  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with a generic Avro writer schema. This constructor takes a column string which must
   * contain the column family and qualifier in the form 'family:qualifier'.
   *
   * @param family of the column to write to.
   * @param qualifier of the column to write to.
   * @param schema with which to write data.
   * @return a new column output spec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      schema: Schema
  ): QualifiedColumnOutputSpec = {
    QualifiedColumnOutputSpec(family, qualifier, Generic(schema))
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with a specific Avro record class. This constructor takes a column string which must
   * contain the column family and qualifier in the form 'family:qualifier'.
   *
   * @param family of the column to write to.
   * @param qualifier of the column to write to.
   * @param specificRecord class to write to the output column.
   * @return a new column output spec with supplied options.
   */
  def apply(
      family: String,
      qualifier: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): QualifiedColumnOutputSpec = {
    QualifiedColumnOutputSpec(family, qualifier, Specific(specificRecord))
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with schema options. This constructor takes a column string which must contain the column
   * family and qualifier in the form 'family:qualifier'.
   *
   * @param column name of the column to write to in the format 'family:qualifier'.
   * @param schemaSpec specification with which to write data.
   * @return a new column output spec with supplied options.
   */
  def apply(
      column: String,
      schemaSpec: SchemaSpec
  ): QualifiedColumnOutputSpec = {
    column.split(':').toList match {
      case family :: qualifier :: Nil => QualifiedColumnOutputSpec(family, qualifier, schemaSpec)
      case _ => throw new IllegalArgumentException(
          "Must specify column to QualifiedColumnOutputSpec in the format 'family:qualifier'.")
    }
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with a [[org.kiji.express.flow.SchemaSpec.Generic]] Avro writer schema. This constructor
   * takes a column string which must contain the column family and qualifier in the form
   * 'family:qualifier'.
   *
   * @param column name of the column to write to in the format 'family:qualifier'.
   * @param schema with which to write data.
   * @return a new column output spec with supplied options.
   */
  def apply(
      column: String,
      schema: Schema
  ): QualifiedColumnOutputSpec = {
    QualifiedColumnOutputSpec(column, Generic(schema))
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with a [[org.kiji.express.flow.SchemaSpec.Specific]] Avro record class. This constructor
   * takes a column string which must contain the column family and qualifier in the form
   * 'family:qualifier'.
   *
   * @param column name of the column to write to in the format 'family:qualifier'.
   * @param specificRecord class to write to the output column.
   * @return a new column output spec with supplied options.
   */
  def apply(
      column: String,
      specificRecord: Class[_ <: SpecificRecord]
  ): QualifiedColumnOutputSpec = {
    QualifiedColumnOutputSpec(column, Specific(specificRecord))
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a column in a Kiji
   * table with the [[org.kiji.express.flow.SchemaSpec.Writer]] schema spec. This constructor takes
   * a column string which must contain the column family and qualifier in the form
   * 'family:qualifier'.
   *
   * @param column name of the column to write to in the format 'family:qualifier'.
   * @return a new column output spec with supplied options.
   */
  def apply(
      column: String
  ): QualifiedColumnOutputSpec = {
    QualifiedColumnOutputSpec(column, Writer)
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
 *       ColumnFamilyOutputSpec(
 *           family = "matrix",
 *           qualifierSelector = 'column
 *       )
 * }}}
 *
 * If compiled avro classes are being used, the class of the data that will be written can be
 * specified:
 * {{{
 *   // Write instances of User records versioned with the current timestamp to a column in the
 *   // "users" column family.
 *   val myColumnSpec: ColumnFamilyOutputSpec =
 *       ColumnFamilyOutputSpec(
 *           family = "users",
 *           qualifierSelector = 'name,
 *           schemaSpec = SchemaSpec.Specific(classOf[User])
 *       )
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
@ApiStability.Experimental
@Inheritance.Sealed
final case class ColumnFamilyOutputSpec(
    family: String,
    qualifierSelector: Symbol,
    schemaSpec: SchemaSpec = Writer
) extends ColumnOutputSpec {
  if (family.contains(':')) {
    throw new KijiInvalidNameException(
        "family name in ColumnFamilyOutputSpec may not contain a ':'")
  }
  override def columnName: KijiColumnName = new KijiColumnName(family)
}

/**
 * Provides factory functions for creating [[org.kiji.express.flow.ColumnFamilyOutputSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
object ColumnFamilyOutputSpec {
  /**
   * A specification describing how to write data from a scalding tuple field to a family of columns
   * in a Kiji table with a [[org.kiji.express.flow.SchemaSpec.Generic]] Avro writer schema.
   *
   * @param family of the column to write to.
   * @param qualifierSelector is the tuple field used to specify the qualifier of the column to
   *     write to. If an attempt is made to write a tuple that is missing the qualifierSelector
   *     field, an error will be thrown.
   * @param schema The schema to use for writing values.
   * @return a new column output spec with supplied options.
   */
  def apply(
      family: String,
      qualifierSelector: Symbol,
      schema: Schema
  ): ColumnFamilyOutputSpec = {
    ColumnFamilyOutputSpec(family, qualifierSelector, Generic(schema))
  }

  /**
   * A specification describing how to write data from a scalding tuple field to a family of columns
   * in a Kiji table with a [[org.kiji.express.flow.SchemaSpec.Specific]] Avro record class.
   *
   * @param family of the column to write to.
   * @param qualifierSelector is the tuple field used to specify the qualifier of the column to
   *     write to. If an attempt is made to write a tuple that is missing the qualifierSelector
   *     field, an error will be thrown.
   * @param specificRecord class to write to the output column.
   * @return a new column output spec with supplied options.
   */
  def apply(
      family: String,
      qualifierSelector: Symbol,
      specificRecord: Class[_ <: SpecificRecord]
  ): ColumnFamilyOutputSpec = {
    ColumnFamilyOutputSpec(family, qualifierSelector, Specific(specificRecord))
  }
}

