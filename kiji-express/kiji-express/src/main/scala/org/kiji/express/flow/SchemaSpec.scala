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

/**
 * A specification describing the schema to use when reading or writing values to a column in a Kiji
 * table.
 *
 * [[http://avro.apache.org/docs/current/ Avro]] provides us with the ability to read and write
 * structured data with different schemas. SchemaSpec provides options for explicitly specifying the
 * schemas that should be used when reading/writing data from/to Kiji tables. SchemaSpec also
 * provides the ability to use Avro SpecificRecords defined using
 * [[http://avro.apache.org/docs/current/idl.html Avro's IDL]].
 *
 * @note Defaults to [[org.kiji.express.flow.SchemaSpec.Writer SchemaSpec.Writer]] when reading or
 *     writing data.
 * @example SchemaSpec usage.
 *      - [[org.kiji.express.flow.SchemaSpec.DefaultReader SchemaSpec.DefaultReader]] - Use the
 *        default reader schema defined in your table layout (assuming that your table layout uses a
 *        default reader schema):
 *        {{{
 *          // Data read/written will be decoded/encoded using the default reader schema for the
 *          // table it belongs to. Expects/produces Avro GenericRecords.
 *          .withSchemaSpec(SchemaSpec.DefaultReader)
 *        }}}
 *      - [[org.kiji.express.flow.SchemaSpec.Writer SchemaSpec.Writer]] - Use the schema that the
 *        desired cell was written with if reading and attempts to infer the schema from your data
 *        if writing (will fail on raw maps/lists):
 *        {{{
 *          // Data will be read with the schema it was written with. Data will be written with an
 *          // inferred schema. Expects/produces Avro GenericRecords.
 *          .withSchemaSpec(SchemaSpec.Writer)
 *        }}}
 *      - [[org.kiji.express.flow.SchemaSpec.Generic SchemaSpec.Generic]] - Use the specified Avro
 *        schema to decode/encode data:
 *        {{{
 *          val myAvroSchema: Schema = // ...
 *
 *          // ...
 *
 *          // Data will be read/written with the provided avro schema. Expects/produces Avro
 *          // GenericRecords.
 *          .withSchemaSpec(SchemaSpec.Generic(myAvroSchema))
 *        }}}
 *      - [[org.kiji.express.flow.SchemaSpec.Specific SchemaSpec.Specific]] - Use the specified Avro
 *        specific record to decode/encode data:
 *        {{{
 *          val myAvroSpecificRecordClass: Class[MyAvroRecord] = classOf[MyAvroRecord]
 *
 *          // ...
 *
 *          // Data will be read/written as the provided avro specific record. Expects/produces Avro
 *          // SpecificRecords. This option should be used when you have a specific record that has
 *          // been compiled by Avro.
 *          .withSchemaSpec(SchemaSpec.Specific(myAvroSpecificRecordClass))
 *        }}}
 * @see [[org.kiji.express.flow.ColumnFamilyInputSpec]] for information about using SchemaSpec when
 *     reading data from a Kiji column family.
 * @see [[org.kiji.express.flow.QualifiedColumnInputSpec]] for information about using SchemaSpec
 *     when reading data from a Kiji column.
 * @see [[org.kiji.express.flow.ColumnFamilyOutputSpec]] for information about using SchemaSpec
 *     when writing data to a Kiji column family.
 * @see [[org.kiji.express.flow.QualifiedColumnOutputSpec]] for information about using SchemaSpec
 *     when writing data to a Kiji column.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait SchemaSpec extends Serializable {
  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] object associated with this SchemaSpec,
   * if possible.
   */
  private[kiji] def schema: Option[Schema]
}

/**
 * Provides [[org.kiji.express.flow.SchemaSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object SchemaSpec {
  /**
   * Specifies reading or writing with the supplied Avro schema.
   *
   * @see [[org.kiji.express.flow.SchemaSpec]] for more usage information.
   *
   * @param genericSchema of data
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Generic(genericSchema: Schema) extends SchemaSpec {
    override val schema: Option[Schema] = Some(genericSchema)
  }

  /**
   * A specification for reading or writing as an instance of the supplied Avro specific record.
   *
   * @see [[org.kiji.express.flow.SchemaSpec]] for more usage information.
   *
   * @param klass of the specific record.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Specific(klass: Class[_ <: SpecificRecord]) extends SchemaSpec {
    override val schema: Option[Schema] = Some(klass.newInstance.getSchema)
  }

  /**
   * Use the writer schema associated with a value to read or write.
   *
   * In the case of reading a value, the writer schema used to serialize the value will be used.
   * In the case of writing a value, the schema attached to or inferred from the value will be used.
   *
   * @see [[org.kiji.express.flow.SchemaSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object Writer extends SchemaSpec {
    override val schema: Option[Schema] = None
  }

  /**
   * Specifies that the default reader for this column, stored in the table layout, should be used
   * for reading or writing this data.  If you use this option, first make sure the column in your
   * Kiji table has a default reader specified.
   *
   * @see [[org.kiji.express.flow.SchemaSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object DefaultReader extends SchemaSpec {
    override val schema: Option[Schema] = None
  }
}

