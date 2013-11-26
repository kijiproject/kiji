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
 * A specification of how to read or write values to a Kiji column.
 *
 * An instance of one of the subclasses of SchemaSpec, [[org.kiji.express.flow.SchemaSpec.Generic]],
 * [[org.kiji.express.flow.SchemaSpec.Specific]],
 * [[org.kiji.express.flow.SchemaSpec.DefaultReader]], or
 * [[org.kiji.express.flow.SchemaSpec.Writer]], can be used as an optional parameter to
 * [[org.kiji.express.flow.ColumnFamilyInputSpec]],
 * [[org.kiji.express.flow.QualifiedColumnInputSpec]],
 * [[org.kiji.express.flow.ColumnFamilyOutputSpec]], and
 * [[org.kiji.express.flow.QualifiedColumnOutputSpec]].
 *
 * These classes specify the Avro schema to read the data in a column with, or the Avro schema to
 * write the data to a column with.  Here are the possible subclasses you may use in your
 * `ColumnInputSpec` or `ColumnOutputSpec`:
 * <ul>
 *   <li>`SchemaSpec.Specific(classOf[MySpecificRecordClass])`: This option should be used when you
 *   have a specific class that has been compiled by Avro.  `MySpecificRecordClass` must extend
 *   `org.apache.avro.SpecificRecord`</li>
 *   <li>`SchemaSpec.Generic(myGenericSchema)`: If you don't have the specific class you want to
 *   use to read or write on the classpath, you can construct a generic schema and use it as the
 *   reader schema.</li>
 *   <li>`SchemaSpec.Writer`: used when you want to read with the same schema that the data
 *   was written with, or a schema attached to or inferred from the value to write with.  This is
 *   the default if you don’t specify any `SchemaSpec` for reading or writing.</li>
 *   <li>`SchemaSpec.DefaultReader`: specifies that the default reader for this column, stored in
 *   the table layout, should be used for reading or writing this data.  If you use this option,
 *   first make sure the column in your Kiji table has a default reader specified.</li>
 * </ul>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait SchemaSpec extends java.io.Serializable {
  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] object associated with this SchemaSpec,
   * if possible.
   */
  private[kiji] def schema: Option[Schema]
}

/**
 * Module to provide SchemaSpec implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object SchemaSpec {
  /**
   * Specifies reading or writing with the supplied [[org.apache.avro.Schema]].
   *
   * @param genericSchema of data
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class Generic(genericSchema: Schema) extends SchemaSpec {
    override val schema: Option[Schema] = Some(genericSchema)
  }

  /**
   * A specification for reading or writing as an instance of the supplied Avro specific record.
   *
   * @param klass of the specific record.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class Specific(klass: Class[_ <: SpecificRecord]) extends SchemaSpec {
    override val schema: Option[Schema] = Some(klass.newInstance.getSchema)
  }

  /**
   * Use the writer schema associated with a value to read or write.
   *
   * In the case of reading a value, the writer schema used to serialize the value will be used.
   * In the case of writing a value, the schema attached to or inferred from the value will be used.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  case object Writer extends SchemaSpec {
    override val schema: Option[Schema] = None
  }

  /**
   * Specifies that the default reader for this column, stored in the table layout, should be used
   * for reading or writing this data.  If you use this option, first make sure the column in your
   * Kiji table has a default reader specified.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  case object DefaultReader extends SchemaSpec {
    override val schema: Option[Schema] = None
  }
}

