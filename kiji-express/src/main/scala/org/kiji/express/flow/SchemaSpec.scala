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

/**
 * A specification of how to read or write values to a Kiji column.
 */
sealed trait SchemaSpec extends java.io.Serializable {
  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] object associated with this SchemaSpec,
   * if possible.
   */
  private[express] def schema: Option[Schema]
}

/**
 * Module to provide SchemaSpec implementations.
 */
object SchemaSpec {
  /**
   * Specifies reading or writing with the supplied [[org.apache.avro.Schema]].
   *
   * @param genericSchema of data
   */
  case class Generic(genericSchema: Schema) extends SchemaSpec {
    override val schema = Some(genericSchema)
  }

  /**
   * A specification for reading or writing as an instance of the supplied Avro specific record.
   *
   * @param klass of the specific record.
   */
  case class Specific(klass: Class[_ <: SpecificRecord]) extends SchemaSpec {
    override val schema = Some(klass.newInstance.getSchema)
  }

  /**
   * Use the writer schema associated with a value to read or write.
   *
   * In the case of reading a value, the writer schema used to serialize the value will be used.
   * In the case of writing a value, the schema attached to or inferred from the value will be used.
   */
  case object Writer extends SchemaSpec {
    override val schema = None
  }

  /**
   * Use the default reader schema of the column to read or write the values to the column.
   */
  case object DefaultReader extends SchemaSpec {
    override val schema = None
  }
}

