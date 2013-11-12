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

package org.kiji.express.flow.framework.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * Provides serialization for Avro schemas while using Kryo serialization.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
// TODO (EXP-295): Should these maybe be Framework?
class AvroSchemaSerializer
    extends Serializer[Schema] {
  setAcceptsNull(false)

  override def write(
      kryo: Kryo,
      output: Output,
      schema: Schema
  ) {
    val encodedSchema = schema.toString(false)
    output.writeString(encodedSchema)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      klazz: Class[Schema]
  ): Schema = {
    val encodedSchema = input.readString()
    new Schema.Parser().parse(encodedSchema)
  }
}

/**
 * Provides serialization for Avro generic records while using Kryo serialization. Record schemas
 * are prepended to the encoded generic record data.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
// TODO (EXP-295): Should these maybe be Framework?
class AvroGenericSerializer
    extends Serializer[GenericContainer] {
  // TODO(EXP-269): Cache encoders per schema.

  // We at least need an avro schema to perform serialization.
  setAcceptsNull(false)

  override def write(
      kryo: Kryo,
      output: Output,
      avroObject: GenericContainer
  ) {
    // Serialize the schema.
    new AvroSchemaSerializer().write(kryo, output, avroObject.getSchema)

    // Serialize the data.
    val writer = new GenericDatumWriter[GenericContainer](avroObject.getSchema)
    val encoder = EncoderFactory
        .get()
        .directBinaryEncoder(output, null)
    writer.write(avroObject, encoder)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      klazz: Class[GenericContainer]
  ): GenericContainer = {
    // Deserialize the schema.
    val schema = new AvroSchemaSerializer().read(kryo, input, null)

    // Deserialize the data.
    val reader = new GenericDatumReader[GenericContainer](schema)
    val decoder = DecoderFactory
        .get()
        .directBinaryDecoder(input, null)
    reader.read(null.asInstanceOf[GenericContainer], decoder)
  }
}

/**
 * Provides serialization for Avro specific records while using Kryo serialization. Record schemas
 * are not serialized as all clients interacting with this data are assumed to have the correct
 * specific record class on their classpath.
 */
final class AvroSpecificSerializer
    extends Serializer[SpecificRecord] {
  // TODO(EXP-269) Cache encoders per class/schema.

  setAcceptsNull(false)

  override def write(
      kryo: Kryo,
      output: Output,
      record: SpecificRecord
  ) {
    val writer =
        new SpecificDatumWriter[SpecificRecord](record.getClass.asInstanceOf[Class[SpecificRecord]])
    val encoder = EncoderFactory
        .get()
        .directBinaryEncoder(output, null)
    writer.write(record, encoder)
  }

  override def read(
      kryo: Kryo,
      input: Input,
      klazz: Class[SpecificRecord]
  ): SpecificRecord = {
    val reader = new SpecificDatumReader[SpecificRecord](klazz)
    val decoder = DecoderFactory
        .get()
        .directBinaryDecoder(input, null)
    reader.read(null.asInstanceOf[SpecificRecord], decoder)
  }
}
