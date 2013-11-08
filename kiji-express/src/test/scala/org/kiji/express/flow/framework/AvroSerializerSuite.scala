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

package org.kiji.express.flow.framework

import scala.collection.JavaConverters.seqAsJavaListConverter

import cascading.kryo.KryoFactory
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.hbase.HBaseConfiguration
import org.scalatest.FunSuite

import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.framework.serialization.AvroSpecificSerializer
import org.kiji.express.flow.framework.serialization.AvroGenericSerializer
import org.kiji.express.flow.framework.serialization.AvroSchemaSerializer

class AvroSerializerSuite
    extends FunSuite {
  def serDeTest[I](inputName: String, serdeName: String, input: => I)(operation: I => I) {
    test("Serialization/Deserialization of a %s using %s".format(inputName, serdeName)) {
      val expected: I = input
      val actual: I = operation(expected)

      assert(expected === actual)
    }
  }

  val recordSchema: Schema = {
    val fields = Seq(
        new Schema.Field("field1", Schema.create(Schema.Type.INT), "First test field.", null),
        new Schema.Field("field2", Schema.create(Schema.Type.STRING), "First test field.", null),
        new Schema.Field("field3", Schema.create(Schema.Type.FLOAT), "First test field.", null))

    val record = Schema.createRecord("TestRecord", "", "", false)
    record.setFields(fields.asJava)

    record
  }

  val genericRecord: GenericContainer = {
    new GenericRecordBuilder(recordSchema)
        .set("field1", 42)
        .set("field2", "foo")
        .set("field3", 3.14f)
        .build()
  }

  val specificRecord: SpecificRecord = {
    SimpleRecord
        .newBuilder()
        .setL(42L)
        .setO("foo")
        .setS("bar")
        .build()
  }

  serDeTest("Schema", "Avro", recordSchema) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.
    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[Schema], classOf[AvroSchemaSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize the schema.
    val output = new Output(1024)
    kryo.writeObject(output, actual)

    // Deserialize the schema.
    val input = new Input(output.getBuffer)
    kryo.readObject(input, classOf[Schema])
  }

  serDeTest("GenericRecord", "Avro", genericRecord) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.
    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[GenericContainer], classOf[AvroGenericSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize the schema.
    val output = new Output(1024)
    kryo.writeObject(output, actual)

    // Deserialize the schema.
    val input = new Input(output.getBuffer)
    kryo.readObject(input, classOf[GenericContainer])
  }

  serDeTest("SpecificRecord", "Avro", specificRecord) { actual =>
    // Use cascading.kryo to mimic scalding's actual behavior.
    val kryo = new Kryo()
    val kryoFactory = new KryoFactory(HBaseConfiguration.create())
    val registrations = Seq(
        new KryoFactory.ClassPair(classOf[SpecificRecord], classOf[AvroSpecificSerializer]))
    kryoFactory.setHierarchyRegistrations(registrations.asJava)
    kryoFactory.populateKryo(kryo)

    // Serialize the schema.
    val output = new Output(1024)
    kryo.writeObject(output, actual)

    // Deserialize the schema.
    val input = new Input(output.getBuffer)
    kryo.readObject(input, classOf[SimpleRecord])
  }
}
