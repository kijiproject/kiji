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

package org.kiji.express.util

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AvroUtilWithSchemaSuite extends FunSuite {

  test("avroToScala will convert an Avro GenericArray to a Scala equivalent") {
    val list = List("foo", "bar", "baz")
    val schema = SchemaBuilder.array.items(Schema.create(Schema.Type.STRING))
    val glist = new GenericData.Array(schema, list.asJava)
    assert(list === AvroUtil.avroToScala(glist))
  }

  test("avroToScala will convert a GenericFixed to a Array[Byte]") {
    val bytes = "foo bar, baz?".getBytes
    val schema = SchemaBuilder.fixed("name").size(bytes.length)
    val fixed = new GenericData.Fixed(schema, bytes)
    assert(bytes === AvroUtil.avroToScala(fixed))
  }

  test("avroToScala will convert a GenericEnumSymbol to the String equivalent") {
    val name = "CACTUS"
    val schema = SchemaBuilder.enumeration("name").symbols("CACTUS", "CHERRY", "PIZZA")
    val enum = new GenericData.EnumSymbol(schema, name)
    assert(name === AvroUtil.avroToScala(enum))
  }

  /** TODO: implement once Avro generic map object exists. */
  ignore("avroToScala will convert a GenericMap to the Scala equivalent.") { }

  test("avroToScala will recursively convert a GenericArray to the Scala equivalents.") {
    val names = List("CACTUS", "CHERRY", "CHERRY", "PIZZA")
    val enumSchema = SchemaBuilder.enumeration("name").symbols("CACTUS", "CHERRY", "PIZZA")
    val listSchema = SchemaBuilder.array.items(enumSchema)

    val glist = new GenericData.Array(listSchema,
        names.map(new GenericData.EnumSymbol(enumSchema, _)).asJava)

    assert(names === AvroUtil.avroToScala(glist))
  }

  /** TODO: implement once Avro generic map object exists. */
  ignore("avroToScala will recursively convert a GenericMap's values to the Scala equivalent.") { }
}
