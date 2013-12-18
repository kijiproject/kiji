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

package org.kiji.express.flow.util

import scala.util.Random
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder

import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.EntityId
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/** Utils for testing against the avro-types-complete layout. */
object AvroTypesComplete {

  val layout = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout("layout/avro-types-complete.json"))

  val name = layout.getName

  val family = "strict"

  /** Column names. */
  val counterColumn = "counter"
  val rawColumn = "raw"
  val nullColumn = "null"
  val booleanColumn = "boolean"
  val intColumn = "int"
  val longColumn = "long"
  val floatColumn = "float"
  val doubleColumn = "double"
  val bytesColumn = "bytes"
  val stringColumn = "string"
  val specificColumn = "specific"
  val genericColumn = "generic"
  val enumColumn = "enum"
  val arrayColumn = "array"
  val mapColumn = "map"
  val unionColumn = "union"
  val fixedColumn = "fixed"

  private val schemaParser = new Schema.Parser()

  /** Column schemas. */
  val nullSchema = Schema.create(Schema.Type.NULL)
  val booleanSchema = Schema.create(Schema.Type.BOOLEAN)
  val intSchema = Schema.create(Schema.Type.INT)
  val longSchema = Schema.create(Schema.Type.LONG)
  val floatSchema = Schema.create(Schema.Type.FLOAT)
  val doubleSchema = Schema.create(Schema.Type.DOUBLE)
  val bytesSchema = Schema.create(Schema.Type.BYTES)
  val stringSchema = schemaParser.parse(
      "{ \"type\": \"string\", \"avro.java.string\": \"String\" }")
  val specificSchema = SimpleRecord.getClassSchema
  val genericSchema = schemaParser.parse(
      "{\"type\": \"record\", \"name\": \"Vector\", \"fields\": [" +
          "{\"name\": \"length\", \"type\": \"int\"}," +
          " {\"name\": \"angle\", \"type\": \"float\"}]}")
  val enumSchema = schemaParser.parse(
      "{\"type\": \"enum\", \"name\": \"Direction\", \"symbols\":" +
          " [\"NORTH\", \"EAST\", \"SOUTH\", \"WEST\"]}")
  val arraySchema = schemaParser.parse(
    "{\"type\": \"array\", \"items\": { \"type\": \"string\", \"avro.java.string\": \"String\" }}")
  val mapSchema = schemaParser.parse("{\"type\": \"map\", \"values\": \"int\"}")
  val unionSchema = schemaParser.parse("[\"string\", \"int\"]")
  val fixedSchema = schemaParser.parse("{\"type\": \"fixed\", \"size\": 10, \"name\": \"hash\"}")

  /** Record builders. */
  val specificBuilder = SimpleRecord.newBuilder()
  val genericBuilder = new GenericRecordBuilder(genericSchema)
  val genericData = new GenericData()


  /** Value generators. */
  val rand = new Random
  val base: Iterable[_] = Range(0, 10) // Determines the number of inputs per test
  val nulls = base.map { _ => null }
  def booleans: Iterable[Boolean] = base.map { _ => rand.nextBoolean() }
  def ints: Iterable[Int] = base.map { _ => rand.nextInt() }
  def longs: Iterable[Long] = base.map { _ => rand.nextLong() }
  def floats: Iterable[Float] = base.map { _ => rand.nextFloat() }
  def doubles: Iterable[Double] = base.map { _ => rand.nextDouble() }
  def bytes: Iterable[Array[Byte]] = base.map { _ =>
    val ary = Array.ofDim[Byte](32)
    rand.nextBytes(ary)
    ary
  }
  def strings: Iterable[String] = base.map { _ => rand.nextString(32) }
  def specificRecords: Iterable[SimpleRecord] = longs.zip(strings)
      .map { fields => specificBuilder.setL(fields._1).setS(fields._2).build() }
  def genericRecords: Iterable[GenericRecord] = ints.zip(floats)
      .map { fields => genericBuilder.set("length", fields._1).set("angle", fields._2).build() }
  def enumValues: Vector[String] = Vector("NORTH", "EAST", "SOUTH", "WEST")
  def enums: Iterable[GenericEnumSymbol] = base.map { _ =>
    genericData.createEnum(enumValues(rand.nextInt(4)), enumSchema).asInstanceOf[GenericEnumSymbol]}
  def enumStrings: Iterable[String] = base.map { _ => enumValues(rand.nextInt(4))}
  def arrays: Iterable[Iterable[String]] = base.map { _ => strings }
  def avroArrays: Iterable[GenericArray[String]] = arrays.map { strings =>
    new GenericData.Array(arraySchema, strings.toSeq.asJava)
  }
  def maps: Iterable[Map[String, Int]] = base.map { _ => strings.zip(ints).toMap }
  def unions: Iterable[Any] = booleans.zip(strings.zip(ints)).map { t =>
    val (bool, (string, int)) = t
    if (bool) string else int
  }
  def fixedByteArrays: Iterable[Array[Byte]] = base.map { _ =>
    val ary = Array.ofDim[Byte](10)
    rand.nextBytes(ary)
    ary
  }
  def fixeds: Iterable[GenericFixed] = fixedByteArrays.map { bs => new Fixed(fixedSchema, bs) }
  def eids: Iterable[EntityId] = strings.map(EntityId(_))
}
