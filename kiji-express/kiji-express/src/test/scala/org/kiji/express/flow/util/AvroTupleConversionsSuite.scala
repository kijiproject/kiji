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

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import org.apache.avro.generic.GenericRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.avro.ArrayRecord
import org.kiji.express.avro.MapRecord
import org.kiji.express.avro.NameFormats
import org.kiji.express.avro.NestedArrayRecord
import org.kiji.express.avro.NestedMapRecord
import org.kiji.express.avro.SimpleRecord

@RunWith(classOf[JUnitRunner])
class AvroTupleConversionsSuite extends KijiSuite {

  test("Specific record packing works with any avro field name style.") {
    val inputs = Map(
        "snake_case_ugh" -> 13,
        "CamelCaseEvenWorse" -> 14,
        "camelPupCase" -> 15,
        "SCREAMING_SNAKE_CASE_YAH" -> 16)
    val fields = new Fields(inputs.keys.toSeq:_*)

    val converter = AvroSpecificTupleConverter[NameFormats](fields,
        implicitly[Manifest[NameFormats]])

    val tuple = new Tuple()
    inputs.values.foreach(tuple.addInteger)

    val record: NameFormats = converter(new TupleEntry(fields, tuple))

    inputs.foreach { case (k, v) => assert(v === record.get(k)) }
  }

  test("Generic record packing works with any avro field name style.") {
    val inputs = Map(
      "snake_case_ugh" -> 13,
      "CamelCaseEvenWorse" -> 14,
      "camelPupCase" -> 15,
      "SCREAMING_SNAKE_CASE_YAH" -> 16)
    val fields = new Fields(inputs.keys.toSeq:_*)

    val converter = new AvroGenericTupleConverter(fields, NameFormats.getClassSchema)

    val tuple = new Tuple()
    inputs.values.foreach(tuple.addInteger)

    val record: GenericRecord = converter(new TupleEntry(fields, tuple))

    inputs.foreach { case (k, v) => assert(v === record.get(k)) }
  }

  test("Specific record packing works with any scalding field name style.") {
    val inputs = Map(
      "snake_case_ugh" -> 13,
      "CamelCaseEvenWorse" -> 14,
      "camelPupCase" -> 15,
      "SCREAMING_SNAKE_CASE_YAH" -> 16)
    val alternateNames =
      List("snakeCaseUgh", "Camel_Case_Even_Worse", "camel_pup_case", "SCREAMING_sNAKE_cASE_yAH")
    val fields = new Fields(alternateNames.toSeq:_*)

    val converter = AvroSpecificTupleConverter[NameFormats](fields,
      implicitly[Manifest[NameFormats]])

    val tuple = new Tuple()
    inputs.values.foreach(tuple.addInteger)

    val record: NameFormats = converter(new TupleEntry(fields, tuple))

    inputs.foreach { case (k, v) => assert(v === record.get(k)) }
  }

  test("Generic record packing works with any scalding field name style.") {
    val inputs = Map(
      "snake_case_ugh" -> 13,
      "CamelCaseEvenWorse" -> 14,
      "camelPupCase" -> 15,
      "SCREAMING_SNAKE_CASE_YAH" -> 16)
    val alternateNames =
      List("snakeCaseUgh", "Camel_Case_Even_Worse", "camel_pup_case", "SCREAMING_sNAKE_cASE_yAH")
    val fields = new Fields(alternateNames.toSeq:_*)

    val converter = new AvroGenericTupleConverter(fields, NameFormats.getClassSchema)

    val tuple = new Tuple()
    inputs.values.foreach(tuple.addInteger)

    val record: GenericRecord = converter(new TupleEntry(fields, tuple))

    inputs.foreach { case (k, v) => assert(v === record.get(k)) }
  }

  val arrayInput = 1 to 5
  val arrayFields = new Fields("int_array")
  val specArrayConverter =
    AvroSpecificTupleConverter[ArrayRecord](arrayFields, implicitly[Manifest[ArrayRecord]])
  val genArrayConverter =
    new AvroGenericTupleConverter(arrayFields, ArrayRecord.getClassSchema)
  val javaArrayTuple = new Tuple(arrayInput.asJava)
  val scalaArrayTuple = new Tuple(arrayInput)

  test("Specific tuple converter can pack array field with a Java List") {
    val record: ArrayRecord = specArrayConverter(new TupleEntry(arrayFields, javaArrayTuple))
    assert(arrayInput.asJava === record.getIntArray)
  }

  test("Specific tuple converter can pack array field with a Scala Seq") {
    val record: ArrayRecord = specArrayConverter(new TupleEntry(arrayFields, scalaArrayTuple))
    assert(arrayInput.asJava === record.getIntArray)
  }

  test("Generic tuple converter can pack array field with a Java List") {
    val record: GenericRecord = genArrayConverter(new TupleEntry(arrayFields, javaArrayTuple))
    assert(arrayInput.asJava === record.get("int_array"))
  }

  test("Generic tuple converter can pack array fields with a Scala Seq") {
    val record: GenericRecord = genArrayConverter(new TupleEntry(arrayFields, scalaArrayTuple))
    assert(arrayInput.asJava === record.get("int_array"))
  }

  val mapInput = Map("foo" -> 1, "bar" -> 2, "baz" -> 3)
  val mapFields = new Fields("int_map")
  val specMapConverter =
    AvroSpecificTupleConverter[MapRecord](mapFields, implicitly[Manifest[MapRecord]])
  val genMapConverter =
    new AvroGenericTupleConverter(mapFields, MapRecord.getClassSchema)
  val javaMapTuple = new Tuple(mapInput.asJava)
  val scalaMapTuple = new Tuple(mapInput)

  test("Specific tuple converter can pack a map field with a Java Map") {
    val record: MapRecord = specMapConverter(new TupleEntry(mapFields, javaMapTuple))
    assert(mapInput.asJava === record.getIntMap)
  }

  test("Specific tuple converter can pack a map field with a Scala Map") {
    val record: MapRecord = specMapConverter(new TupleEntry(mapFields, scalaMapTuple))
    assert(mapInput.asJava === record.getIntMap)
  }

  test("Generic tuple converter can pack a map field with a Java Map") {
    val record: GenericRecord = genMapConverter(new TupleEntry(mapFields, javaMapTuple))
    assert(mapInput.asJava === record.get("int_map"))
  }

  test("Generic tuple converter can pack a map fields with a Scala Map") {
    val record: GenericRecord = genMapConverter(new TupleEntry(mapFields, scalaMapTuple))
    assert(mapInput.asJava === record.get("int_map"))
  }

  val nestedArrayInput = (1 to 5).sliding(2).toList
  val nestedArrayInputJava = nestedArrayInput.map(_.asJava).asJava
  val nestedArrayFields = new Fields("nested_int_array")
  val specNestedArrayConverter =
    AvroSpecificTupleConverter[NestedArrayRecord](nestedArrayFields,
        implicitly[Manifest[NestedArrayRecord]])
  val genNestedArrayConverter =
    new AvroGenericTupleConverter(nestedArrayFields, NestedArrayRecord.getClassSchema)
  val javaNestedArrayTuple = new Tuple(nestedArrayInputJava)
  val scalaNestedArrayTuple = new Tuple(nestedArrayInput)

  test("Specific tuple converter can pack a nested array field with nested Java Lists") {
    val record: NestedArrayRecord =
      specNestedArrayConverter(new TupleEntry(nestedArrayFields, javaNestedArrayTuple))
    assert(nestedArrayInputJava === record.getNestedIntArray)
  }

  test("Specific tuple converter can pack a nested array field with nested Scala Seqs") {
    val record: NestedArrayRecord =
      specNestedArrayConverter(new TupleEntry(nestedArrayFields, scalaNestedArrayTuple))
    assert(nestedArrayInputJava === record.getNestedIntArray)
  }

  test("Generic tuple converter can pack a nested array field with nested Java Lists") {
    val record: GenericRecord =
      genNestedArrayConverter(new TupleEntry(nestedArrayFields, javaNestedArrayTuple))
    assert(nestedArrayInputJava === record.get("nested_int_array"))
  }

  test("Generic tuple converter can pack a nested array field with nested Scala Seqs") {
    val record: GenericRecord =
      genNestedArrayConverter(new TupleEntry(nestedArrayFields, scalaNestedArrayTuple))
    assert(nestedArrayInputJava === record.get("nested_int_array"))
  }

  val nestedMapInput = Map("foo" -> Map("a" -> 1, "b" -> 2), "bar" -> Map("c" -> 3, "d" -> 4))
  val nestedMapInputJava = nestedMapInput.mapValues(_.asJava).asJava
  val nestedMapFields = new Fields("nested_int_map")
  val specNestedMapConverter =
    AvroSpecificTupleConverter[NestedMapRecord](nestedMapFields,
      implicitly[Manifest[NestedMapRecord]])
  val genNestedMapConverter =
    new AvroGenericTupleConverter(nestedMapFields, NestedMapRecord.getClassSchema)
  val javaNestedMapTuple = new Tuple(nestedMapInputJava)
  val scalaNestedMapTuple = new Tuple(nestedMapInput)

  test("Specific tuple converter can pack a nested array field with nested Java Maps") {
    val record: NestedMapRecord =
      specNestedMapConverter(new TupleEntry(nestedMapFields, javaNestedMapTuple))
    assert(nestedMapInputJava === record.getNestedIntMap)
  }

  test("Specific tuple converter can pack a nested array field with nested Scala Maps") {
    val record: NestedMapRecord =
      specNestedMapConverter(new TupleEntry(nestedMapFields, scalaNestedMapTuple))
    assert(nestedMapInputJava === record.getNestedIntMap)
  }

  test("Generic tuple converter can pack a nested array field with nested Java Maps") {
    val record: GenericRecord =
      genNestedMapConverter(new TupleEntry(nestedMapFields, javaNestedMapTuple))
    assert(nestedMapInputJava === record.get("nested_int_map"))
  }

  test("Generic tuple converter can pack a nested array field with nested Scala Maps") {
    val record: GenericRecord =
      genNestedMapConverter(new TupleEntry(nestedMapFields, scalaNestedMapTuple))
    assert(nestedMapInputJava === record.get("nested_int_map"))
  }

  test("Specific tuple converter validates that fields exist in the specific record") {
    val fields = new Fields(List("non_existant"):_*)

    intercept[IllegalArgumentException] {
      AvroSpecificTupleConverter[SimpleRecord](fields, implicitly[Manifest[SimpleRecord]])
    }
  }

  test("Generic tuple converter validates that fields exist in the generic record") {
    val fields = new Fields(List("non_existant"):_*)

    intercept[IllegalArgumentException] {
      new AvroGenericTupleConverter(fields, SimpleRecord.getClassSchema)
    }
  }
}
