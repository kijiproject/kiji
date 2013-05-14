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

package org.kiji.express

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.scalatest.FunSuite

import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType

class AvroValueSuite extends FunSuite {
  test("AvroInt throws unsupported exceptions when converting to the wrong type.") {
    val int: java.lang.Integer = new java.lang.Integer(10)
    val avroInt = AvroUtil.wrapGenericAvro(int)
    intercept[UnsupportedOperationException] { avroInt.asBoolean() }
    intercept[UnsupportedOperationException] { avroInt.asLong() }
    intercept[UnsupportedOperationException] { avroInt.asFloat() }
    intercept[UnsupportedOperationException] { avroInt.asDouble() }
    intercept[UnsupportedOperationException] { avroInt.asBytes() }
    intercept[UnsupportedOperationException] { avroInt.asString() }
    intercept[UnsupportedOperationException] { avroInt.asList() }
    intercept[UnsupportedOperationException] { avroInt.asMap() }
    intercept[UnsupportedOperationException] { avroInt.asEnumName() }
    intercept[UnsupportedOperationException] { avroInt.asRecord() }
  }

  test("Ints are correctly wrapped by AvroInt.") {
    val int: java.lang.Integer = new java.lang.Integer(10)
    val avroInt = AvroUtil.wrapGenericAvro(int)
    assert(10 === avroInt.asInt())
  }

  test("AvroBoolean throws unsupported exceptions when converting to the wrong type.") {
    val boolean: java.lang.Boolean = new java.lang.Boolean(false)
    val avroBoolean = AvroUtil.wrapGenericAvro(boolean)
    intercept[UnsupportedOperationException] { avroBoolean.asInt() }
    intercept[UnsupportedOperationException] { avroBoolean.asLong() }
    intercept[UnsupportedOperationException] { avroBoolean.asFloat() }
    intercept[UnsupportedOperationException] { avroBoolean.asDouble() }
    intercept[UnsupportedOperationException] { avroBoolean.asBytes() }
    intercept[UnsupportedOperationException] { avroBoolean.asString() }
    intercept[UnsupportedOperationException] { avroBoolean.asList() }
    intercept[UnsupportedOperationException] { avroBoolean.asMap() }
    intercept[UnsupportedOperationException] { avroBoolean.asEnumName() }
    intercept[UnsupportedOperationException] { avroBoolean.asRecord() }
  }

  test("Booleans are correctly wrapped by AvroBoolean.") {
    val boolean: java.lang.Boolean = new java.lang.Boolean(false)
    val avroBoolean = AvroUtil.wrapGenericAvro(boolean)
    assert(false === avroBoolean.asBoolean())
  }

  test("AvroLong throws unsupported exceptions when converting to the wrong type.") {
    val long: java.lang.Long= new java.lang.Long(1)
    val avroLong = AvroUtil.wrapGenericAvro(long)
    intercept[UnsupportedOperationException] { avroLong.asInt() }
    intercept[UnsupportedOperationException] { avroLong.asBoolean() }
    intercept[UnsupportedOperationException] { avroLong.asFloat() }
    intercept[UnsupportedOperationException] { avroLong.asDouble() }
    intercept[UnsupportedOperationException] { avroLong.asBytes() }
    intercept[UnsupportedOperationException] { avroLong.asString() }
    intercept[UnsupportedOperationException] { avroLong.asList() }
    intercept[UnsupportedOperationException] { avroLong.asMap() }
    intercept[UnsupportedOperationException] { avroLong.asEnumName() }
    intercept[UnsupportedOperationException] { avroLong.asRecord() }
  }

  test("Longs are correctly wrapped by AvroLong.") {
    val long: java.lang.Long= new java.lang.Long(1)
    val avroLong = AvroUtil.wrapGenericAvro(long)
    assert(1 === avroLong.asLong())
  }

  test("AvroFloat throws unsupported exceptions when converting to the wrong type.") {
    val float: java.lang.Float =  new java.lang.Float(10)
    val avroFloat = AvroUtil.wrapGenericAvro(float)
    intercept[UnsupportedOperationException] { avroFloat.asInt() }
    intercept[UnsupportedOperationException] { avroFloat.asBoolean() }
    intercept[UnsupportedOperationException] { avroFloat.asLong() }
    intercept[UnsupportedOperationException] { avroFloat.asDouble() }
    intercept[UnsupportedOperationException] { avroFloat.asBytes() }
    intercept[UnsupportedOperationException] { avroFloat.asString() }
    intercept[UnsupportedOperationException] { avroFloat.asList() }
    intercept[UnsupportedOperationException] { avroFloat.asMap() }
    intercept[UnsupportedOperationException] { avroFloat.asEnumName() }
    intercept[UnsupportedOperationException] { avroFloat.asRecord() }
  }

  test("Floats are correctly wrapped by AvroFloat.") {
    val float: java.lang.Float =  new java.lang.Float(10)
    val avroFloat = AvroUtil.wrapGenericAvro(float)
    assert(10 === avroFloat.asFloat())
  }

  test("AvroDouble throws unsupported exceptions when converting to the wrong type.") {
    val double: java.lang.Double = new java.lang.Double(1)
    val avroDouble = AvroUtil.wrapGenericAvro(double)
    intercept[UnsupportedOperationException] { avroDouble.asInt() }
    intercept[UnsupportedOperationException] { avroDouble.asBoolean() }
    intercept[UnsupportedOperationException] { avroDouble.asLong() }
    intercept[UnsupportedOperationException] { avroDouble.asFloat() }
    intercept[UnsupportedOperationException] { avroDouble.asBytes() }
    intercept[UnsupportedOperationException] { avroDouble.asString() }
    intercept[UnsupportedOperationException] { avroDouble.asList() }
    intercept[UnsupportedOperationException] { avroDouble.asMap() }
    intercept[UnsupportedOperationException] { avroDouble.asEnumName() }
    intercept[UnsupportedOperationException] { avroDouble.asRecord() }
  }

  test("Doubles are correctly wrapped by AvroDouble.") {
    val double: java.lang.Double = new java.lang.Double(1)
    val avroDouble = AvroUtil.wrapGenericAvro(double)
    assert(1 === avroDouble.asDouble())
  }

  test("AvroByteArray throws unsupported exceptions when converting to the wrong type.") {
    val bytes: java.nio.ByteBuffer =  java.nio.ByteBuffer.wrap(Array(0x11, 0x12))
    val avroByteArray = AvroUtil.wrapGenericAvro(bytes)
    intercept[UnsupportedOperationException] { avroByteArray.asInt() }
    intercept[UnsupportedOperationException] { avroByteArray.asBoolean() }
    intercept[UnsupportedOperationException] { avroByteArray.asLong() }
    intercept[UnsupportedOperationException] { avroByteArray.asFloat() }
    intercept[UnsupportedOperationException] { avroByteArray.asDouble() }
    intercept[UnsupportedOperationException] { avroByteArray.asString() }
    intercept[UnsupportedOperationException] { avroByteArray.asList() }
    intercept[UnsupportedOperationException] { avroByteArray.asMap() }
    intercept[UnsupportedOperationException] { avroByteArray.asEnumName() }
    intercept[UnsupportedOperationException] { avroByteArray.asRecord() }
  }

  test("Byte arrays are correctly wrapped by AvroByteArray.") {
    val bytes: java.nio.ByteBuffer =  java.nio.ByteBuffer.wrap(Array(0x11, 0x12))
    val avroByteArray = AvroUtil.wrapGenericAvro(bytes)
    assert(Array(0x11, 0x12) === avroByteArray.asBytes())
  }

  test("AvroString throws unsupported exceptions when converting to the wrong type.") {
    val charSequence: java.lang.CharSequence = "hello".asInstanceOf[java.lang.CharSequence]
    val avroString = AvroUtil.wrapGenericAvro(charSequence)
    intercept[UnsupportedOperationException] { avroString.asInt() }
    intercept[UnsupportedOperationException] { avroString.asBoolean() }
    intercept[UnsupportedOperationException] { avroString.asLong() }
    intercept[UnsupportedOperationException] { avroString.asFloat() }
    intercept[UnsupportedOperationException] { avroString.asDouble() }
    intercept[UnsupportedOperationException] { avroString.asBytes() }
    intercept[UnsupportedOperationException] { avroString.asList() }
    intercept[UnsupportedOperationException] { avroString.asMap() }
    intercept[UnsupportedOperationException] { avroString.asEnumName() }
    intercept[UnsupportedOperationException] { avroString.asRecord() }
  }

  test("A CharSequence is correctly wrapped by an AvroString.") {
    val charSequence: java.lang.CharSequence = "hello".asInstanceOf[java.lang.CharSequence]
    val avroString = AvroUtil.wrapGenericAvro(charSequence)
    assert("hello" === avroString.asString())
  }

  test("AvroList throws unsupported exceptions when converting to the wrong type.") {
    val avroInt1 = AvroUtil.wrapGenericAvro(1)
    val avroInt2 = AvroUtil.wrapGenericAvro(2)
    val avroList = AvroUtil.wrapGenericAvro(List(1,2).asJava)
    intercept[UnsupportedOperationException] { avroList.asInt() }
    intercept[UnsupportedOperationException] { avroList.asBoolean() }
    intercept[UnsupportedOperationException] { avroList.asLong() }
    intercept[UnsupportedOperationException] { avroList.asFloat() }
    intercept[UnsupportedOperationException] { avroList.asDouble() }
    intercept[UnsupportedOperationException] { avroList.asBytes() }
    intercept[UnsupportedOperationException] { avroList.asString() }
    intercept[UnsupportedOperationException] { avroList.asEnumName() }
    intercept[UnsupportedOperationException] { avroList.asRecord() }
  }

  test("A List is correctly wrapped by an AvroList.") {
    val avroInt1 = AvroUtil.wrapGenericAvro(1)
    val avroInt2 = AvroUtil.wrapGenericAvro(2)
    val avroList = AvroUtil.wrapGenericAvro(List(1,2).asJava)
    assert(List(avroInt1, avroInt2) === avroList.asList())
    assert(avroInt1 === avroList(0))
    assert(avroInt2 === avroList(1))
  }

  test("AvroMap throws unsupported exceptions when converting to the wrong type.") {
    val avroInt2 = AvroUtil.wrapGenericAvro(2)
    val avroInt4 = AvroUtil.wrapGenericAvro(4)
    val avroMap = AvroUtil.wrapGenericAvro(Map("a"->2,"b"->4).asJava)
    intercept[UnsupportedOperationException] { avroMap.asInt() }
    intercept[UnsupportedOperationException] { avroMap.asBoolean() }
    intercept[UnsupportedOperationException] { avroMap.asLong() }
    intercept[UnsupportedOperationException] { avroMap.asFloat() }
    intercept[UnsupportedOperationException] { avroMap.asDouble() }
    intercept[UnsupportedOperationException] { avroMap.asBytes() }
    intercept[UnsupportedOperationException] { avroMap.asString() }
    intercept[UnsupportedOperationException] { avroMap.asList() }
    intercept[UnsupportedOperationException] { avroMap.asEnumName() }
    intercept[UnsupportedOperationException] { avroMap.asRecord() }
  }

  test("A Map is correctly wrapped by an AvroMap.") {
    val avroInt2 = AvroUtil.wrapGenericAvro(2)
    val avroInt4 = AvroUtil.wrapGenericAvro(4)
    val avroMap = AvroUtil.wrapGenericAvro(Map("a"->2,"b"->4).asJava)
    assert(Map("a"->avroInt2,"b"->avroInt4) === avroMap.asMap())
    assert(avroInt2 === avroMap("a"))
    assert(avroInt4 === avroMap("b"))
  }

  test("AvroEnum throws unsupported exceptions when converting to the wrong type.") {
    // Use HashType enum for testing.
    val avroEnum = AvroUtil.wrapGenericAvro(HashType.MD5)
    intercept[UnsupportedOperationException] { avroEnum.asInt() }
    intercept[UnsupportedOperationException] { avroEnum.asBoolean() }
    intercept[UnsupportedOperationException] { avroEnum.asLong() }
    intercept[UnsupportedOperationException] { avroEnum.asFloat() }
    intercept[UnsupportedOperationException] { avroEnum.asDouble() }
    intercept[UnsupportedOperationException] { avroEnum.asBytes() }
    intercept[UnsupportedOperationException] { avroEnum.asString() }
    intercept[UnsupportedOperationException] { avroEnum.asList() }
    intercept[UnsupportedOperationException] { avroEnum.asMap() }
    intercept[UnsupportedOperationException] { avroEnum.asRecord() }
  }

  test("An Enum is correctly wrapped by an AvroEnum.") {
    // Use HashType enum for testing.
    val avroEnum = AvroUtil.wrapGenericAvro(HashType.MD5)
    assert(avroEnum.isInstanceOf[AvroEnum])
    assert("MD5" === avroEnum.asEnumName)
  }

  test("AvroRecord throws unsupported exceptions when converting to the wrong type.") {
    // construct a genericrecord with arbitrary fields; in this case it's from HashSpec.
    val genericRecord: GenericRecord = new GenericData.Record(HashSpec.SCHEMA$)
    genericRecord.put("hash_type", HashType.MD5)
    genericRecord.put("hash_size", 12)
    genericRecord.put("suppress_key_materialization", true)
    val avroRecord = AvroUtil.wrapGenericAvro(genericRecord).asInstanceOf[AvroValue]
    intercept[UnsupportedOperationException] { avroRecord.asInt() }
    intercept[UnsupportedOperationException] { avroRecord.asBoolean() }
    intercept[UnsupportedOperationException] { avroRecord.asLong() }
    intercept[UnsupportedOperationException] { avroRecord.asFloat() }
    intercept[UnsupportedOperationException] { avroRecord.asDouble() }
    intercept[UnsupportedOperationException] { avroRecord.asBytes() }
    intercept[UnsupportedOperationException] { avroRecord.asString() }
    intercept[UnsupportedOperationException] { avroRecord.asList() }
    intercept[UnsupportedOperationException] { avroRecord.asMap() }
    intercept[UnsupportedOperationException] { avroRecord.asEnumName() }
  }

  test("A Record is correctly wrapped by an AvroRecord.") {
    val genericRecord = new GenericData.Record(HashSpec.SCHEMA$)
    genericRecord.put("hash_type", HashType.MD5)
    genericRecord.put("hash_size", 12)
    genericRecord.put("suppress_key_materialization", true)
    val avroRecord = AvroUtil.wrapGenericAvro(genericRecord).asInstanceOf[AvroValue]
    assert(classOf[AvroRecord] === avroRecord.getClass)
    assert("MD5" === avroRecord("hash_type").asEnumName)
    assert(12 === avroRecord("hash_size").asInt)
    assert(true === avroRecord("suppress_key_materialization").asBoolean)
  }
}
