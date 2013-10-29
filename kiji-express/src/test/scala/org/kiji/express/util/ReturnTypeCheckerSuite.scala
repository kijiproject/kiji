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

import java.io.InvalidClassException

import org.apache.avro.generic.GenericData
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import org.kiji.express.AvroFixed
import org.kiji.express.AvroRecord
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * Test for converting back and forth between Java and Scala for values
 * that are read/written from a Kiji column.
 */
@RunWith(classOf[JUnitRunner])
class ReturnTypeCheckerSuite extends FunSuite {
  val int: java.lang.Integer = new java.lang.Integer(10)
  test("Test conversion of Integers from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(int)
    assert(res.isInstanceOf[Int])
    assert(10 === res)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.Integer])
    assert(resJava === int)
  }

  val boolean: java.lang.Boolean = new java.lang.Boolean(true)
  test("Test conversion of Boolean type from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(boolean)
    assert(res.isInstanceOf[Boolean])
    assert(true === res)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.Boolean])
    assert(resJava === boolean)
  }

  val long: java.lang.Long= new java.lang.Long(10)
  test("Test conversion of Long from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(long)
    assert(res.isInstanceOf[Long])
    assert(10L === res)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.Long])
    assert(resJava === long)
  }

  val float: java.lang.Float =  new java.lang.Float(10)
  test("Test conversion of Float from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(float)
    assert(res.isInstanceOf[Float])
    assert(10F === res)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.Float])
    assert(resJava === float)
  }

  val double: java.lang.Double = new java.lang.Double(10)
  test("Test conversion of Double from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(double)
    assert(res.isInstanceOf[Double])
    assert(10 === res)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.Double])
    assert(resJava === double)
  }

  val bytes: java.nio.ByteBuffer =  java.nio.ByteBuffer.wrap(Array(0x11, 0x12))
  test("Test conversion of Avro bytes from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(bytes)
    assert(res.isInstanceOf[Array[Byte]])
    assert(Array(17, 18).deep === res.asInstanceOf[Array[Byte]].deep)
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.nio.ByteBuffer])
    assert(resJava === bytes)
  }

  val charSequence: java.lang.CharSequence = "string".asInstanceOf[java.lang.CharSequence]
  test("Test conversion of Avro CharSequence from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(charSequence)
    assert(res.isInstanceOf[String])
    assert("string" === res.asInstanceOf[String])
    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.lang.CharSequence])
    assert(resJava === charSequence)
  }

  val list: java.util.List[java.lang.Integer] = {
    val arr = new java.util.ArrayList[java.lang.Integer]()
    arr.add(1)
    arr.add(2)
    arr
  }
  test("Test conversion of Avro list from Java, to Scala, and back again.") {
    // Java => Scala
    val res = AvroUtil.decodeGenericFromJava(list)
    assert(res.isInstanceOf[List[_]])
    assert(res.asInstanceOf[List[_]](0).isInstanceOf[Int])
    assert(List(1, 2) === res.asInstanceOf[List[Int]])

    // Scala => Java
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.util.List[_]])
    assert(resJava.asInstanceOf[java.util.List[_]].get(0).isInstanceOf[java.lang.Integer])
    assert(resJava === list)
  }

  // Avro maps always have String keys
  val map: java.util.Map[java.lang.String, java.lang.Integer] = {
    val internalMap = new java.util.HashMap[java.lang.String, java.lang.Integer]()
    internalMap.put("t1", 1)
    internalMap.put("t2", 2)
    internalMap
  }
  test("Test conversion of Avro map from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(map)
    assert(res.isInstanceOf[Map[_, _]])
    res.asInstanceOf[Map[String, Int]].foreach { case (key, value) =>
      key.isInstanceOf[String]
      value.isInstanceOf[Int]
    }
    assert(2 === res.asInstanceOf[Map[String, Int]].size)
    assert(Map("t1"->1, "t2"->2) === res)
    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.util.Map[_, _]])
    val iter = resJava.asInstanceOf[java.util.Map[java.lang.String, java.lang.Integer]]
        .entrySet
        .iterator
    while (iter.hasNext()) {
      val entry = iter.next()
      entry.getKey.isInstanceOf[java.lang.String]
      entry.getValue.isInstanceOf[java.lang.Integer]
    }
    assert(resJava === map)
  }

  // scalastyle:off null
  val void: java.lang.Void = null.asInstanceOf[java.lang.Void]
  test("Test conversion of null from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(void)
    assert(res === null)
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava === void)
  }
  // scalastyle:on null

  val unionList: java.util.List[java.lang.Object] = {
    val list = new java.util.ArrayList[java.lang.Object]()
    list.add(new java.lang.Long(10))
    list.add("test")
    list
  }
  test("Test conversion of Avro unions from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(unionList)
    assert(res.isInstanceOf[List[_]])
    assert(2 === res.asInstanceOf[List[Any]].size)
    assert(10L === res.asInstanceOf[List[Any]](0))
    assert("test" === res.asInstanceOf[List[Any]](1))

    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.util.List[_]])
    assert(resJava === unionList)
  }

  val enum: RowKeyEncoding = RowKeyEncoding.FORMATTED
  test("Test conversion of enums from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(enum)
    assert(res.isInstanceOf[RowKeyEncoding])
    assert(RowKeyEncoding.FORMATTED === res)

    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[RowKeyEncoding])
    assert(resJava === enum)
  }

  val specificFixed: org.kiji.schema.avro.MD5Hash =
      new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04))
  test("Test conversion of Avro fixed type from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(specificFixed)
    assert(res.isInstanceOf[AvroFixed])
    assert(Array[Byte](03, 04).deep === res.asInstanceOf[AvroFixed].asFixedBytes.deep)

    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[org.apache.avro.generic.GenericData.Fixed])
    assert(resJava === specificFixed)
  }
  val record: org.kiji.schema.avro.HashSpec = HashSpec.newBuilder().build()
  test("Test conversion of specific Avro record from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeSpecificFromJava(record)
    assert(res.isInstanceOf[HashSpec])
    val expect2 = HashSpec.newBuilder().build()
    assert(record === res.asInstanceOf[HashSpec])

    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[HashSpec])
    assert(record === resJava)
  }

  test("Test conversion of generic Avro record from Java, to Scala, and back again.") {
    // Construct generic record.
    val genericRecord = new GenericData.Record(HashSpec.SCHEMA$)
    genericRecord.put("hash_type", HashType.MD5)
    genericRecord.put("hash_size", 12)
    genericRecord.put("suppress_key_materialization", true)

    // convert to scala
    val res = AvroUtil.decodeGenericFromJava(genericRecord)
    assert(res.isInstanceOf[AvroRecord])
    val resGeneric = res.asInstanceOf[AvroRecord]
    assert("MD5" === resGeneric("hash_type").asEnumName)
    assert(12 === resGeneric("hash_size").asInt)
    assert(true === resGeneric("suppress_key_materialization").asBoolean)
  }

  val nestedList: java.util.List[org.kiji.schema.avro.MD5Hash] = {
    val arr = new java.util.ArrayList[org.kiji.schema.avro.MD5Hash]()
    arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](05, 06)))
    arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](07, 0x08)))
    arr
  }
  test("Test conversion of Avro list of fixed type from Java, to Scala, and back again.") {
    val res = AvroUtil.decodeGenericFromJava(nestedList)
    assert(res.isInstanceOf[List[Array[Byte]]])
    assert(Array[Byte](05, 06).deep === res.asInstanceOf[List[AvroFixed]](0).asFixedBytes.deep)
    assert(Array[Byte](07, 0x08).deep === res.asInstanceOf[List[AvroFixed]](1).asFixedBytes.deep)

    // convert back
    val resJava = AvroUtil.encodeToJava(res)
    assert(resJava.isInstanceOf[java.util.List[_]])
    assert(resJava === nestedList)
  }

  test("Test for exception when converting unsupported an Avro class.") {
    case class NotARecord(dummyArg: Int)
    val badRecord = NotARecord(0)
    intercept[InvalidClassException] {
      AvroUtil.decodeGenericFromJava(badRecord)
    }
  }

}
