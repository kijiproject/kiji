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

package org.kiji.chopsticks

import java.io.InvalidClassException

import scala.collection.JavaConversions._

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.filter.RegexQualifierColumnFilter
import org.kiji.schema.KijiColumnName
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.apache.avro.Schema

/**
 * Test for converting back and forth between Java and Scala for values
 * that are read/written from a Kiji column.
 */
class ReturnTypeCheckerSuite extends FunSuite with ShouldMatchers {
  val int: java.lang.Integer = new java.lang.Integer(10)
  test("Test conversion of Integers from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(int)
    assert(res.isInstanceOf[Int])
    assert(10 == res)
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.Integer])
    assert(resJava == int)
  }

  val boolean: java.lang.Boolean = new java.lang.Boolean(true)
  test("Test conversion of Boolean type from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(boolean)
    assert(res.isInstanceOf[Boolean])
    assert(true == res)
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.Boolean])
    assert(resJava == boolean)
  }

  val long: java.lang.Long= new java.lang.Long(10)
  test("Test conversion of Long from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(long)
    assert(res.isInstanceOf[Long])
    assert(10L == res)
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.Long])
    assert(resJava == long)
  }

  val float: java.lang.Float =  new java.lang.Float(10)
  test("Test conversion of Float from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(float)
    assert(res.isInstanceOf[Float])
    assert(10F == res)
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.Float])
    assert(resJava == float)
  }

  val double: java.lang.Double = new java.lang.Double(10)
  test("Test conversion of Double from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(double)
    assert(res.isInstanceOf[Double])
    assert(10 == res)
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.Double])
    assert(resJava == double)
  }

  val bytes: java.nio.ByteBuffer =  java.nio.ByteBuffer.wrap(Array(0x11, 0x12))
  test("Test conversion of Avro bytes from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(bytes)
    assert(res.isInstanceOf[Array[Byte]])
    assert(Array(17, 18).deep == res.asInstanceOf[Array[Byte]].deep)
    // Scala => Java
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column2")
    val resJava = KijiScheme.convertScalaTypes(res, tableLayout.getSchema(columnName))
    assert(resJava.isInstanceOf[java.nio.ByteBuffer])
    assert(resJava == bytes)
  }

  val charSequence: java.lang.CharSequence = "string".asInstanceOf[java.lang.CharSequence]
  test("Test conversion of Avro CharSequence from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(charSequence)
    assert(res.isInstanceOf[String])
    assert("string" == res.asInstanceOf[String])
    // Scala => Java
    val resJava = KijiScheme.convertScalaTypes(res, null)
    assert(resJava.isInstanceOf[java.lang.CharSequence])
    assert(resJava == charSequence)
  }

  val list: java.util.List[java.lang.Integer] = {
    val arr = new java.util.ArrayList[java.lang.Integer]()
    arr.add(1)
    arr.add(2)
    arr
  }
  test("Test conversion of Avro list from Java, to Scala, and back again.") {
    // Java => Scala
    val res = KijiScheme.convertJavaTypes(list)
    assert(res.isInstanceOf[List[Int]])
    assert(List(1, 2) == res.asInstanceOf[List[Int]])
    // Scala => Java
    val cellSchema = Schema.createArray(Schema.create(Schema.Type.INT))
    val resJava = KijiScheme.convertScalaTypes(res, cellSchema)
    assert(resJava.isInstanceOf[java.util.List[java.lang.Integer]])
    assert(resJava == list)
  }

  // Avro maps always have String keys
  val map: java.util.Map[java.lang.String, java.lang.Integer] = {
    val internalMap = new java.util.HashMap[java.lang.String, java.lang.Integer]()
    internalMap.put("t1", 1)
    internalMap.put("t2", 2)
    internalMap
  }
  test("Test conversion of Avro map from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(map)
    assert(res.isInstanceOf[Map[String, Int]])
    assert(2 == res.asInstanceOf[Map[String, Int]].size)
    assert(Map("t1"->1, "t2"->2) == res)
    // convert back
    val cellSchema = Schema.createMap(Schema.create(Schema.Type.INT))
    val resJava = KijiScheme.convertScalaTypes(res, cellSchema)
    assert(resJava.isInstanceOf[java.util.Map[java.lang.String, java.lang.Object]])
    assert(resJava.asInstanceOf[java.util.Map[java.lang.String, java.lang.Object]].get("t1")
        .isInstanceOf[java.lang.Integer])
    assert(resJava == map)
  }

  val void: java.lang.Void = null.asInstanceOf[java.lang.Void]
  test("Test conversion of null from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(void)
    assert(res == null)
    val resJava = KijiScheme.convertScalaTypes(res, null)
    // scalastyle:on null
    assert(resJava == void)
  }

  val unionList: java.util.List[java.lang.Object] = {
    val list = new java.util.ArrayList[java.lang.Object]()
    list.add(new java.lang.Long(10))
    list.add("test")
    list
  }
  test("Test conversion of Avro unions from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(unionList)
    assert(res.isInstanceOf[List[Any]])
    assert(2 == res.asInstanceOf[List[Any]].size)
    assert(10L == res.asInstanceOf[List[Any]](0))
    assert("test" == res.asInstanceOf[List[Any]](1))

    // convert back
    // scalastyle:off null
    val unionSchema = Schema.createUnion(seqAsJavaList(List(Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.STRING))))
    val cellSchema = Schema.createArray(unionSchema)
    val resJava = KijiScheme.convertScalaTypes(res, cellSchema)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.List[java.lang.Object]])
    assert(resJava == unionList)
  }

  val enum: RowKeyEncoding = RowKeyEncoding.FORMATTED
  test("Test conversion of enums from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(enum)
    assert(res.isInstanceOf[RowKeyEncoding])
    assert(RowKeyEncoding.FORMATTED == res)

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypes(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[RowKeyEncoding])
    assert(resJava == enum)
  }

  val fixed: org.kiji.schema.avro.MD5Hash =
      new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04))
  test("Test conversion of Avro fixed type from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(fixed)
    assert(res.isInstanceOf[Any])
    assert(Array[Byte](03, 04).deep == res.asInstanceOf[Array[Byte]].deep)

    // convert back
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column1")
    val resJava = KijiScheme.convertScalaTypes(res, tableLayout.getSchema(columnName))
    assert(resJava.isInstanceOf[org.apache.avro.generic.GenericData.Fixed])
    assert(resJava == fixed)
  }
  val record: org.kiji.schema.avro.HashSpec = HashSpec.newBuilder().build()
  test("Test conversion of Avro record from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(record)
    assert(res.isInstanceOf[HashSpec])
    val expect2 = HashSpec.newBuilder().build()
    assert(record == res.asInstanceOf[HashSpec])

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypes(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[org.kiji.schema.avro.HashSpec])
    assert(record == resJava)
  }

  val nestedList: java.util.List[org.kiji.schema.avro.MD5Hash] = {
    val arr = new java.util.ArrayList[org.kiji.schema.avro.MD5Hash]()
    arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](05, 06)))
    arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](07, 0x08)))
    arr
  }
  test("Test conversion of Avro list of fixed type from Java, to Scala, and back again.") {
    val res = KijiScheme.convertJavaTypes(nestedList)
    assert(res.isInstanceOf[List[Array[Byte]]])
    assert(Array[Byte](05, 06).deep == res.asInstanceOf[List[Array[Byte]]](0).deep)
    assert(Array[Byte](07, 0x08).deep == res.asInstanceOf[List[Array[Byte]]](1).deep)

    // convert back
    val columnSchema = Schema.createArray(Schema.createFixed("test", "fixed schema", "", 2))
    val resJava = KijiScheme.convertScalaTypes(res, columnSchema)
    assert(resJava.isInstanceOf[java.util.List[org.kiji.schema.avro.MD5Hash]])
    assert(resJava == nestedList)
  }
  val filter = new RegexQualifierColumnFilter(".*")
  val maxVersions = 2
  val badRecord: ColumnRequestOptions = new ColumnRequestOptions(maxVersions, Some(filter))
  test("Test for exception when converting unsupported an Avro class.") {
    intercept[InvalidClassException] {
      KijiScheme.convertJavaTypes(badRecord)
    }
  }
}
