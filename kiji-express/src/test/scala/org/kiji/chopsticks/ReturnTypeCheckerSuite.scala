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
  val intHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Integer]()
    map.put(new java.lang.Long(10), new java.lang.Integer(10))
    map.put(new java.lang.Long(20), new java.lang.Integer(20))
    map
  }

  val boolHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Boolean]()
    map.put(new java.lang.Long(10), new java.lang.Boolean(true))
    map.put(new java.lang.Long(20), new java.lang.Boolean(false))
    map
  }

  val longHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Long]()
    map.put(new java.lang.Long(10), new java.lang.Long(10))
    map
  }

  val floatHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Float]()
    map.put(new java.lang.Long(10), new java.lang.Float(10))
    map
  }

  val doubleHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Double]()
    map.put(new java.lang.Long(10), new java.lang.Double(10))
    map.put(new java.lang.Long(20), new java.lang.Double(20))
    map.put(new java.lang.Long(30), new java.lang.Double(25))
    map
  }

  val bytesHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.nio.ByteBuffer]()
    map.put(new java.lang.Long(10), java.nio.ByteBuffer.wrap(Array(0x11, 0x12)))
    map
  }

  val charSequenceHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.CharSequence]()
    map.put(new java.lang.Long(10), "test")
    map.put(new java.lang.Long(20), "string")
    map
  }

  val listHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.util.List[java.lang.Integer]]()
    map.put(new java.lang.Long(10),
      {
        val arr = new java.util.ArrayList[java.lang.Integer]()
        arr.add(1)
        arr.add(2)
        arr
      })
    map.put(new java.lang.Long(20),
      {
        val arr = new java.util.ArrayList[java.lang.Integer]()
        arr.add(3)
        arr.add(4)
        arr
      })
    map
  }

  // avro hash maps always have String keys
  val mapHashMap = {
    val map = new java.util.TreeMap[java.lang.Long,
      java.util.Map[java.lang.String, java.lang.Integer]]()
    map.put(new java.lang.Long(10),
      {
        val internalMap = new java.util.HashMap[java.lang.String, java.lang.Integer]()
        internalMap.put("t1", 1)
        internalMap.put("t2", 2)
        internalMap
      }
    )
    map.put(new java.lang.Long(20),
      {
        val internalMap = new java.util.HashMap[java.lang.String, java.lang.Integer]()
        internalMap.put("t3", 3)
        internalMap.put("t4", 4)
        internalMap
      }
    )
    map
  }

  val voidHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Void]()
    // scalastyle:off null
    map.put(new java.lang.Long(10), null)
    // scalastyle:on null
    map
  }

  val unionHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.lang.Object]()
    map.put(new java.lang.Long(10), new java.lang.Integer(10))
    map.put(new java.lang.Long(20), "test")
    map
  }

  val enumHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, RowKeyEncoding]()
    map.put(new java.lang.Long(10), RowKeyEncoding.FORMATTED)
    map.put(new java.lang.Long(20), RowKeyEncoding.HASH_PREFIX)
    map
  }

  val fixedHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, org.kiji.schema.avro.MD5Hash]()
    map.put(new java.lang.Long(10), new org.kiji.schema.avro.MD5Hash(Array[Byte](01, 02)))
    map.put(new java.lang.Long(20), new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04)))
    map
  }

  val recordHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, org.kiji.schema.avro.HashSpec]()
    map.put(new java.lang.Long(10), HashSpec.newBuilder.setHashSize(2).build)
    map.put(new java.lang.Long(20), HashSpec.newBuilder().build())
    map
  }

  val badRecordHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, ColumnRequestOptions]()
    val filter = new RegexQualifierColumnFilter(".*")
    val maxVersions = 2
    val opts = new ColumnRequestOptions(maxVersions, Some(filter))
    map.put(new java.lang.Long(10), opts)
    map
  }

  val nestedListHashMap = {
    val map = new java.util.TreeMap[java.lang.Long, java.util.List[org.kiji.schema.avro.MD5Hash]]()
    map.put(new java.lang.Long(10),
    {
      val arr = new java.util.ArrayList[org.kiji.schema.avro.MD5Hash]()
      arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](01, 02)))
      arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04)))
      arr
    })
    map.put(new java.lang.Long(20),
    {
      val arr = new java.util.ArrayList[org.kiji.schema.avro.MD5Hash]()
      arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](05, 06)))
      arr.add(new org.kiji.schema.avro.MD5Hash(Array[Byte](07, 0x08)))
      arr
    })
    map
  }

  val nestedMapHashMap = {
    val map = new java.util.TreeMap[java.lang.Long,
      java.util.Map[java.lang.String, org.kiji.schema.avro.MD5Hash]]()
    map.put(new java.lang.Long(10),
    {
      val internalMap = new java.util.HashMap[java.lang.String, org.kiji.schema.avro.MD5Hash]()
      internalMap.put("t1", new org.kiji.schema.avro.MD5Hash(Array[Byte](01, 02)))
      internalMap.put("t2", new org.kiji.schema.avro.MD5Hash(Array[Byte](03, 04)))
      internalMap
    }
    )
    map.put(new java.lang.Long(20),
    {
      val internalMap = new java.util.HashMap[java.lang.String, org.kiji.schema.avro.MD5Hash]()
      internalMap.put("t3", new org.kiji.schema.avro.MD5Hash(Array[Byte](05, 06)))
      internalMap.put("t4", new org.kiji.schema.avro.MD5Hash(Array[Byte](07, 0x08)))
      internalMap
    }
    )
    map
  }

  test("Test conversion of Int type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(intHashMap)
    assert(2 == res.size)
    res.foreach(kv => {
      assert(kv._2.isInstanceOf[Int])
    })
    assert(10 == res(10L))
    assert(20 == res(20L))

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == intHashMap)
  }

  test("Test conversion of Boolean type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(boolHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Boolean]))
    assert(true == res(10L))
    assert(false == res(20L))
    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == boolHashMap)
  }

  test("Test conversion of Long type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(longHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Long]))
    assert(10L == res(10L))
    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == longHashMap)
  }

  test("Test conversion of Float type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(floatHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Float]))
    assert(10F == res(10L))
    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == floatHashMap)
  }

  test("Test conversion of Double type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(doubleHashMap)
    assert(3 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Double]))
    assert(10D == res(10L))
    assert(20D == res(20L))
    assert(25D == res(30L))
    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == doubleHashMap)
  }

  test("Test conversion of avro bytes type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(bytesHashMap)
    assert(1 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Array[Byte]]))
    val expected: Array[Byte] = Array(17, 18)
    assert(expected.deep == res(10L).asInstanceOf[Array[Byte]].deep)

    // convert back
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column2")
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res,
        tableLayout.getSchema(columnName))
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == bytesHashMap)

  }

  test("Test conversion of avro CharSequence type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(charSequenceHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[String]))
    assert("test" == res(10L))
    assert("string" == res(20L))

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == charSequenceHashMap)
  }

  test("Test conversion of avro list type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(listHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[List[Int]]))
    val expect1: List[Int] = List(1, 2)
    val expect2: List[Int] = List(3, 4)
    assert(expect1 == res(10L))
    assert(expect2 == res(20L))
    // convert back
    val cellSchema = Schema.createArray(Schema.create(Schema.Type.INT))
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, cellSchema)
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == listHashMap)
  }

  test("Test conversion of avro map type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(mapHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Map[String, Int]]))
    val expect1: Map[String, Int] = Map("t1"->1, "t2"->2)
    val expect2: Map[String, Int] = Map("t3"->3, "t4"->4)
    assert(expect1 == res(10L))
    assert(expect2 == res(20L))
    // convert back
    val cellSchema = Schema.createMap(Schema.create(Schema.Type.INT))
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, cellSchema)
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == mapHashMap)
  }

  test("Test conversion of null type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(voidHashMap)
    assert(1 == res.size)
    // scalastyle:off null
    res.foreach(kv => assert(null == kv._2))
    // convert back
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == voidHashMap)
  }

  test("Test conversion of union type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(unionHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[java.lang.Object]))
    assert(10 == res(10L))
    assert("test" == res(20L))

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == unionHashMap)
  }

  test("Test conversion of enum type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(enumHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[RowKeyEncoding]))
    assert(RowKeyEncoding.FORMATTED == res(10L))
    assert(RowKeyEncoding.HASH_PREFIX == res(20L))

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == enumHashMap)
  }

  test("Test conversion of avro fixed type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(fixedHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Array[Byte]]))
    assert(Array[Byte](01, 02).deep == res(10L).asInstanceOf[Array[Byte]].deep)
    assert(Array[Byte](03, 04).deep == res(20L).asInstanceOf[Array[Byte]].deep)

    // convert back
    val tableLayout = KijiTableLayout.newLayout(KijiTableLayouts.getLayout("avro-types.json"))
    val columnName = new KijiColumnName("family", "column1")
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res,
        tableLayout.getSchema(columnName))
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == fixedHashMap)
  }

  test("Test conversion of avro record type while reading/writing from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(recordHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[HashSpec]))
    val expect1 = HashSpec.newBuilder.setHashSize(2).build
    val expect2 = HashSpec.newBuilder().build()
    assert(expect1 == res(10L).asInstanceOf[HashSpec])
    assert(expect2 == res(20L).asInstanceOf[HashSpec])

    // convert back
    // scalastyle:off null
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, null)
    // scalastyle:on null
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    assert(resJava == recordHashMap)
  }

  test("Test conversion of avro list of fixed type elements while reading/writing "
      + "from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(nestedListHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[List[Array[Byte]]]))
    res(10L).asInstanceOf[List[Array[Byte]]](0).deep == Array[Byte](01, 02).deep
    res(10L).asInstanceOf[List[Array[Byte]]](1).deep == Array[Byte](03, 04).deep
    res(20L).asInstanceOf[List[Array[Byte]]](0).deep == Array[Byte](05, 06).deep
    res(20L).asInstanceOf[List[Array[Byte]]](1).deep == Array[Byte](07, 0x08).deep

    // convert back
    val columnSchema = Schema.createArray(Schema.createFixed("test", "fixed schema", "", 2))
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, columnSchema)
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    resJava should equal(nestedListHashMap)
  }

  test("Test conversion of avro map of string to fixed type elements while reading/writing "
    + "from/to Kiji Table") {
    val res = KijiScheme.convertKijiValuesToScalaTypes(nestedMapHashMap)
    assert(2 == res.size)
    res.foreach(kv => assert(kv._2.isInstanceOf[Map[String, Array[Byte]]]))
    res(10L).asInstanceOf[Map[String, Array[Byte]]]("t1") == Array[Byte](01, 02)
    res(10L).asInstanceOf[Map[String, Array[Byte]]]("t2") == Array[Byte](03, 04)
    res(20L).asInstanceOf[Map[String, Array[Byte]]]("t3") == Array[Byte](05, 06)
    res(20L).asInstanceOf[Map[String, Array[Byte]]]("t4") == Array[Byte](07, 0x08)

    // convert back
    val columnSchema = Schema.createMap(Schema.createFixed("test", "fixed schema", "", 2))
    val resJava = KijiScheme.convertScalaTypesToKijiValues(res, columnSchema)
    assert(resJava.isInstanceOf[java.util.NavigableMap[java.lang.Long, java.lang.Object]])
    resJava should equal(nestedMapHashMap)
  }

  test("Test for exception when converting unsupported avro class") {
    intercept[InvalidClassException] {
      KijiScheme.convertKijiValuesToScalaTypes(badRecordHashMap)
    }
  }
}
