/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.commons.scala.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.FloatNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.ShortNode
import com.fasterxml.jackson.databind.node.TextNode
import org.junit.Assert
import org.junit.Test

import org.kiji.commons.avro.FakeRecord
import org.kiji.commons.scala.json.JsonUtils.AvroToJson
import org.kiji.commons.scala.json.JsonUtils.JsonToAvro

class TestJsonUtils {
  import org.kiji.commons.scala.json.TestJsonUtils._

  @Test
  def findsNulls() {
    Assert.assertTrue(JsonUtils.isNull(null))
    Assert.assertTrue(JsonUtils.isNull(JsonUtils.Mapper.getNodeFactory.nullNode()))
    Assert.assertFalse(JsonUtils.isNull(emptyObject))
    Assert.assertFalse(JsonUtils.isNull(JsonUtils.parseJsonString("\"string\"")))
    Assert.assertFalse(JsonUtils.isNull(JsonUtils.parseJsonString("1")))
    Assert.assertFalse(JsonUtils.isNull(JsonUtils.parseJsonString("[1, 2]")))
  }

  @Test
  def extractsArrays() {
    expectException(
        JsonUtils.extractArray,
        emptyObject,
        null)

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("[1, 2, 3]"),
        Seq("1", "2", "3").map { JsonUtils.parseJsonString })

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("""["a", "b", "c"]"""),
        Seq("\"a\"", "\"b\"", "\"c\"").map { JsonUtils.parseJsonString })

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("[{\"a\":1}, {\"b\":2}, {\"c\":3}]"),
        Seq("{\"a\":1}", "{\"b\":2}", "{\"c\":3}").map { JsonUtils.parseJsonString })

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("""[{"a":1}, "b", 3]"""),
        Seq(
            JsonUtils.parseJsonString("{\"a\":1}"),
            JsonUtils.parseJsonString("\"b\""),
            JsonUtils.parseJsonString("3")))

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("[]"),
        Seq())

    expectSuccess(
        JsonUtils.extractArray,
        JsonUtils.parseJsonString("""[[1, 2, 3], ["a", "b", "c"]]"""),
        Seq(
            JsonUtils.parseJsonString("[1, 2, 3]"),
            JsonUtils.parseJsonString("""["a", "b", "c"]""")))
  }

  @Test
  def createsArrays() {
    expectSuccess(
        JsonUtils.arrayNode,
        Seq("1", "2", "3").map { JsonUtils.parseJsonString },
        JsonUtils.parseArrayString("[1, 2, 3]"))

    expectSuccess(
        JsonUtils.arrayNode,
        Seq("\"a\"", "\"b\"", "\"c\"").map { JsonUtils.parseJsonString },
        JsonUtils.parseArrayString("""["a", "b", "c"]"""))

    expectSuccess(
        JsonUtils.arrayNode,
        Seq("{\"a\":1}", "{\"b\":2}", "{\"c\":3}").map { JsonUtils.parseJsonString },
        JsonUtils.parseArrayString("""[{"a":1}, {"b":2}, {"c":3}]"""))

    expectSuccess(
        JsonUtils.arrayNode,
        Seq(
            JsonUtils.parseJsonString("{\"a\":1}"),
            JsonUtils.parseJsonString("\"b\""),
            JsonUtils.parseJsonString("3")),
        JsonUtils.parseArrayString("""[{"a":1}, "b", 3]"""))

    expectSuccess(
        JsonUtils.arrayNode,
        Seq(),
        JsonUtils.parseArrayString("[]"))
  }

  @Test
  def flattensArrays() {
    expectSuccess(
        JsonUtils.flattenArrays,
        Seq(JsonUtils.parseArrayString("[1, 2, 3]"), JsonUtils.parseArrayString("[4, 5, 6]")),
        JsonUtils.parseArrayString("[1, 2, 3, 4, 5, 6]"))

    expectSuccess(
        JsonUtils.flattenArrays,
        Seq(
            JsonUtils.parseArrayString("[1, 2, 3]"),
            JsonUtils.parseArrayString("""["a", "b", "c"]"""),
            JsonUtils.parseArrayString("""[{"d":4},"e",5]""")),
        JsonUtils.parseArrayString("""[1, 2, 3, "a", "b", "c", {"d":4}, "e", 5]"""))

    expectSuccess(
        JsonUtils.flattenArrays,
        Seq(
            JsonUtils.parseArrayString("[1, 2, 3]"),
            JsonUtils.parseArrayString("[]")),
        JsonUtils.parseArrayString("[1, 2, 3]"))

    expectSuccess(
        JsonUtils.flattenArrays,
        Seq(
            JsonUtils.parseArrayString("[]"),
            JsonUtils.parseArrayString("[]")),
        JsonUtils.parseArrayString("[]"))
  }

  @Test
  def extractsStringArrays() {
    expectException(
        JsonUtils.extractStringArray,
        JsonUtils.parseJsonString("""{"a":1}"""),
        "Non-array node passed to extractStringArray().")

    expectException(
        JsonUtils.extractStringArray,
        JsonUtils.parseJsonString("""[1, 2, 3]"""),
        "Non-string node found in array in extractStringArray().")

    expectSuccess(
        JsonUtils.extractStringArray,
        JsonUtils.parseJsonString("""["a", "b", "c"]"""),
        Seq("a", "b", "c"))
  }

  @Test
  def getsStringOrArrayOfString() {
    expectException(
        JsonUtils.getStringOrArrayOfString,
        JsonUtils.parseJsonString("{\"a\":1}"),
        "Neither array nor text node passed to getStringOrArrayOfString().")

    expectSuccess(
        JsonUtils.getStringOrArrayOfString,
        JsonUtils.parseJsonString("\"string\""),
        Seq("string"))
    expectSuccess(
        JsonUtils.getStringOrArrayOfString,
        JsonUtils.parseJsonString("""["string", "string2", "string3"]"""),
        Seq("string", "string2", "string3"))
  }

  @Test
  def setsFieldAtPath() {
    def expectException(
        objectNode: ObjectNode,
        pathString: String,
        value: JsonNode,
        expectedError: String
    ) {
      try {
        JsonUtils.setFieldAtPath(objectNode, pathString, value)
        Assert.fail("expected exception.")
      } catch {
        case e: Exception => Assert.assertEquals(expectedError, e.getMessage)
      }
    }

    def expectSuccess(
        objectNode: ObjectNode,
        pathString: String,
        value: JsonNode,
        expected: ObjectNode
    ): ObjectNode = {
      JsonUtils.setFieldAtPath(objectNode, pathString, value)
      Assert.assertEquals(expected, objectNode)
      objectNode
    }

    expectException(
        emptyObject.deepCopy(),
        "",
        emptyObject.deepCopy(),
        "Cannot set field at an empty path.")

    expectException(
        emptyObject.deepCopy(),
        "thing.",
        emptyObject.deepCopy(),
        "Illegal path: 'thing.'. Ends with '.'.")

    expectSuccess(
        emptyObject.deepCopy(),
        "path",
        emptyObject.deepCopy(),
        JsonUtils.parseObjectString("{\"path\":{}}}"))

    expectSuccess(
        emptyObject.deepCopy(),
        "path.field",
        emptyObject.deepCopy(),
        JsonUtils.parseObjectString("{\"path\":{\"field\":{}}}"))

    expectSuccess(
        emptyObject.deepCopy(),
        "path.field",
        JsonUtils.parseJsonString("1"),
        JsonUtils.parseObjectString("{\"path\":{\"field\":1}}"))

    expectSuccess(
        emptyObject.deepCopy(),
        "path.field",
        JsonUtils.parseJsonString("[1, \"2\", {\"field\":3}]"),
        JsonUtils.parseObjectString("{\"path\":{\"field\":[1, \"2\", {\"field\":3}]}}"))
  }

  @Test
  def accessesFieldsAtPath() {
    def expectException(
        jsonNode: JsonNode,
        pathString: String,
        expectedError: String
    ) {
      try {
        JsonUtils.accessFieldAtPath(jsonNode, pathString)
        Assert.fail("expected exception.")
      } catch {
        case e: Exception => Assert.assertEquals(expectedError, e.getMessage)
      }
    }

    def expectSuccess(
        jsonNode: JsonNode,
        pathString: String,
        expected: JsonNode
    ): JsonNode = {
      val actual: JsonNode = JsonUtils.accessFieldAtPath(jsonNode, pathString)
      Assert.assertEquals(expected, actual)
      actual
    }

    expectException(
        emptyObject.deepCopy(),
        "thing.",
        "Illegal path: 'thing.'. Ends with '.'.")

    expectException(
        emptyObject.deepCopy(),
        ".invalid",
        "Illegal path: '.invalid'. Contains empty accessor.")

    expectSuccess(
        emptyObject.deepCopy(),
        "path",
        JsonUtils.Mapper.getNodeFactory.nullNode())

    expectException(
        JsonUtils.parseJsonString("\"string\""),
        "path",
        "Cannot access field of a non-Object Json node: '\"string\"'.")

    val json: JsonNode = JsonUtils.parseJsonString(
        """{
          |  "field" : "value",
          |  "field2" : [1, 2, 3],
          |  "field3" : {
          |    "sub_field" : 1
          |  }
          |}
        """.stripMargin)

    expectSuccess(
        json,
        "field",
        JsonUtils.parseJsonString("\"value\""))

    expectSuccess(
        json,
        "field2",
        JsonUtils.parseArrayString("[1, 2, 3]"))

    expectSuccess(
        json,
        "field3",
        JsonUtils.parseObjectString("{\"sub_field\" : 1}"))

    expectSuccess(
        json,
        "field3.sub_field",
        JsonUtils.parseJsonString("1"))
  }

  @Test
  def testAvroConversions(): Unit = {
    val record: FakeRecord = FakeRecord.newBuilder().setData(1).build()
    Assert.assertEquals(
        record.toString,
        record.toJson.toAvro[FakeRecord](record.getSchema).toString
    )
  }

  @Test
  def testEquals(): Unit = {
    def test(left: JsonNode, right: JsonNode, expected: Boolean): Unit = {
      Assert.assertEquals(expected, JsonUtils.equals(left, right))
    }

    test(text("a"), text("a"), true)
    test(text("a"), text("b"), false)
    test(text("b"), text("a"), false)
    test(text("a"), num(1), false)
    test(text("1"), num(1), false)
    test(text("a"), bool(true), false)
    test(text("true"), bool(true), false)
    test(text("abc"), arr(text("a"), text("b"), text("c")), false)
    test(text("abc"), arr(text("abc")), false)

    test(bool(true), bool(true), true)
    test(bool(true), bool(false), false)
    test(bool(false), bool(true), false)
    test(bool(false), bool(false), true)

    test(NullNode.getInstance(), NullNode.getInstance(), true)
    test(NullNode.getInstance(), text("null"), false)
    test(NullNode.getInstance(), bool(true), false)
    test(NullNode.getInstance(), bool(false), false)
    test(NullNode.getInstance(), num(0), false)

    test(new ShortNode(5), new ShortNode(5), true)
    test(new ShortNode(5), new IntNode(5), true)
    test(new ShortNode(5), new LongNode(5l), true)
    test(new ShortNode(5), new FloatNode(5.0f), true)
    test(new ShortNode(5), new DoubleNode(5.0d), true)
    test(new ShortNode(5), new ShortNode(4), false)
    test(new ShortNode(5), new IntNode(4), false)
    test(new ShortNode(5), new LongNode(4l), false)
    test(new ShortNode(5), new FloatNode(4.0f), false)
    test(new ShortNode(5), new DoubleNode(4.0d), false)

    test(new IntNode(5), new ShortNode(5), true)
    test(new IntNode(5), new IntNode(5), true)
    test(new IntNode(5), new LongNode(5l), true)
    test(new IntNode(5), new FloatNode(5.0f), true)
    test(new IntNode(5), new DoubleNode(5.0d), true)
    test(new IntNode(5), new ShortNode(4), false)
    test(new IntNode(5), new IntNode(4), false)
    test(new IntNode(5), new LongNode(4l), false)
    test(new IntNode(5), new FloatNode(4.0f), false)
    test(new IntNode(5), new DoubleNode(4.0d), false)

    test(new LongNode(5l), new ShortNode(5), true)
    test(new LongNode(5l), new IntNode(5), true)
    test(new LongNode(5l), new LongNode(5l), true)
    test(new LongNode(5l), new FloatNode(5.0f), true)
    test(new LongNode(5l), new DoubleNode(5.0d), true)
    test(new LongNode(5l), new ShortNode(4), false)
    test(new LongNode(5l), new IntNode(4), false)
    test(new LongNode(5l), new LongNode(4l), false)
    test(new LongNode(5l), new FloatNode(4.0f), false)
    test(new LongNode(5l), new DoubleNode(4.0d), false)

    test(new FloatNode(5.0f), new ShortNode(5), true)
    test(new FloatNode(5.0f), new IntNode(5), true)
    test(new FloatNode(5.0f), new LongNode(5l), true)
    test(new FloatNode(5.0f), new FloatNode(5.0f), true)
    test(new FloatNode(5.0f), new DoubleNode(5.0d), true)
    test(new FloatNode(5.0f), new ShortNode(4), false)
    test(new FloatNode(5.0f), new IntNode(4), false)
    test(new FloatNode(5.0f), new LongNode(4l), false)
    test(new FloatNode(5.0f), new FloatNode(4.0f), false)
    test(new FloatNode(5.0f), new DoubleNode(4.0d), false)

    test(new DoubleNode(5.0d), new ShortNode(5), true)
    test(new DoubleNode(5.0d), new IntNode(5), true)
    test(new DoubleNode(5.0d), new LongNode(5l), true)
    test(new DoubleNode(5.0d), new FloatNode(5.0f), true)
    test(new DoubleNode(5.0d), new DoubleNode(5.0d), true)
    test(new DoubleNode(5.0d), new ShortNode(4), false)
    test(new DoubleNode(5.0d), new IntNode(4), false)
    test(new DoubleNode(5.0d), new LongNode(4l), false)
    test(new DoubleNode(5.0d), new FloatNode(4.0f), false)
    test(new DoubleNode(5.0d), new DoubleNode(4.0d), false)

    test(arr(new ShortNode(5)), arr(new IntNode(5)), true)
    test(arr(new ShortNode(5), text("a")), arr(new IntNode(5)), false)

    test(obj("a" -> new ShortNode(5)), obj("a" -> new IntNode(5)), true)
    test(
      obj("a" -> new ShortNode(5), "b" -> text("b")),
      obj("a" -> new IntNode(5)),
      false
    )

    test(
      arr(
        obj(
          "a" -> text("a"),
          "b" -> bool(true),
          "c" -> new ShortNode(5)
        )
      ),
      arr(
        obj(
          "a" -> text("a"),
          "b" -> bool(true),
          "c" -> new ShortNode(5)
        )
      ),
      true
    )
  }
}

object TestJsonUtils {
  val emptyObject: JsonNode = JsonUtils.parseJsonString("{}")

  def text(t: String): TextNode = JsonUtils.textNode(t)
  def bool(b: Boolean): BooleanNode = JsonUtils.booleanNode(b)
  def num(i: Int): NumericNode = JsonUtils.numberNode(i)
  def arr(es: JsonNode*): ArrayNode = JsonUtils.arrayNode(es)
  def obj(es: (String, JsonNode)*): ObjectNode = JsonUtils.objectNode(es.toMap)

  // TODO move this and expectException to a TestingUtils class.
  def expectSuccess[T, U](
      f: T => U,
      input: T,
      expectedOutput: U
  ): U = {
    val actual: U = f(input)
    Assert.assertEquals(expectedOutput, actual)
    actual
  }

  def expectException[T, U](
      f: T => U,
      input: T,
      expectedError: String
  ): Unit = {
    try {
      f(input)
      Assert.fail("expected exception.")
    } catch {
      case e: Exception => Assert.assertEquals(expectedError, e.getMessage)
    }
  }
}
