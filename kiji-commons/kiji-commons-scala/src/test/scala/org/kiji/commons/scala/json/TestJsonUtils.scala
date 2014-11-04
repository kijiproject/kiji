// (c) Copyright 2014 WibiData, Inc.
package org.kiji.commons.scala.json

import scala.collection.JavaConverters.seqAsJavaListConverter

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.Assert
import org.junit.Test

class TestJsonUtils {
  import TestJsonUtils._

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
}

object TestJsonUtils {
  val emptyObject: JsonNode = JsonUtils.parseJsonString("{}")

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
