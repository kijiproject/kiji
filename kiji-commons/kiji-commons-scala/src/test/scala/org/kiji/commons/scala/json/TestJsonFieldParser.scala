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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.Assert
import org.junit.Test

class TestJsonFieldParser {
  import org.kiji.commons.scala.json.TestJsonFieldParser._

  @Test
  def testJson(): Unit = {
    expectSuccess[JsonNode](
      JsonUtils.parseJsonString("1"),
      JsonUtils.Mapper.getNodeFactory.numberNode(1)
    )

    val jsonParser: JsonFieldParser[JsonNode] = JsonFieldParser(field)
    val invalid: JsonNode = JsonUtils.parseJsonString("{\"other\":1}")
    try {
      jsonParser(invalid)
    } catch {
      case iae: IllegalArgumentException => Assert.assertEquals(
          "Input field: 'null' at path: 'field' cannot be parsed to expected type.",
          iae.getMessage)
    }
  }

  @Test
  def testObject(): Unit = {
    expectSuccess[ObjectNode](
      Factory.objectNode(),
      Factory.objectNode()
    )
    expectFailure[ObjectNode](
      Factory.textNode("a"),
      "Input field: '\"a\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testArray(): Unit = {
    expectSuccess[ArrayNode](
      Factory.arrayNode(),
      Factory.arrayNode()
    )
    expectFailure[ArrayNode](
      Factory.textNode("a"),
      "Input field: '\"a\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testBoolean(): Unit = {
    expectSuccess[Boolean](
      Factory.booleanNode(true),
      true
    )
    expectFailure[Boolean](
      Factory.textNode("true"),
      "Input field: '\"true\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testString(): Unit = {
    expectSuccess[String](
      Factory.textNode("string"),
      "string"
    )
    expectFailure[String](
      Factory.numberNode(1),
      "Input field: '1' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testShort(): Unit = {
    expectSuccess[Short](
      Factory.numberNode(1),
      1
    )
    expectFailure[Short](
      Factory.textNode("one"),
      "Input field: '\"one\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testInt(): Unit = {
    expectSuccess[Int](
      Factory.numberNode(1),
      1
    )
    expectFailure[Int](
      Factory.textNode("one"),
      "Input field: '\"one\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testLong(): Unit = {
    expectSuccess[Long](
      Factory.numberNode(1),
      1
    )
    expectFailure[Long](
      Factory.textNode("one"),
      "Input field: '\"one\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testBigInt(): Unit = {
    expectSuccess[BigInt](
      Factory.numberNode(1),
      BigInt(1)
    )
    expectFailure[BigInt](
      Factory.textNode("one"),
      "Input field: '\"one\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testFloat(): Unit = {
    expectSuccess[Float](
      Factory.numberNode(1.5f),
      1.5f
    )
    expectFailure[Float](
      Factory.textNode("one point five"),
      "Input field: '\"one point five\"' at path: 'field' cannot be parsed to expected type."
    )
    // Test how a Float parser handles nodes other than FloatNode and values too large to be a Float
    expectSuccess[Float](
      new DoubleNode(1.5),
      1.5f
    )
    expectSuccess[Float](
      new DecimalNode(BigDecimal.valueOf(1.5).bigDecimal),
      1.5f
    )
    expectFailure[Float](
      new DoubleNode(Double.MaxValue),
      s"Input field: '${Double.MaxValue}' at path: 'field' cannot be parsed to expected type."
    )
    expectFailure[Float](
      new DecimalNode(BigDecimal.valueOf(Double.MaxValue).bigDecimal),
      s"Input field: '${BigDecimal.valueOf(Double.MaxValue).bigDecimal}' at path: 'field' cannot " +
          s"be parsed to expected type."
    )
  }

  @Test
  def testDouble(): Unit = {
    expectSuccess[Double](
      Factory.numberNode(1.5),
      1.5
    )
    expectFailure[Double](
      Factory.textNode("one point five"),
      "Input field: '\"one point five\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testBigDec(): Unit = {
    expectSuccess[BigDecimal](
      Factory.numberNode(1.5),
      BigDecimal(1.5)
    )
    expectFailure[BigDecimal](
      Factory.textNode("one point five"),
      "Input field: '\"one point five\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testSeq(): Unit = {
    expectSuccess[Seq[String]](
      JsonUtils.parseArrayString("""["one", "two", "three"]"""),
      Seq("one", "two", "three")
    )
    expectSuccess[Seq[Seq[String]]](
      JsonUtils.parseArrayString("""[["one one", "one two"],["two one", "two two"]]"""),
      Seq(Seq("one one", "one two"), Seq("two one", "two two"))
    )
    expectFailure[Seq[String]](
      Factory.textNode("string"),
      "Input field: '\"string\"' at path: 'field' cannot be parsed to expected type."
    )
    expectFailure[Seq[String]](
      JsonUtils.parseArrayString("""[1, 2, 3]"""),
      "Input field: '[1,2,3]' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testMap(): Unit = {
    expectSuccess[Map[String, String]](
      JsonUtils.parseObjectString(
        """{
          |  "k1":"v1",
          |  "k2":"v2"
          |}
        """.stripMargin),
      Map("k1" -> "v1", "k2" -> "v2")
    )
    expectSuccess[Map[String, Int]](
      JsonUtils.parseObjectString(
        """{
          |  "k1":1,
          |  "k2":2
          |}
        """.stripMargin),
      Map("k1" -> 1, "k2" -> 2)
    )
    expectFailure[Map[String, String]](
      JsonUtils.parseObjectString(
        """{
          |  "k1":1,
          |  "k2":2
          |}
        """.stripMargin),
      """Input field: '{"k1":1,"k2":2}' at path: 'field' cannot be parsed to expected type."""
    )
    expectFailure[Map[String, String]](
      Factory.textNode("string"),
      "Input field: '\"string\"' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testOption() {
    expectSuccess[Option[String]](
      Factory.nullNode(),
      None
    )
    expectSuccess[Option[String]](
      null,
      None
    )
    expectSuccess[Option[String]](
      Factory.textNode("string"),
      Some("string")
    )
    expectFailure[Option[String]](
      Factory.numberNode(1),
      "Input field: '1' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testEither() {
    expectSuccess[Either[String, Int]](
      Factory.textNode("string"),
      Left("string")
    )
    expectSuccess[Either[String, Int]](
      Factory.numberNode(1),
      Right(1)
    )
    expectFailure[Either[String, Int]](
      Factory.objectNode(),
      "Input field: '{}' at path: 'field' cannot be parsed to expected type."
    )
  }

  @Test
  def testTry() {
    def tryExpectSuccess[T: JsonField](
      input: JsonNode,
      output: T
    ) {
      val parser: JsonFieldParser[Try[T]] = JsonFieldParser(field)
      val json: JsonNode = makeJson(input)
      Assert.assertEquals(output, parser(json).get)
    }

    def tryExpectFailure[T: JsonField](
      input: JsonNode,
      error: String
    ) {
      val parser: JsonFieldParser[Try[T]] = JsonFieldParser(field)
      val json: JsonNode = makeJson(input)
      parser(json) match {
        case Failure(exception) => Assert.assertEquals(error, exception.getMessage)
        case Success(value) => Assert.fail(s"expected failure succeeded with output: $value")
      }
    }

    tryExpectSuccess[String](
      Factory.textNode("string"),
      "string"
    )
    tryExpectSuccess[Seq[Either[String, Int]]](
      JsonUtils.arrayNode(Seq(Factory.textNode("a"), Factory.textNode("b"), Factory.numberNode(3))),
      Seq(Left("a"), Left("b"), Right(3))
    )
    tryExpectFailure[String](
      Factory.numberNode(1),
      "Cannot extract '1' to expected type."
    )
    tryExpectFailure[Seq[Int]](
      JsonUtils.arrayNode(Seq(Factory.textNode("1"), Factory.textNode("2"), Factory.numberNode(3))),
      "Cannot extract '[\"1\",\"2\",3]' to expected type."
    )

    {
      val input: Try[JsonNode] = Failure(new Exception("error message"))
      val input2: Try[JsonNode] = Success(JsonUtils.textNode("a"))
      val parser: JsonFieldParser[Try[JsonNode]] = JsonFieldParser("field")
      val obj: ObjectNode = Factory.objectNode()
      try {
        parser.write(obj, input)
      } catch {
        case uoe: UnsupportedOperationException => Assert.assertEquals(
          uoe.getMessage,
          "May not insert a Try."
        )
      }
      try {
        parser.write(obj, input2)
      } catch {
        case uoe: UnsupportedOperationException => Assert.assertEquals(
          uoe.getMessage,
          "May not insert a Try."
        )
      }
    }
  }

  @Test
  def testComplex() {
    expectSuccess[Seq[Either[String, Int]]](
      Factory.arrayNode().add("one").add(2),
      Seq(Left("one"), Right(2))
    )
    expectSuccess[Map[String, Seq[Option[Either[String, Int]]]]](
      JsonUtils.parseObjectString(
        """{
          |  "a1":[null, "GENERIC", 3],
          |  "a2":["SPECIFIC", 2, null]
          |}
        """.stripMargin),
      Map(
        "a1" -> Seq(None, Some(Left("GENERIC")), Some(Right(3))),
        "a2" -> Seq(Some(Left("SPECIFIC")), Some(Right(2)), None)
      )
    )
  }

  // Uncomment this test to see the compile error behavior.
//  @Test
//  def testCompilationError(): Unit = {
//    // This gives a bad error message because the compiler cannot infer a type parameter for
//    // JsonFieldParser's apply method that fulfills the constraints imposed by the type class. It
//    // therefor sets the type to Nothing, which is a subclass of everything and then gives an
//    // error about ambiguous implicits between various types, none of which are Kiji.
//    val incorrectError: JsonFieldParser[Array[String]] = JsonFieldParser(field)
//
//    // This gives the desired error message about the type not being valid json because no type
//    // inference is required.
//    val correctError: JsonFieldParser[Array[String]] = JsonFieldParser[Array[String]](field)
//    // So does this because the inferred type has no constraints.
//    val correctErrorTwo = JsonFieldParser[Array[String]](field)
//  }
}

object TestJsonFieldParser {
  val Factory: JsonNodeFactory = JsonUtils.Mapper.getNodeFactory

  val field: String = "field"
  def makeJson(
      node: JsonNode
  ): JsonNode = {
    val obj: ObjectNode = Factory.objectNode()
    JsonUtils.setFieldAtPath(obj, field, node)
  }

  def expectSuccess[T: JsonField](
      input: JsonNode,
      output: T
  ): Unit = {
    val parser: JsonFieldParser[T] = JsonFieldParser(field)
    val json: JsonNode = makeJson(input)
    Assert.assertEquals(output, parser(json))
  }

  def expectFailure[T: JsonField](
      input: JsonNode,
      error: String
  ): Unit = {
    val parser: JsonFieldParser[T] = JsonFieldParser(field)
    val json: JsonNode = makeJson(input)
    try {
      parser(json)
      Assert.fail()
    } catch {
      case iae: IllegalArgumentException => Assert.assertTrue(iae.getMessage.contains(error))

    }
  }
}
