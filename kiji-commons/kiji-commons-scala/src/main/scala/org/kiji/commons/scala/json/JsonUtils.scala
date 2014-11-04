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

import java.io.InputStream
import java.math.BigInteger

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.base.Preconditions

object JsonUtils {
  /** ObjectMapper with which to read and write Json. */
  lazy val Mapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }

  /**
   * Checks if a JsonNode is a null reference or is a Json encoded null value.
   *
   * @param jsonNode JsonNode to check for nullity.
   * @return True if the given JsonNode is a null reference of a NullNode.
   */
  def isNull(
      jsonNode: JsonNode
  ): Boolean = {
    (jsonNode == null) || (jsonNode.getNodeType == JsonNodeType.NULL)
  }

  /**
   * Create a new BooleanNode containing the given Boolean.
   *
   * @param bool Boolean from which to create a new BooleanNode.
   * @return A new BooleanNode containing the given Boolean.
   */
  def booleanNode(
      bool: Boolean
  ): BooleanNode = {
    Mapper.getNodeFactory.booleanNode(bool)
  }

  /**
   * Create a TextNode containing the given String.
   *
   * If the input String contains Json it will still be read only as a String.
   *
   * @param string String from which to create a TextNode.
   * @return A new TextNode containing the given String.
   */
  def textNode(
      string: String
  ): TextNode = {
    Mapper.getNodeFactory.textNode(string)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param byte Byte value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      byte: Byte
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(byte)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param short Short value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      short: Short
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(short)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param int Int value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      int: Int
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(int)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param long Long value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      long: Long
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(long)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param bigInt BigInteger value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      bigInt: BigInteger
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(bigInt)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param float Float value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      float: Float
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(float)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param double Double value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      double: Double
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(double)
  }

  /**
   * Create a NumericNode containing the given number value.
   *
   * @param bigDecimal BigDecimal value from which to create a NumericNode.
   * @return A new NumericNode containing the given number.
   */
  def numberNode(
      bigDecimal: BigDecimal
  ): NumericNode = {
    Mapper.getNodeFactory.numberNode(bigDecimal.bigDecimal)
  }

  /**
   * Create an ObjectNode containing values from the given Map.
   *
   * @param kvs Optional Map of values to include in the ObjectNode.
   * @return A new ObjectNode containing values from the given Map.
   */
  def objectNode(
      kvs: Map[String, JsonNode] = Map()
  ): ObjectNode = {
    val obj: ObjectNode = Mapper.createObjectNode()
    kvs.foreach { kv: (String, JsonNode) =>
      val (key: String, value: JsonNode) = kv
      obj.put(key, value)
    }
    obj
  }

  /**
   * Create a Json ArrayNode from a sequence of JsonNodes.
   *
   * @param values Optional values from which to build an ArrayNode.
   * @return an ArrayNode containing the input sequence.
   */
  def arrayNode(
      values: Seq[JsonNode] = Seq()
  ): ArrayNode = {
    val an: ArrayNode = Mapper.createArrayNode()
    values.foreach { elem: JsonNode => an.add(elem) }
    an
  }

  /**
   * Read an InputStream into an JsonNode.
   *
   * @param stream InputStream from which to read a Json value.
   * @return A JsonNode containing the value encoded in the input stream.
   */
  def parseJsonStream(
      stream: InputStream
  ): JsonNode = {
    Mapper.readTree(stream)
  }

  /**
   * Read an InputStream into an ObjectNode.
   *
   * @param stream InputStream from which to read a Json value.
   * @return An ObjectNode containing the values encoded in the input stream.
   */
  def parseObjectStream(
      stream: InputStream
  ): ObjectNode = {
    val obj: JsonNode = parseJsonStream(stream)
    Preconditions.checkArgument(
      obj.isObject,
      s"Json Stream: '${obj}' is not a Json Object.": Any)
    obj.asInstanceOf[ObjectNode]
  }

  /**
   * Read an InputStream into an ArrayNode.
   *
   * @param stream InputStream from which to read a Json value.
   * @return An ArrayNode containing the values encoded in the input stream.
   */
  def parseArrayStream(
      stream: InputStream
  ): ArrayNode = {
    val array: JsonNode = parseJsonStream(stream)
    Preconditions.checkArgument(
      array.isArray,
      s"Json Stream: '${array}' is not a Json Object.": Any)
    array.asInstanceOf[ArrayNode]
  }

  /**
   * Read a Json String into a Jackson JsonNode.
   *
   * @param string to interpret as Json.
   * @return a Jackson JsonNode corresponding to the input String.
   */
  def parseJsonString(
      string: String
  ): JsonNode = {
    Mapper.readTree(string)
  }

  /**
   * Read a Json String into a Jackson ObjectNode.
   *
   * @param string to interpret as Json.
   * @return a Jackson ObjectNode corresponding to the input String.
   * @throws ClassCastException if the input Json is not an Object (e.g. it is an array).
   */
  def parseObjectString(
      string: String
  ): ObjectNode = {
    val obj: JsonNode = parseJsonString(string)
    Preconditions.checkArgument(
      obj.isObject,
      s"Json String: '${string}' is not a Json Object.": Any)
    obj.asInstanceOf[ObjectNode]
  }

  /**
   * Read a Json String into a Jackson ArrayNode.
   *
   * @param string to interpret as Json.
   * @return a Jackson ArrayNode corresponding to the input String.
   * @throws ClassCastException if the input Json is not an Array (e.g. it is an object).
   */
  def parseArrayString(
      string: String
  ): ArrayNode = {
    val array: JsonNode = parseJsonString(string)
    Preconditions.checkArgument(
      array.isArray,
      s"Json String: '${string}' is not a Json Array.": Any)
    array.asInstanceOf[ArrayNode]
  }

  /**
   * Converts a scala case class to json.
   *
   * @param value to convert to json.
   * @return a json string.
   */
  def toJson(value: Any): String = {
    Mapper.writeValueAsString(value)
  }

  /**
   * Converts a json string into a scala case class. Note: You must provide a type parameter here.
   *
   * @tparam T is the case class to read into.
   * @param json to convert to a scala case class.
   * @return a case class containing the data represented by the provided json.
   */
  def fromJson[T](json: String)(implicit manifest: Manifest[T]): T = {
    Mapper.readValue[T](json)(manifest)
  }

  /**
   * Flatten a sequence of ArrayNodes into a single ArrayNode, preserving order.
   *
   * @param arrays to flatten.
   * @return A single ArrayNode containing the contents of all input ArrayNodes in the same order.
   */
  def flattenArrays(
      arrays: Seq[ArrayNode]
  ): ArrayNode = {
    val an: ArrayNode = Mapper.createArrayNode()
    arrays.foreach { elems: ArrayNode => an.addAll(elems) }
    an
  }

  /**
   * Pretty print the input JsonNode.
   *
   * @param jsonNode to pretty print.
   * @return a String of pretty printed Json.
   */
  def prettyPrint(
      jsonNode: JsonNode
  ): String = {
    Mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode)
  }

  /**
   * Set the field of a JsonNode at a specified accessor path. Specified path may not be empty.
   * Recursively creates any necessary path components if they are not already present.
   *
   * @param objectNode in which to set a value.
   * @param pathString at which to set the value. Specified as a period separated list of accessors.
   * @param value to set at the given path.
   * @return The ObjectNode in which the field was being set.
   */
  def setFieldAtPath(
      objectNode: ObjectNode,
      pathString: String,
      value: JsonNode
  ): ObjectNode = {
    def recursiveHelper(
        recursiveObjectNode: ObjectNode,
        path: Array[String]
    ): ObjectNode = {
      if (path.size == 1) {
        recursiveObjectNode.set(path.head, value)
        objectNode
      } else {
        if (JsonUtils.isNull(recursiveObjectNode.get(path.head))) {
          // If the next node is null, create a new one and add it to the input.
          recursiveObjectNode.set(path.head, new ObjectNode(JsonNodeFactory.instance))
        }
        recursiveHelper(recursiveObjectNode.get(path.head).asInstanceOf[ObjectNode], path.tail)
      }
    }
    if (pathString.isEmpty) {
      sys.error("Cannot set field at an empty path.")
    } else if (pathString.endsWith(".")) {
      sys.error("Illegal path: '%s'. Ends with '.'.".format(pathString))
    } else {
      recursiveHelper(objectNode, pathString.split("\\."))
    }
  }

  /**
   * Read a field of a JsonNode by a specified accessor path.
   *
   * @param jsonNode from which to access a field.
   * @param pathString at which to find the field. Specified as a period separated list of
   *                   accessors. May not contain empty accessors (i.e. "a..b" is illegal). If empty
   *                   returns the input JsonNode.
   * @return the field of the given JsonNode at the given path. If the path is empty, returns the
   *     input node.
   * @throws IllegalArgumentException if the path is invalid of cannot be accessed in the JsonNode.
   */
  def accessFieldAtPath(
      jsonNode: JsonNode,
      pathString: String
  ): JsonNode = {
    def recursiveHelper(
        recursiveNode: JsonNode,
        path: Array[String]
    ): JsonNode = {
      if (path.size == 0) {
        recursiveNode
      } else {
        if (path.head.isEmpty) {
          sys.error("Illegal path: '%s'. Contains empty accessor.".format(pathString))
        }
        Preconditions.checkArgument(
          recursiveNode.isObject,
          "Cannot access field of a non-Object Json node: '%s'.",
          recursiveNode)
        val nextNode: JsonNode = recursiveNode.get(path.head)
        if (isNull(nextNode)) {
          Mapper.getNodeFactory.nullNode()
        } else {
          recursiveHelper(nextNode, path.tail)
        }
      }
    }
    if (pathString.isEmpty) {
      jsonNode
    } else if (pathString.endsWith(".")) {
      sys.error("Illegal path: '%s'. Ends with '.'.".format(pathString))
    } else {
      recursiveHelper(jsonNode, pathString.split("\\."))
    }
  }

  /**
   * Convert an ArrayNode into a Seq[JsonNode].
   *
   * @param jsonNode JsonNode to convert to a sequence. Must be an ArrayNode.
   * @return a Seq[JsonNode] containing the values from the input ArrayNode.
   */
  @deprecated("use JsonFieldParser[Seq[JsonNode]]")
  def extractArray(
      jsonNode: JsonNode
  ): Seq[JsonNode] = {
    Preconditions.checkArgument(JsonNodeType.ARRAY == jsonNode.getNodeType)
    jsonNode.elements().asScala.toList
  }


  /**
   * Read a JsonNode containing an array of Strings into a sequence of Strings.
   *
   * @param arrayNode from which to extract strings.
   * @return a sequence of strings representing the data contained in the input ArrayNode.
   */
  @deprecated("use JsonFieldParser[Seq[String]]")
  def extractStringArray(
      arrayNode: JsonNode
  ): Seq[String] = {
    arrayNode.getNodeType match {
      case JsonNodeType.ARRAY => arrayNode.elements().asScala.map { arrayElem: JsonNode =>
        arrayElem.getNodeType match {
          case JsonNodeType.STRING => arrayElem.asText()
          case _ => sys.error("Non-string node found in array in extractStringArray().")
        }
      }.toList
      case _ => sys.error("Non-array node passed to extractStringArray().")
    }
  }

  /**
   * Get a String or array of Strings from a JsonNode. Fails if the node is neither Array nor Text.
   *
   * @param jsonNode from which to get a String or array of Strings.
   * @return The sequence of Strings contained in the input node. May be size 0 or greater.
   */
  @deprecated("use JsonFieldParser[Either[String, Seq[String]]]")
  def getStringOrArrayOfString(
      jsonNode: JsonNode
  ): Seq[String] = {
    jsonNode.getNodeType match {
      case JsonNodeType.ARRAY => JsonUtils.extractStringArray(jsonNode)
      case JsonNodeType.STRING => Seq(jsonNode.asText())
      case _ => sys.error("Neither array nor text node passed to getStringOrArrayOfString().")
    }
  }
}
