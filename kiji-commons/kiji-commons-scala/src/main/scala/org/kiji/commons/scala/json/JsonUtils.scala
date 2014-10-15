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
import com.fasterxml.jackson.databind.node.BigIntegerNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.MissingNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.POJONode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.common.base.Preconditions
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer

import org.kiji.commons.FromJson
import org.kiji.commons.ToJson

object JsonUtils {
  /** ObjectMapper with which to read and write Json. */
  lazy val Mapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper
  }

  /**
   * Compare two Json nodes for equality.
   *
   * Differs from built in node.equals(node2) in the handling of number nodes with different types.
   * Jackson's built in equals methods require type equality in addition to value equality, which
   * differs from the json spec which makes no distinction between types of numbers. This method
   * compares number nodes by value only.
   *
   * Behavior is undefined if input nodes include integral number values outside the bounds of Long
   * or floating point number values outside the bounds of Double.
   *
   * @param left First node to compare for equality.
   * @param right Second node to compare for equality.
   * @return Whether the two input nodes are equal to each other.
   */
  def equals(left: JsonNode, right: JsonNode): Boolean = {
    if (left.eq(right)) {
      // left and right are the same JVM object; they are necessarily equal.
      true
    } else if (left == null || right == null) {
      // left and right are not the same JVM object and one is null; they are necessarily not equal.
      false
    } else {
      // left and right are not the same JVM object and neither is null; compare their values.
      left match {
        // Most node types compare fine, so we use their built in equals methods.
        case l: TextNode => l.equals(right)
        case l: BooleanNode => l.equals(right)
        case l: NullNode => l.equals(right)
        case l: POJONode => l.equals(right)
        case l: MissingNode => l.equals(right)
        // TODO: Investigate how BinaryNodes compare since they are represented as Strings in Json.
        case l: BinaryNode => l.equals(right)
        // Numbers require special comparison logic to ensure that Long(5) equals Int(5).
        case l: NumericNode => {
          right match {
            case r: NumericNode => numbersEqual(l, r)
            case _ => false
          }
        }
        // Collection nodes built in comparison logic is correct, but must be rewritten so that the
        // fixed number comparisons apply during recursive calls.
        case l: ArrayNode => right match {
          case r: ArrayNode => {
            if (l.size() != r.size()) {
              false
            } else {
              val lElements: Iterator[JsonNode] = l.elements().asScala
              val rElements: Iterator[JsonNode] = r.elements().asScala
              lElements.zip(rElements).forall { pair: (JsonNode, JsonNode) =>
                val (lElem, rElem) = pair
                equals(lElem, rElem)
              }
            }
          }
          case _ => false
        }
        case l: ObjectNode => right match {
          case r: ObjectNode => {
            if (l.size() != r.size()) {
              false
            } else {
              val lFields: Set[String] = l.fieldNames().asScala.toSet
              val rFields: Set[String] = r.fieldNames().asScala.toSet
              (lFields == rFields) && lFields.forall { field: String =>
                val lElem: JsonNode = l.get(field)
                val rElem: JsonNode = r.get(field)
                equals(lElem, rElem)
              }
            }
          }
          case _ => false
        }
      }
    }
  }

  private val LongMax: BigInteger = BigInt(Long.MaxValue).bigInteger
  private val LongMin: BigInteger = BigInt(Long.MinValue).bigInteger

  /**
   * Check if a NumericNode's value fits within a Long.
   *
   * Input is expected to be an integral number.
   *
   * @param number NumericNode to bounds check.
   * @return Whether the input value can be represented as a Long.
   */
  private def fitsInLong(number: NumericNode): Boolean = {
    require(number.isIntegralNumber)
    number match {
      case big: BigIntegerNode => {
        big.bigIntegerValue().compareTo(LongMax) <= 0 &&
            big.bigIntegerValue().compareTo(LongMin) >= 0
      }
      case _ => true // All other cases fit in long.
    }
  }

  private val DoubleMax: java.math.BigDecimal = BigDecimal(Double.MaxValue).bigDecimal
  private val DoubleMin: java.math.BigDecimal = BigDecimal(Double.MinValue).bigDecimal

  /**
   * Check if a NumericNode's value fits within a Double.
   *
   * Input is expected to be a floating point number.
   *
   * Does not check for precision of decimal numbers. A DecimalNode containing a value smaller than
   * Double.MaxValue, but more precise than can be represented by a Double will pass.
   *
   * @param number NumericNode to bounds check.
   * @return Whether the input value can be represented as a Double.
   */
  private def fitsInDouble(number: NumericNode): Boolean = {
    require(number.isFloatingPointNumber)
    number match {
      case big: DecimalNode => {
        big.decimalValue().compareTo(DoubleMax) <= 0 &&
            big.decimalValue().compareTo(DoubleMin) >= 0
      }
      case _ => true // All other cases fit in double.
    }
  }

  /**
   * Helper for equals which compares two NumericNodes by value, ignoring type.
   *
   * Does not work with integral number values outside the bounds of Long or floating point values
   * outside the bounds of Double.
   *
   * @param left First numeric node to check for equality.
   * @param right Second numeric node to check for equality.
   * @return Whether the value of the input nodes are equal.
   */
  private def numbersEqual(left: NumericNode, right: NumericNode): Boolean = {
    if (left.isIntegralNumber) {
      require(fitsInLong(left), s"Number out of bounds '$left'.")
      if (right.isIntegralNumber) {
        require(fitsInLong(right), s"Number out of bounds '$right'.")
        numbersEqual(left.asLong, right.asLong)
      } else {
        require(fitsInDouble(right), s"Number out of bounds '$right'.")
        numbersEqual(left.asLong, right.asDouble)
      }
    } else {
      require(fitsInDouble(left), s"Number out of bounds '$left'.")
      if (right.isIntegralNumber) {
        require(fitsInLong(right), s"Number out of bounds '$right'.")
        numbersEqual(left.asDouble, right.asLong)
      } else {
        require(fitsInDouble(right), s"Number out of bounds '$right'.")
        numbersEqual(left.asDouble, right.asDouble)
      }
    }
  }
  private def numbersEqual(left: Long, right: Long): Boolean = left == right
  private def numbersEqual(left: Long, right: Double): Boolean = left == right
  private def numbersEqual(left: Double, right: Long): Boolean = left == right
  private def numbersEqual(left: Double, right: Double): Boolean = left == right

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

  /**
   * Parses a json node into an avro object given an avro schema. Call this method with the
   * following syntax:
   *
   * val avro: GeneratedType = JsonUtils.fromJson(node, GeneratedType.getClassSchema)
   *
   * @param jsonNode The json node to be parsed.
   * @param schema The avro schema to be used to parse the json node.
   * @tparam T The type that is expected to be returned.
   * @return The extracted avro object cast to type T.
   */
  def fromJson[T](jsonNode: JsonNode, schema: Schema): T = {
    FromJson.fromJsonNode(jsonNode, schema).asInstanceOf[T]
  }

  /**
   * Converts an avro specific record to a json node.
   *
   * @param avro The avro record to be converted.
   * @tparam T The type of the avro record.
   * @return A constructed json node.
   */
  def toJson[T <: GenericContainer](avro: T): JsonNode = {
    ToJson.toJsonNode(avro, avro.getSchema)
  }

  // Pimp my class pattern to give avro records a .toJson method and json nodes a .toAvro method.
  /**
   * Implicitly grants avro records a .toJson method.
   *
   * @param avro The avro record.
   * @tparam T The type of the avro record.
   */
  implicit class AvroToJson[T <: GenericContainer](avro: T) {
    /**
     * Creates a json node from the avro record.
     *
     * @return A json node.
     */
    def toJson: JsonNode = JsonUtils.toJson[T](avro)
  }

  /**
   * Implicitly grants json nodes a toAvro method.
   *
   * @param node The json node to be converted.
   */
  implicit class JsonToAvro(node: JsonNode) {
    /**
     * Converts a json node into an avro object with the given type and schema.
     *
     * @param schema The schema of the avro record.
     * @tparam T The type of the resulting avro object.
     * @return The avro record resulting from the conversion.
     */
    def toAvro[T](schema: Schema): T = fromJson(node, schema)
  }
}
