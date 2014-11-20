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

import java.util.{Map => JMap}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Type class providing operations for reading and writing values from json.
 */
@implicitNotFound("Specified type: '${T}' is not a valid json field.")
sealed trait JsonField[T] {
  def insert(t: T): JsonNode
  def extract(node: JsonNode): T
  def canExtract(node: JsonNode): Boolean
}

/**
 * Contains implicit evidence that various types can be read from and written to json.
 *
 * Implementations are:
 *   JsonNode
 *   ObjectNode
 *   ArrayNode
 *   Boolean
 *   String
 *   Short
 *   Int
 *   Long
 *   BigInt
 *   Float
 *   Double
 *   BigDecimal
 *   Seq[T: JsonField]
 *   Map[String, T: JsonField]
 *   Option[T: JsonField]
 *   Either[T: JsonField, U: JsonField]
 *   Try[T: JsonField]
 *
 * Option interprets a non-existent field and a null literal the same.
 * Either favors the Left option if both are valid. e.g. Either[Long, Int] will always be a
 *     Left(Long) or an error.
 * Try is able to extract any value. If the parameterized type class implementation is also able to
 *     extract a given value, returns Success containing the extracted value. If the parameterized
 *     type is not able to extract the given value, returns Failure. When inserting, a Success is
 *     unwrapped and its contained value inserted, while a Failure becomes a TextNode containing the
 *     message and stack trace of the exception.
 */
object JsonField {
  private[this] val Factory: JsonNodeFactory = JsonUtils.Mapper.getNodeFactory

  implicit val json: JsonField[JsonNode] = new JsonField[JsonNode] {
    override def insert(t: JsonNode): JsonNode = t
    override def extract(node: JsonNode): JsonNode = node
    override def canExtract(node: JsonNode): Boolean = node != null
  }

  implicit val obj: JsonField[ObjectNode] = new JsonField[ObjectNode] {
    override def insert(t: ObjectNode): JsonNode = t
    override def extract(node: JsonNode): ObjectNode = node.asInstanceOf[ObjectNode]
    override def canExtract(node: JsonNode): Boolean = node.isObject
  }

  implicit val array: JsonField[ArrayNode] = new JsonField[ArrayNode] {
    override def insert(t: ArrayNode): JsonNode = t
    override def extract(node: JsonNode): ArrayNode = node.asInstanceOf[ArrayNode]
    override def canExtract(node: JsonNode): Boolean = node.isArray
  }

  implicit val boolean: JsonField[Boolean] = new JsonField[Boolean] {
    override def insert(t: Boolean): JsonNode = Factory.booleanNode(t)
    override def extract(node: JsonNode): Boolean = node.asBoolean
    override def canExtract(node: JsonNode): Boolean = node.isBoolean
  }

  implicit val string: JsonField[String] = new JsonField[String] {
    override def insert(t: String): JsonNode = Factory.textNode(t)
    override def extract(node: JsonNode): String = node.asText
    override def canExtract(node: JsonNode): Boolean = node.isTextual
  }

  implicit val short: JsonField[Short] = new JsonField[Short] {
    override def insert(t: Short): JsonNode = Factory.numberNode(t)
    override def extract(node: JsonNode): Short = node.shortValue()
    override def canExtract(node: JsonNode): Boolean = node.isShort ||
        (node.isInt && node.intValue() <= Short.MaxValue && node.intValue() >= Short.MinValue)
  }

  implicit val int: JsonField[Int] = new JsonField[Int] {
    override def insert(t: Int): JsonNode = Factory.numberNode(t)
    override def extract(node: JsonNode): Int = node.asInt
    override def canExtract(node: JsonNode): Boolean = node.isInt || node.isShort
  }

  implicit val long: JsonField[Long] = new JsonField[Long] {
    override def insert(t: Long): JsonNode = Factory.numberNode(t)
    override def extract(node: JsonNode): Long = node.asLong
    override def canExtract(node: JsonNode): Boolean = node.isLong || node.isInt || node.isShort
  }

  implicit val bigInt: JsonField[BigInt] = new JsonField[BigInt] {
    override def insert(t: BigInt): JsonNode = Factory.numberNode(t.bigInteger)
    override def extract(node: JsonNode): BigInt = node.bigIntegerValue()
    override def canExtract(node: JsonNode): Boolean =
        node.isBigInteger || node.isLong || node.isInt || node.isShort
  }

  implicit val float: JsonField[Float] = new JsonField[Float] {
    private val max: BigDecimal = BigDecimal(Float.MaxValue)
    private val min: BigDecimal = BigDecimal(Float.MinValue)
    override def insert(t: Float): JsonNode = Factory.numberNode(t)
    override def extract(node: JsonNode): Float = node.floatValue()
    override def canExtract(node: JsonNode): Boolean = node.isFloat ||
        (node.isDouble &&
            node.doubleValue() <= Float.MaxValue &&
            node.doubleValue() >= Float.MinValue) ||
        (node.isFloatingPointNumber &&
            node.decimalValue().compareTo(max.bigDecimal) <= 0 &&
            node.decimalValue().compareTo(min.bigDecimal) >= 0)
  }

  implicit val double: JsonField[Double] = new JsonField[Double] {
    override def insert(t: Double): JsonNode = Factory.numberNode(t)
    override def extract(node: JsonNode): Double = node.asDouble
    override def canExtract(node: JsonNode): Boolean = {
      node.isDouble || node.isFloat || node.isInt || node.isShort
    }
  }

  implicit val bigDec: JsonField[BigDecimal] = new JsonField[BigDecimal] {
    override def insert(t: BigDecimal): JsonNode = Factory.numberNode(t.bigDecimal)
    override def extract(node: JsonNode): BigDecimal = node.decimalValue()
    override def canExtract(node: JsonNode): Boolean = node.isFloatingPointNumber
  }

  implicit def seq[T](
      implicit tField: JsonField[T]
  ): JsonField[Seq[T]] = new JsonField[Seq[T]] {
    override def insert(t: Seq[T]): JsonNode =
        JsonUtils.arrayNode(t.map { elem: T => tField.insert(elem) })
    override def extract(node: JsonNode): Seq[T] =
        JsonUtils.extractArray(node).map { elem: JsonNode => tField.extract(elem) }
    override def canExtract(node: JsonNode): Boolean =
        node.isArray && node.elements().asScala.forall { elem: JsonNode => tField.canExtract(elem) }
  }

  implicit def map[T](
      implicit tField: JsonField[T]
  ): JsonField[Map[String, T]] = new JsonField[Map[String, T]] {
    override def insert(t: Map[String, T]): JsonNode = {
      val obj: ObjectNode = Factory.objectNode()
      t.foreach { kv: (String, T) =>
        val (key: String, value) = kv
        obj.put(key, tField.insert(value))
      }
      obj
    }
    override def extract(node: JsonNode): Map[String, T] = {
      node.fields().asScala.map { entry: JMap.Entry[String, JsonNode] =>
        (entry.getKey, tField.extract(entry.getValue))
      }.toMap
    }
    override def canExtract(node: JsonNode): Boolean = {
      node.isObject &&
          node.fields().asScala.forall { entry: JMap.Entry[String, JsonNode] =>
            tField.canExtract(entry.getValue)
          }
    }
  }

  implicit def option[T](
      implicit tField: JsonField[T]
  ): JsonField[Option[T]] = new JsonField[Option[T]] {
    override def insert(t: Option[T]): JsonNode = {
      t match {
        case Some(value) => tField.insert(value)
        case None => Factory.nullNode
      }
    }
    override def extract(node: JsonNode): Option[T] = {
      if (JsonUtils.isNull(node)) {
        None
      } else {
        Some(tField.extract(node))
      }
    }
    override def canExtract(node: JsonNode): Boolean = {
      JsonUtils.isNull(node) || tField.canExtract(node)
    }
  }

  implicit def either[T, U](
      implicit tField: JsonField[T],
      uField: JsonField[U]
  ): JsonField[Either[T, U]] = new JsonField[Either[T, U]] {
    override def insert(either: Either[T, U]): JsonNode = either match {
      case Left(t) => tField.insert(t)
      case Right(u) => uField.insert(u)
    }
    override def extract(node: JsonNode): Either[T, U] = {
      if (tField.canExtract(node)) {
        Left(tField.extract(node))
      } else {
        Right(uField.extract(node))
      }
    }
    override def canExtract(node: JsonNode): Boolean = {
      tField.canExtract(node) || uField.canExtract(node)
    }
  }

  implicit def tri[T](
      implicit tField: JsonField[T]
  ): JsonField[Try[T]] = new JsonField[Try[T]] {
    override def insert(t: Try[T]): JsonNode = {
      throw new UnsupportedOperationException("May not insert a Try.")
    }
    override def extract(node: JsonNode): Try[T] = if (tField.canExtract(node)) {
      Success(tField.extract(node))
    } else {
      Failure(new IllegalArgumentException(s"Cannot extract '$node' to expected type."))
    }
    override def canExtract(node: JsonNode): Boolean = true
  }
}
