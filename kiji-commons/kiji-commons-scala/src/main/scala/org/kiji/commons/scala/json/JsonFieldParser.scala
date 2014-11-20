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
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Function for reading or writing typed values from and to Json. Relies on a type class (JsonField)
 * to ensure that parsers can only be created for valid Json types.
 *
 * Example usage:
 *
 * {{{
 *   val parser: JsonFieldParser[Option[Seq[String]]] = JsonFieldParser("maybeStringList")
 *
 *   val maybeStringList: Option[Seq[String]] = parser(
 *     JsonUtils.parseObjectString(
 *       """{
 *         |  "maybeStringList" : ["a", "b", "c"]
 *         |  "ignored" : "this value does not affect the parser"
 *         |}
 *       """.stripMargin
 *     )
 *   )
 * }}}
 *
 * Tip: The above syntax is common but will cause a confusing compiler error if an invalid type is
 * specified in place of `Option[Seq[String]]`. Specifically the issue comes from the compiler's
 * inability to infer the correct type onto the the method invocation JsonFieldParser(String).
 * Normally the compiler would simply infer the type specified on the left side of the equals sign,
 * but if that type does not meet the type bound imposed by the JsonField type class it will search
 * for another type that is compatible with the left side type and fulfills the type class. If it
 * cannot find such a type it will infer `Nothing` as the type which results in an ambiguous
 * implicit error message instead of the desirable "type is not a valid JsonField" message. This can
 * be avoided by explicitly providing the type annotation on the JsonFieldParser method invocation.
 *
 * {{{
 *   val parser = JsonFieldParser[Option[Seq[String]]]("maybeStringList")
 * }}}
 *
 * For more details on the behavior of individual types see the scaladoc for JsonField.
 *
 * @param fieldPath Period delimited path at which to find the value to parse.
 * @tparam T Type of the value to parse.
 */
final case class JsonFieldParser[T](
    fieldPath: String
)(
    implicit tField: JsonField[T]
) extends (JsonNode => T) {
  /**
   * Read the value at `fieldPath` from the given JsonNode as an instance of type T.
   *
   * @param node JsonNode from which to read a value.
   * @return The value at `fieldPath` of the given JsonNode as an instance of type T.
   */
  override def apply(node: JsonNode): T = JsonFieldParser.read(fieldPath, node)

  /**
   * Read the value at `fieldPath` from the given JsonNode as an instance of type T.
   *
   * @param node JsonNode from which to read a value.
   * @return The value at `fieldPath` of the given JsonNode as an instance of type T.
   */
  def read(node: JsonNode): T = JsonFieldParser.read(fieldPath, node)

  /**
   * Write the given value to `fieldPath` in the given ObjectNode.
   *
   * @param node ObjectNode into which to write the value.
   * @param value Value to write into the given ObjectNode.
   */
  def write(node: ObjectNode, value: T): Unit = JsonFieldParser.write(fieldPath, node, value)
}

object JsonFieldParser {

  /**
   * Read a value of type `T` from the field at `path` in `node`
   *
   * @param path Period delimited path into `node` at which to find the value to read as type `T`.
   * @param node JsonNode in which to find the field to read as type `T`.
   * @param tField Implicit evidence that type `T` is a JsonField type.
   * @tparam T Type of the value to read from `node`.
   * @return The value found at `path` in `node`.
   */
  def read[T](
      path: String,
      node: JsonNode
  )(
      implicit tField: JsonField[T]
  ): T = {
    val field: JsonNode = JsonUtils.accessFieldAtPath(node, path)
    require(
      tField.canExtract(field),
      s"Input field: '${field}' at path: '${path}' cannot be parsed to expected type."
    )
    tField.extract(field)
  }

  /**
   * Write a value of type `T` at `path` in `node`.
   *
   * @param path Period delimited path at which to write `value` in `node`.
   * @param node ObjectNode in which to write `value`.
   * @param value Instance of type `T` to write into `node`.
   * @param tField Implicit evidence that type `T` is a JsonField type.
   * @tparam T Type of the value to write into `node`.
   */
  def write[T](
      path: String,
      node: ObjectNode,
      value: T
  )(
      implicit tField: JsonField[T]
  ): Unit = {
    JsonUtils.setFieldAtPath(node, path, tField.insert(value))
  }
}
