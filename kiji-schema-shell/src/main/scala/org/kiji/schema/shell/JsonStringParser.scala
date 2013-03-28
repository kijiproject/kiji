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

package org.kiji.schema.shell

import scala.collection.JavaConversions
import scala.util.parsing.combinator._

import org.apache.commons.lang3.StringUtils

import org.kiji.annotations.ApiAudience

/**
 * Parser that matches a JSON value (scalar, record, literal, or array) and returns
 * a valid JSON string expression for this value. The value is not converted into a
 * Java value of the appropriate type; the source JSON is used.
 */
@ApiAudience.Private
trait JsonStringParser extends JavaTokenParsers {
  def jsonRecord: Parser[String] = (
      "{"~> repsep(jsonAttr, ",") <~"}"
      ^^ ({ attrs:List[String] => "{" + toCommaSeparatedStr(attrs) + "}" })
  )

  def jsonArray: Parser[String] = (
      "["~> repsep(jsonValue, ",") <~"]"
      ^^ ({ vals:List[String] => "[" + toCommaSeparatedStr(vals) + "]" })
  )

  def jsonAttr: Parser[String] =
      stringLiteral~":"~jsonValue ^^ { case name~":"~value => name + ": " + value }

  def jsonValue: Parser[String] = (
      jsonRecord
    | jsonArray
    | stringLiteral
    | floatingPointNumber
    | "null"
    | "true"
    | "false"
  )

  /**
   * Convert a list of strings into a single string with elements separated by commas.
   */
  private def toCommaSeparatedStr(vals:List[String]): String = {
    StringUtils.join(JavaConversions.seqAsJavaList(vals), ",")
  }
}
