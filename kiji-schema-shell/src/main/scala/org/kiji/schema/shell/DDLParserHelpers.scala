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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.Inheritance

import org.kiji.schema.avro.HashType
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat
import org.kiji.schema.KConstants

import org.kiji.schema.shell.ddl._
import org.kiji.schema.shell.ddl.CompressionTypeToken._
import org.kiji.schema.shell.ddl.LocalityGroupPropName._
import org.kiji.schema.shell.ddl.key._
import org.kiji.schema.shell.ddl.key.RowKeyElemType._
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory

import org.kiji.schema.util.KijiNameValidator

import scala.util.parsing.combinator._

/**
 * Parser for a kiji-schema DDL command.
 */
@ApiAudience.Framework
@Inheritance.Extensible
trait DDLParserHelpers extends JavaTokenParsers {

  /**
   * Matches a string enclosed by 'single quotes', that may contain escapes.
   *
   * @return a parser tha matches the above and returns the string contained within
   * the quotes, with the enclosing single-quote-marks removed and any escape character
   * sequences converted to the actual characters they represent.
   */
  def singleQuotedString: Parser[String] = (
    // Regex adapted from http://blog.stevenlevithan.com/archives/match-quoted-string
    """'(?:\\?+.)*?'""".r
    ^^ (strWithEscapes => unescape(strWithEscapes.substring(1, strWithEscapes.length - 1)))
  )

  /** An identifier that is optionally 'single quoted' */
  def optionallyQuotedString: Parser[String] = (
      ident | singleQuotedString
  )

  /**
   * Matches table, family, etc. names are strings that are optionally 'single quoted',
   * and must match the Kiji name restrictions.
   **/
  def validatedNameFromOptionallyQuotedString: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateLayoutName(name); name })
  )

  /**
   * Matches a legal Kiji instance name.
   */
  def instanceName: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateKijiName(name); name })
  )

  /** Matches a legal Kiji table name. */
  def tableName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji row key component name. */
  def rowKeyElemName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji locality group name. */
  def localityGroupName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji column family name. */
  def familyName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Matches a legal Kiji column qualifier name. */
  def qualifier: Parser[String] = validatedNameFromOptionallyQuotedString

  /** Column names take the form info:foo, 'info':foo, info:'foo', or 'info':'foo' */
  def colName: Parser[ColumnName] = (
      validatedNameFromOptionallyQuotedString~":"~validatedNameFromOptionallyQuotedString
      ^^ ({case ~(~(family, _), qualifier) => new ColumnName(family, qualifier) })
  )

  def bool: Parser[Boolean] = (
      i("TRUE") ^^ (_ => true)
    | i("FALSE") ^^ (_ => false)
  )

  /**
   * Matches an integer. The strings INFINITY and FOREVER are both synonyms for Int.MaxValue.
   */
  def intValue: Parser[Int] = (
      wholeNumber ^^ (x => x.toInt)
    | i("INFINITY") ^^ (_ => Int.MaxValue)
    | i("FOREVER") ^^ (_ => Int.MaxValue)
  )

  /**
   * @param s the string to recognize in a case-insensitive fashion.
   * @return a parser which matches the word in 's' in a case-insensitive fashion.
   */
  protected def i(s: String): Parser[String] = {
    val sb = new StringBuilder
    s.foreach { ch =>
      sb.append('[')
      sb.append(ch.toUpper)
      sb.append(ch.toLower)
      sb.append(']')
    }
    regex(sb.toString().r)
  }

  /**
   * Given a string that contains \\ and \', convert these sequences to \ and ' respectively.
   *
   * @param s a string that may contain escape sequences to protect characters in
   * a 'single quoted string' matched by a parser.
   * @return the same string in 's' with the escapes converted to their true character
   * representations.
   */
  def unescape(s: String): String = {
    val sb = new StringBuilder
    var i: Int = 0;
    while (i < s.length) {
      if (i < s.length - 1 && s.charAt(i) == '\\') {
        s.charAt(i + 1) match {
          case '\\' => { sb.append("\\"); i += 1 }
          case '\'' => { sb.append("\'"); i += 1 }
          case 'n' => { sb.append("\n"); i += 1 }
          case 't' => { sb.append("\t"); i += 1 }
          case c => { sb.append(c); i += 1 } // Everything else escapes to itself.
        }
      } else {
        sb.append(s.charAt(i))
      }

      i += 1
    }

    return sb.toString()
  }
}
