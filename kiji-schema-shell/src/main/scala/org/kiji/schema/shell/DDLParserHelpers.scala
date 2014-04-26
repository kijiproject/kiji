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
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.KijiInvalidNameException

import org.kiji.schema.shell.ddl._

import org.kiji.schema.util.KijiNameValidator

import scala.util.parsing.combinator._

/**
 * Parsers for elements of grammar common to many kiji-schema DDL commands. These
 * grammar elements (for example, table names, column names, etc) may be of use to
 * authors of additional parser plugins that extend the language.
 *
 * <p>Elements like `optionallyQuotedString` should have somewhat obvious
 * application; this matcher is also used in a more semantically-meaningful form
 * in the case of matchers like `instanceName`, `tableName`, etc.</p>
 *
 * <p>Case-insensitive strings can be matched with `i("SOMESTR")`;
 * this will match input like `SOMESTR`, `somestr`, and `SoMeStR`, etc.
 * Uniform definitions of boolean values and integer values are also provided.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait DDLParserHelpers extends JavaTokenParsers {

  /**
   * Matches a string enclosed by 'single quotes', that may contain escapes.
   *
   * @return a parser that matches the above and returns the string contained within
   *     the quotes, with the enclosing single-quote-marks removed and any escape character
   *     sequences converted to the actual characters they represent.
   */
  def singleQuotedString: Parser[String] = (
    """'(?:[^'\\]|\\.)*'""".r
    ^^ (strWithEscapes => unescape(strWithEscapes.substring(1, strWithEscapes.length - 1)))
  )

  /** @return a matcher for an identifier that is optionally 'single quoted' */
  def optionallyQuotedString: Parser[String] = (
      ident | singleQuotedString
  )

  /**
   * @return a matcher for table, family, etc. names are strings that are optionally
   *     'single quoted', and must match the Kiji name restrictions.
   **/
  def validatedNameFromOptionallyQuotedString: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateLayoutName(name); name })
  )

  /**
   * @return a matcher for a legal Kiji instance name.
   */
  def instanceName: Parser[String] = (
    optionallyQuotedString ^^ (name => { KijiNameValidator.validateKijiName(name); name })
  )

  /** @return a matcher for a legal Kiji table name. */
  def tableName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** @return a matcher for a legal Kiji row key component name. */
  def rowKeyElemName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** @return a matcher for a legal Kiji locality group name. */
  def localityGroupName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** @return a matcher for a legal Kiji column family name. */
  def familyName: Parser[String] = validatedNameFromOptionallyQuotedString

  /** @return a matcher for a legal Kiji column qualifier name. */
  def qualifier: Parser[String] = validatedNameFromOptionallyQuotedString

  /**
   * @return a matcher for column names, which take the form info:foo, 'info':foo,
   *     info:'foo', or 'info':'foo' */
  def colName: Parser[ColumnName] = (
      validatedNameFromOptionallyQuotedString~":"~validatedNameFromOptionallyQuotedString
      ^^ ({case ~(~(family, _), qualifier) => new ColumnName(family, qualifier) })
  )

  /** @return a matcher for a boolean literal value. */
  def bool: Parser[Boolean] = (
      i("TRUE") ^^ (_ => true)
    | i("FALSE") ^^ (_ => false)
  )

  /**
   * @return a matcher for an integer. The strings INFINITY and FOREVER are both synonyms
   *     for Int.MaxValue.
   */
  def intValue: Parser[Int] = (
      wholeNumber ^^ (x => x.toInt)
    | i("INFINITY") ^^ (_ => Int.MaxValue)
    | i("FOREVER") ^^ (_ => Int.MaxValue)
  )

  /** @return a matcher for an intValue or 'NULL', and returns an Option[Int]. */
  def intValueOrNull: Parser[Option[Int]] = (
      intValue ^^ (x => Some(x))
    | i("NULL") ^^ (_ => None)
  )

  /**
   * @return a matcher for a long-valued integer. The strings INFINITY and FOREVER are
   *     both synonyms for Int.MaxValue.
   */
  def longValue: Parser[Long] = (
      wholeNumber ^^ (x => x.toLong)
    | i("INFINITY") ^^ (_ => Int.MaxValue.toLong)
    | i("FOREVER") ^^ (_ => Int.MaxValue.toLong)
  )

  /** @return a matcher for a longValue or 'NULL', and returns an Option[Long]. */
  def longValueOrNull: Parser[Option[Long]] = (
      longValue ^^ (x => Some(x))
    | i("NULL") ^^ (_ => None)
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
   *     a 'single quoted string' matched by a parser.
   * @return the same string in 's' with the escapes converted to their true character
   *     representations.
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
