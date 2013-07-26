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

package org.kiji.schema.shell.spi

import scala.util.parsing.combinator._

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand

/**
 * Plugin SPI that specifies a parser for new commands that should extend
 * the Kiji Schema shell.
 *
 * A `ParserPlugin` is created through a {@link ParserPluginFactory}. To
 * extend this interface, you must implement a `ParserPluginFactory` that
 * creates your `ParserPlugin` implementation.
 *
 * <p>
 * The `command()` method returns a parser that returns a `DDLCommand` instance
 * that implements the specified command. Your `command()` method can use the parser
 * "or" combinator (`|`) to match multiple commands, each of which returns a different
 * DDLCommand implementation.
 * </p>
 *
 * <p>Your parser must match a statement which includes a trailing semicolon (`';'`)
 * character.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait ParserPlugin extends RegexParsers {

  /**
   * The environment in which the parser and its generated AST components will operate.
   *
   * <p>This must return the same Environment object every time; this Environment will
   * be supplied to your ParserPlugin via {@link ParserPluginFactory#create}.</p>
   *
   * @return the environment in which the parser and its generated AST components operate.
   */
  def env(): Environment

  /**
   * Returns a parser that recognizes all legal statements in the plugin's language extension.
   *
   * <p>The input string to this parser will be the user's command as entered at the console
   * or other script input source. This must match a trailing semicolon. You may
   * match multiple different commands by using the OR ('|') parser combinator.
   *
   * @return a Parser that matches input and returns some DDLCommand subclass.
   */
  def command(): Parser[DDLCommand]
}
