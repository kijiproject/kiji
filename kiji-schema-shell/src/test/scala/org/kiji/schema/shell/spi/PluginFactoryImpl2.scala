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

import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand

/**
 * An implementation of ParserPluginFactory that responds to the "second" module name.
 *
 * <p>This defines the SET X; command; it appends 'x' to a string and prints its value.</p>
 */
class PluginFactoryImpl2 extends ParserPluginFactory {
  override def getName(): String = {
    return "second"
  }

  override def create(env: Environment): ParserPlugin = {
    return new ParserImpl2(env)
  }

  class SetX(val env: Environment) extends DDLCommand {
    var x = ""
    override def exec(): Environment = {
      x = x + "x"
      echo("X is now " + x)
      return env
    }
  }

  class ParserImpl2(val env: Environment) extends ParserPlugin {
    override def command(): Parser[DDLCommand] = (
        "SET"~"X"~";" ^^ (_ => new SetX(env))
    )
  }
}
