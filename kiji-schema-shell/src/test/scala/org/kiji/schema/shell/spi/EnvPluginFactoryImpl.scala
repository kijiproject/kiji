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

import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand

/**
 * An implementation of ParserPluginFactory that includes EnvironmentPlugin.
 *
 * <p>This defines two commands: ENV key val; (saves key=val) and ENV key (prints val for key).</p>
 */
class EnvPluginFactoryImpl extends ParserPluginFactory with EnvironmentPlugin[Map[String, String]] {
  override def getName(): String = {
    return "env"
  }

  override def create(env: Environment): ParserPlugin = {
    return new EnvPlugin(env)
  }

  override def createExtensionState(): Map[String, String] = {
    // Create a blank map for our initial state data.
    return Map[String, String]()
  }

  final class SetEnvCommand(val env: Environment, val k: String, val v: String) extends DDLCommand {
    override def exec(): Environment = {
      val curState: Map[String, String] = getExtensionState()
      return setExtensionState(curState + (k -> v))
    }
  }

  final class GetEnvCommand(val env: Environment, val k: String) extends DDLCommand {
    override def exec(): Environment = {
      val curState: Map[String, String] = getExtensionState()
      echo(k + " = " + curState(k))
      return env
    }
  }

  final class EnvPlugin(val env: Environment) extends ParserPlugin with DDLParserHelpers {
    override def command(): Parser[DDLCommand] = (
        "ENV"~>singleQuotedString~singleQuotedString~";" ^^ {
          // ENV k v; # Sets k = v in state.
          case key ~ value ~ _ => new SetEnvCommand(env, key, value)
        }
      | "ENV"~>singleQuotedString~";" ^^ {
          // ENV k; # Prints value saved for key 'k'.
          case key ~ _ => new GetEnvCommand(env, key)
        }
    )
  }
}
