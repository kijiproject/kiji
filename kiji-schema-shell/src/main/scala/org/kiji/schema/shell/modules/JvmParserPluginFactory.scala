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

package org.kiji.schema.shell.modules

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.shell.spi.HelpPlugin
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory
import org.kiji.schema.shell.util.ForkJvm

/**
 * A plugin for launching child JVMs.
 *
 * Adds the 'FORK JVM mainClass arg1 arg2...' syntax after the user runs "MODULE 'fork';"
 * to enable this module.
 */
@ApiAudience.Private
@ApiStability.Experimental
final class JvmParserPluginFactory extends ParserPluginFactory with HelpPlugin {
  override def getName(): String = "fork"

  override def create(env: Environment): ParserPlugin = {
    return new JvmParserPlugin(env)
  }

  override def helpText(): String = {
    return """Allows you to run Java subprocesses within the Kiji shell.
             |
             |The current classpath will be provided to the subprocess, along with
             |any jars specified by the USE JAR command.
             |
             |Syntax: FORK JVM 'com.example.MyMainClass' ['myarg1' 'myarg2' ...];
             |""".stripMargin
  }

  final class JvmParserPlugin(val env: Environment) extends ParserPlugin with DDLParserHelpers {
    /**
     * Parser plugin that matches the syntax:
     * FORK JVM 'mainClassName' [ arg1 'arg2 with spaces' arg3... ]
     */
    def command: Parser[DDLCommand] = (
      i("FORK") ~> i("JVM" ) ~> optionallyQuotedString ~ opt(rep(optionallyQuotedString)) ~ ";"
      ^^ { case mainClass ~ maybeArgv ~ _ => new UserJvmCommand(env, mainClass, maybeArgv) }
    )
  }

  /**
   * A command that executes a main method in a Java subprocess.
   *
   * The subprocess' classpath will include the current process' classpath, along with any
   * jars specified in the `libJars` element of the provided environment. `libJars` take
   * precedence over inherited classpath jars.
   *
   * @param env is the shell environment.
   * @param mainClass to execute.
   * @param maybeArgv is an optional list of arguments to the main class' main() method.
   */
  final class UserJvmCommand(val env: Environment, val mainClass: String,
      val maybeArgv: Option[List[String]]) extends DDLCommand with ForkJvm {

    /** POSIX exit status for success from a process. */
    private val exitSuccess: Int = 0

    override def exec(): Environment = {
      val argv: List[String] = maybeArgv.getOrElse(List[String]())
      val retCode: Int = forkJvm(env, mainClass, List(), argv)

      if (exitSuccess != retCode) {
        throw new DDLException("Child process exited with non-zero exit status " +
            retCode.toString())
      }

      return env
    }
  }
}
