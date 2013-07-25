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

package org.kiji.schema.shell.util

import scala.util.parsing.combinator._

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.shell.spi._

/**
 * A plugin implementation that defines a command that uses the ForkJvm trait.
 *
 * <p>This defines one command: ADDFOUR; that forks a JVM that does some math, and
 * checks that its result is correct.</p>
 */
class ForkTestPluginFactoryImpl extends ParserPluginFactory {
  override def getName(): String = {
    return "addfour"
  }

  override def create(env: Environment): ParserPlugin = {
    return new ForkTestPlugin(env)
  }

  final class AddFourCommand(val env: Environment) extends DDLCommand with ForkJvm {
    override def exec(): Environment = {
      // This runs the PlusFourChild program to calculate 12 + 4. The retcode should
      // be 16. This method throws a DDLException if that's not the case.

      val inputArg: String = "12"
      val expectedResult: Int = 16
      val mainClass: String = new PlusFourChild().getClass().getName()

      System.out.println("Launching main class: " + mainClass)
      val retCode: Int = forkJvm(env, mainClass, List(), List(inputArg))
      if (expectedResult != retCode) {
        throw new DDLException("Expected return code " + expectedResult + " but got " + retCode)
      }

      System.out.println("Returned with correct result code: " + retCode)
      return env
    }
  }

  final class ForkTestPlugin(val env: Environment) extends ParserPlugin {
    override def command(): Parser[DDLCommand] = (
        "ADDFOUR"~ ";" ^^ { _ => new AddFourCommand(env) }
    )
  }
}
