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

package org.kiji.schema.shell.ddl

import scala.collection.JavaConversions._

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.ColumnDesc
import org.kiji.schema.avro.FamilyDesc
import org.kiji.schema.avro.LocalityGroupDesc
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.TableNotFoundException

/**
 * Abstract base class for DDL command implementations.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Extensible
abstract class DDLCommand {

  /**
   * Get the environment in which the command should be executed.
   *
   * <p>This must return the same Environment for the lifetime of a <tt>DDLCommand</tt>.
   *
   * @return the environment in which the command is executed.
   */
  def env(): Environment

  /**
   * Method called by the runtime to execute this parsed command.
   * @return the environment object to use in subsequent commands.
   */
  def exec(): Environment

  /** Return the Kiji instance name being operated on. */
  final def getKijiURI(): KijiURI = {
    env.instanceURI
  }

  /**
   * Print the supplied string to the output with a newline. Output is typically
   * stdout, but can be redirected e.g. for testing.
   * @param s the string to emit.
   */
  final protected def echo(s: String): Unit = {
    env.printer.println(s)
  }

  /**
   * Print the supplied string to the output with no trailing newline. Output is typically
   * stdout, but can be redirected e.g. for testing.
   * @param s the string to emit.
   */
  final protected def echoNoNL(s:String): Unit = {
    env.printer.print(s)
  }
}
