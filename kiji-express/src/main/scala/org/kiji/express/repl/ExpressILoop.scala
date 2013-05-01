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

package org.kiji.express.repl

import scala.tools.nsc.interpreter.ILoop

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * A class providing KijiExpress specific commands for inclusion in the KijiExpress REPL.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] class ExpressILoop extends ILoop {
  /**
   * Commands specific to the KijiExpress REPL. To define a new command use one of the following
   * factory methods:
   * - `LoopCommand.nullary` for commands that take no arguments
   * - `LoopCommand.cmd` for commands that take one string argument
   * - `LoopCommand.varargs` for commands that take multiple string arguments
   */
  private val expressCommands: List[LoopCommand] = List()

  /**
   * Change the shell prompt to read express&gt;
   *
   * @return a prompt string to use for this REPL.
   */
  override def prompt: String = "\nexpress> "

  /**
   * Gets the list of commands that this REPL supports.
   *
   * @return a list of the command supported by this REPL.
   */
  override def commands: List[LoopCommand] = super.commands ++ expressCommands
}
