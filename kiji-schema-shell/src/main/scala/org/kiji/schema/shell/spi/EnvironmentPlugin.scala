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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.delegation.NamedProvider
import org.kiji.schema.shell.Environment

/**
 * Plugin SPI that specifies an extension to the data tracked by the Environment
 * object in the Kiji shell.
 *
 * This SPI is a "decorator" SPI that should be attached to a {@link ParserPluginFactory}.
 * It declares that the ParserPluginFactory also creates an object to be placed in the
 * {@link Environment}. This object can hold any state required to facilitate your plugin's
 * operation over a session consisting of multiple commands. Within the Environment,
 * this is keyed by the same "name" returned by {@link #getName} as in the
 * ParserPluginFactory itself.
 *
 * The type argument to this class represents the type of object that holds your state.
 *
 * ===Best Practices Regarding Mutability===
 *
 * Changes to the Environment can only happen in the `exec()` method of a
 * `DDLCommand`. Your `ParserPlugin` will emit a `DDLCommand` that
 * represents the custom command to run. The DDLCommand's `exec()` method then
 * returns the Environment to feed forward into the next command.
 *
 * The main `Environment` is an immutable object; "updates" to the Environment
 * are reflected by creating a new `Environment` object that differs from the previous
 * Environment only by a single field.
 *
 * <em>It is recommended that you make your own environment extension immutable as well.</em>
 * When updating the state to propagate forward to subsequent commands, you should create a
 * new instance of your environment state class, holding the new information you need. You
 * can then call the {@link DDLCommand#setExtensionState} method to insert this new object in
 * the Environment (actually, this returns a new Environment that contains the updated extension
 * mapping). Your {@link DDLCommand#exec} method should then return this new Environment.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait EnvironmentPlugin[EnvState] extends NamedProvider {

  /**
   * Create the initial state held by your extension to the Environment. This will
   * be called once when your module loads to initialize the Environment with your
   * default data.
   *
   * @return a new instance of your environment state container, for use when your
   *    module is first initialized.
   */
  def createExtensionState(): EnvState
}
