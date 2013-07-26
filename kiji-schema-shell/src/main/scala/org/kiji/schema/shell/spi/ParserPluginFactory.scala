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
 * Plugin SPI that creates a parser plugin that adds new commands that extend
 * the Kiji Schema shell.
 *
 * <p>You can register a new plugin by doing the following:</p>
 * <ul>
 *   <li>Extend the `ParserPluginFactory` abstract class.</li>
 *   <li>Create a text file named
 *   `src/main/resources/META-INF/services/org.kiji.schema.shell.spi.ParserPluginFactory`.
 *       </li>
 *   <li>In this file, include the fully-qualified name of your factory class.</li>
 *   <li>Create a [[org.kiji.schema.shell.spi.ParserPlugin]] implementation that extends
 *       the schema shell with
 *       new statement parsers. This `ParserPluginFactory` should create an instance
 *       of your `ParserPlugin` in the {@link #create} method.</li>
 * </ul>
 *
 * <p>Clients can use your plugin by doing the following:</p>
 * <ul>
 *   <li>Make sure your compiled jar file is on the classpath of the Kiji schema shell.</li>
 *   <li>Type `MODULE &lt;module-name&gt;`, where <em>&lt;module-name&gt;</em> is
 *       the string returned by getModuleName().</li>
 *   <li>Use the new syntax from your parser plugin.</li>
 * </ul>
 *
 * <ul>
 *   <li>The {@link #getName} method defines the user-friendly name of the module. This must match
 *       the pattern [a-zA-Z_][a-zA-Z0-9_]*.</li>
 *   <li>The {@link #create} method creates a new instance of the parser plugin.</li>
 * </ul>
 *
 * <p>This plugin SPI can be augmented by multiple optional SPI traits. See the
 * {@link EnvironmentPlugin} and {@link HelpPlugin} traits in this module. You can incorporate
 * these traits in your ParserPluginFactory implementation.</p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait ParserPluginFactory extends NamedProvider {
  /**
   * Factory method that creates a new instance of your {@link ParserPlugin}
   * class. You should return an instance of `ParserPlugin` parameterized
   * by the specified environment.
   *
   * @param env the environment under which your parser plugin will evaluate.
   * @return a new ParserPlugin instance parameterized by the specified environment.
   */
  def create(env: Environment): ParserPlugin
}
