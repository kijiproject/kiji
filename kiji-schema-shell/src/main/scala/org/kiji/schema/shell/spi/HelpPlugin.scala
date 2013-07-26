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

/**
 * Plugin SPI that specifies an extension to the text displayed when a user types 'help'
 * in the Kiji shell, if this module is loaded.
 *
 * <p>This SPI is a "decorator" SPI that should be attached to a {@link ParserPluginFactory}.
 * It declares that the ParserPluginFactory can also provide 'help' text specifying the module's
 * usage. If this module is loaded through a `MODULE 'modname';` command, this help
 * text will be appended to the text displayed to the user if she requests help.</p>
 *
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait HelpPlugin extends NamedProvider {

  /**
   * Return text specifying how this plugin operates and what syntax it provides.
   *
   * @return a text message to display to the user.
   */
  def helpText(): String
}
