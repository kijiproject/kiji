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

import org.kiji.annotations.ApiAudience
import org.kiji.delegation.Lookups
import org.kiji.delegation.NamedLookup
import org.kiji.delegation.NoSuchProviderException
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.spi.EnvironmentPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory


/**
 * Returns a modified environment that uses the specified module.
 *
 * @param env the environment in which this command executes.
 * @param moduleName the name of the module to load.
 */
@ApiAudience.Private
final class UseModuleCommand(val env: Environment, val moduleName: String) extends DDLCommand {

  override def exec(): Environment = {
    val lookup = Lookups.getNamed(classOf[ParserPluginFactory])
    try {
      val module = lookup.lookup(moduleName)
      echo("Loading module \"" + moduleName + "\"")
      val envWithModule: Environment = env.withModule(module)
      // Take this environment and add any necessary extension data.
      return addExtensionData(envWithModule, module)
    } catch { case _: NoSuchProviderException =>
      throw new DDLException("No such module: '" + moduleName + "'.")
    }
  }

  /**
   * Further process the environment after loading the module, by adding any initial
   * environment extension data specified by the plugin. This is only applicable to
   * ParserPluginFactory instances that also extend EnvironmentPlugin.
   *
   * <p>This has been called out into its own method as a means to introduce the 'T' type
   * parameter, required for matching the type of envPlugin with the result of calling its
   * createExtensionState() method.</p>
   *
   * @param envWithModule the initial environment 'env', with the specified module loaded.
   * @param module the ParserPluginFactory module being loaded.
   * @tparam T the free type parameter of the EnvironmentPlugin that the ParserPluginFactory
   *     is cast to.
   * @return the provided environment, augmented with any initial environment extension data
   *     supplied by the plugin.
   */
  private def addExtensionData[T](envWithModule: Environment, module: ParserPluginFactory):
      Environment = {
    if (module.isInstanceOf[EnvironmentPlugin[_]]) {
      // This module extends the environment with additional data to track.
      // Load its default data in here.
      val envPlugin: EnvironmentPlugin[T] = module.asInstanceOf[EnvironmentPlugin[T]]
      return envWithModule.updateExtension(envPlugin, envPlugin.createExtensionState())
    } else {
      return envWithModule // Just loading the module was sufficient.
    }
  }
}
