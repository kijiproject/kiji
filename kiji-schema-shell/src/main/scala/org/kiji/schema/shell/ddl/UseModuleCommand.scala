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
      return env.withModule(module)
    } catch { case _: NoSuchProviderException =>
      throw new DDLException("No such module: '" + moduleName + "'.")
    }
  }
}
