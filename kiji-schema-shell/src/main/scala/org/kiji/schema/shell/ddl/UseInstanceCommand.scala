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
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment

import org.kiji.schema.KConstants

/** Return a modified environment that uses a different Kiji instance name. */
@ApiAudience.Private
final class UseInstanceCommand(val env: Environment, val instance: String) extends DDLCommand {
  override def exec(): Environment = {
    val instances = env.kijiSystem.listInstances()

    // TODO: Eventually eliminate hilarity around instance names.
    if (instances.contains(instance)
        || (instance.equals(KConstants.DEFAULT_INSTANCE_NAME)
            && instances.contains("(default)"))) {
      echo("Using Kiji instance \"" + instance + "\"")
      return env.withInstance(instance)
    } else {
      throw new DDLException("No such Kiji instance: " + instance)
    }
  }
}
