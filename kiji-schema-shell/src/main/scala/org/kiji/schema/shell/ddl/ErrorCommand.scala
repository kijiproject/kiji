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

/**
 * Do-nothing command that prints the result of a failed statement parse to stdout.
 *
 * @param env the environment in which this command is executed.
 * @param parseResult an error message returned by the parser, that should be shown
 *     in the course of "executing" this error-handling command.
 * @param throwOnErr if true, then this command throws a DDLException when its `exec()`
 *     method is called (since running this class implies that an error has occurred).
 */
@ApiAudience.Private
final class ErrorCommand(val env: Environment, val parseResult: String, val throwOnErr: Boolean)
    extends DDLCommand {

  override def exec(): Environment = {
    echo(parseResult)
    if (throwOnErr) {
      // Client expects this error command to represent failure. Actually throw an exception.
      throw new DDLException(parseResult)
    }
    return env
  }
}
