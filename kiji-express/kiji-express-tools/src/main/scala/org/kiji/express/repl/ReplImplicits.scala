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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.framework.ExpressConversions
import org.kiji.express.flow.util.PipeConversions

import cascading.pipe.Pipe

/**
 * Implicits used to construct pipes within the REPL.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object ReplImplicits {

  /**
   * Converts a Pipe to a KijiPipeTool. This method permits implicit conversions from Pipe to
   * KijiPipeTool.
   *
   * @param pipe to convert to a KijiPipeTool.
   * @return a KijiPipeTool created from the specified Pipe.
   */
  implicit def pipeToKijiPipeTool(pipe: Pipe): KijiPipeTool = new KijiPipeTool(pipe)

  /**
   * Converts a KijiPipeTool to a Pipe. This method permits implicit conversions from
   * KijiPipeTool to Pipe.
   *
   * @param kijiPipeTool to convert to a Pipe.
   * @return the Pipe wrapped by the specified KijiPipeTool.
   */
  implicit def kijiPipeToolToPipe(kijiPipeTool: KijiPipeTool): Pipe = kijiPipeTool.pipe
}
