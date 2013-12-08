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

package org.kiji.modeling.framework

import cascading.pipe.Pipe
import cascading.flow.FlowDef
import com.twitter.scalding.Mode

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.KijiSource
import org.kiji.modeling.lib.RecommendationPipe

/**
 * ModelPipeConversions contains implicit conversions necessary for KijiModeling that are not
 * included in KijiExpress's KijiJob.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
trait ModelPipeConversions {

  /**
   * Converts a Cascading Pipe to an Express Recommendation Pipe. This method permits implicit
   * conversions from Pipe to RecommendationPipe.
   *
   * @param pipe to convert to a RecommendationPipe.
   * @return a RecommendationPipe wrapping the specified Pipe.
   */
  implicit def pipe2RecommendationPipe(pipe: Pipe): RecommendationPipe =
    new RecommendationPipe(pipe)

  /**
   * Converts a KijiSource to a KijiExpress Recommendation Pipe. This method permits implicit
   * conversions from Source to RecommendationPipe.
   *
   * We expect flowDef and mode implicits to be in scope.  This should be true in the context of a
   * Job, KijiJob, or inside the ShellRunner.
   *
   * @param source to convert to a KijiPipe.
   * @return a RecommendationPipe read from the specified source.
   */
  implicit def source2RecommendationPipe(
      source: KijiSource)(
      implicit flowDef: FlowDef,
      mode: Mode): RecommendationPipe = new RecommendationPipe(source.read(flowDef, mode))
}
