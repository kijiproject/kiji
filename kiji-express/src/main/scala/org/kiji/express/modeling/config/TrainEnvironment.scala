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

package org.kiji.express.modeling.config

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * A specification of the runtime bindings needed in the train phase of a model.
 *
 * @param inputConfig defining a mapping from input data source names to their configurations.
 * @param outputConfig defining a mapping from output data sink names to their configurations.
 * @param kvstores for usage during the train phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class TrainEnvironment private[express](
    val inputConfig: InputSpec,
    val outputConfig: OutputSpec,
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: TrainEnvironment => {
        inputConfig == environment.inputConfig &&
            outputConfig == environment.outputConfig &&
            kvstores == environment.kvstores
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          inputConfig,
          outputConfig,
          kvstores)
}

/**
 * Companion object to TrainEnvironment containing factory methods.
 */
object TrainEnvironment {
  /**
   * Creates a new TrainEnvironment, which is a specification of the runtime bindings needed in
   * the train phase of a model.
   *
   * @param inputConfig defining a mapping from input data source names to their configurations.
   * @param outputConfig defining a mapping from output data sink names to their configurations.
   * @param kvstores for usage during the train phase.
   * @return an TrainEnvironment with the specified configuration.
   */
  def apply(
      inputConfig: InputSpec,
      outputConfig: OutputSpec,
      kvstores: Seq[KVStore]): TrainEnvironment = {
    new TrainEnvironment(
        inputConfig,
        outputConfig,
        kvstores)
  }
}
