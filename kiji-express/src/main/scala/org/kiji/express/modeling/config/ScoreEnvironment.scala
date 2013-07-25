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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

import com.google.common.base.Objects

/**
 * A specification of the runtime bindings for data sources required in the score phase of a model.
 *
 * @param outputColumn to write scores to.
 * @param kvstores for usage during the score phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
final class ScoreEnvironment private[express] (
    val outputColumn: String,
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: ScoreEnvironment => {
        outputColumn == environment.outputColumn &&
            kvstores == environment.kvstores
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(
          outputColumn,
          kvstores)
}

/**
 * Companion object containing factory methods for ScoreEnvironment.
 */
object ScoreEnvironment {
  /**
   * Creates a ScoreEnvironment, which is a specification of the runtime bindings for data sources
   * required in the score phase of a model.
   *
   * @param outputColumn to write scores to.
   * @param kvstores is the specification of the kv stores for usage during the score phase.
   * @return a ScoreEnvironment with the specified settings.
   */
  def apply(outputColumn: String, kvstores: Seq[KVStore]): ScoreEnvironment = {
    new ScoreEnvironment(outputColumn, kvstores)
  }
}
