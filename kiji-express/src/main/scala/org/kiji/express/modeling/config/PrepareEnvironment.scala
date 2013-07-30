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

/**
 * A specification of the runtime bindings needed in the prepare phase of a model.
 *
 * @param dataRequest describing the input columns required during the prepare phase.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the prepare phase is mapped onto named
 *     input fields.
 * @param kvstores for usage during the prepare phase.
 * @param outputColumn to write to.
 */
final class PrepareEnvironment private[express](
    val dataRequest: ExpressDataRequest,
    val fieldBindings: Seq[FieldBinding],
    val kvstores: Seq[KVStore],
    val outputColumn: String) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: PrepareEnvironment => {
        dataRequest == environment.dataRequest &&
            fieldBindings == environment.fieldBindings &&
            kvstores == environment.kvstores
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      dataRequest,
      fieldBindings,
      kvstores)
}

/**
 * Companion object to PrepareEnvironment containing factory methods.
 */
object PrepareEnvironment {
  /**
   * Creates a new PrepareEnvironment, which is a specification of the runtime bindings needed in
   * the prepare phase of a model.
   *
   * @param dataRequest describing the input columns required during the prepare phase.
   * @param fieldBindings describing a mapping from columns requested to their corresponding field
   *     names. This determines how data that is requested for the prepare phase is mapped onto
   *     named input fields.
   * @param kvstores describing the kv stores for usage during the prepare phase.
   * @param outputColumn to write to.
   * @return an PrepareEnvironment with the configuration specified.
   */
  def apply(dataRequest: ExpressDataRequest,
            fieldBindings: Seq[FieldBinding],
            kvstores: Seq[KVStore],
            outputColumn: String): PrepareEnvironment = {
    new PrepareEnvironment(
      dataRequest,
      fieldBindings,
      kvstores, outputColumn)
  }
}
