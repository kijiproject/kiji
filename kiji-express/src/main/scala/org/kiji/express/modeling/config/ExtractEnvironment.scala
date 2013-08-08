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
import org.kiji.annotations.Inheritance

/**
 * A specification of the runtime bindings needed in the extract phase of a model.
 *
 * @param dataRequest describing the input columns required during the extract phase.
 * @param fieldBindings defining a mapping from columns requested to their corresponding field
 *     names. This determines how data that is requested for the extract phase is mapped onto named
 *     input fields.
 * @param kvstores for usage during the extract phase.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class ExtractEnvironment private[express] (
    val dataRequest: ExpressDataRequest,
    val fieldBindings: Seq[FieldBinding],
    val kvstores: Seq[KVStore]) {
  override def equals(other: Any): Boolean = {
    other match {
      case environment: ExtractEnvironment => {
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
 * Companion object to ExtractEnvironment containing factory methods.
 */
object ExtractEnvironment {
  /**
   * Creates a new ExtractEnvironment, which is a specification of the runtime bindings needed in
   * the extract phase of a model.
   *
   * @param dataRequest describing the input columns required during the extract phase.
   * @param fieldBindings describing a mapping from columns requested to their corresponding field
   *     names. This determines how data that is requested for the extract phase is mapped onto
   *     named input fields.
   * @param kvstores describing the kv stores for usage during the extract phase.
   * @return an ExtractEnvironment with the configuration specified.
   */
  def apply(dataRequest: ExpressDataRequest,
      fieldBindings: Seq[FieldBinding],
      kvstores: Seq[KVStore]): ExtractEnvironment = {
    new ExtractEnvironment(
        dataRequest,
        fieldBindings,
        kvstores)
  }
}
