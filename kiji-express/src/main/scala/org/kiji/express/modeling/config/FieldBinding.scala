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
import org.kiji.annotations.Inheritance
import org.kiji.express.avro.AvroFieldBinding

/**
 * A case class wrapper around the parameters necessary for an Avro FieldBinding, which is an
 * association between field names and column names of a table.
 * This is a convenience for users to define FieldBindings when using the ModelEnvironment.
 *
 * @param tupleFieldName is the name of the tuple field to associate with the `storeFieldName`.
 * @param storeFieldName is the name of the store field to associate with the `tupleFieldName`.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class FieldBinding(
    tupleFieldName: String,
    storeFieldName: String) {

  /**
   * Builds an avro record corresponding to this field binding.
   *
   * @return an avro record corresponding to this field binding.
   */
  private[express] def toAvroFieldBinding(): AvroFieldBinding = {
    new AvroFieldBinding(tupleFieldName, storeFieldName)
  }
}

/**
 * The companion object to FieldBinding for factory methods.
 */
object FieldBinding {
  /**
   * Converts an Avro FieldBinding specification into a FieldBinding case class.
   *
   * @param avroFieldBinding is the Avro specification.
   * @return the FieldBinding specification as a FieldBinding case class.
   */
  private[express] def apply(avroFieldBinding: AvroFieldBinding): FieldBinding = {
    FieldBinding(
      tupleFieldName = avroFieldBinding.getTupleFieldName.toString,
      storeFieldName = avroFieldBinding.getStoreFieldName.toString
    )
  }
}
