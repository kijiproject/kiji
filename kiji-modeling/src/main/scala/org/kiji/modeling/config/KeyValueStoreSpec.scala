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

package org.kiji.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * A case class wrapper around the parameters necessary for an Avro KVStore.  This is a convenience
 * for users to define their KVStores when using the ModelEnvironment.
 *
 * @param storeType of this KVStore.  Must be one of "AVRO_KV", "AVRO_RECORD", "KIJI_TABLE".
 * @param name specified by the user as a shorthand identifier for this KVStore.
 * @param properties that may be needed to configure and instantiate a kv store reader.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KeyValueStoreSpec(
    storeType: String,
    name: String,
    properties: Map[String, String]) {

  // The allowed store types.
  val possibleStoreTypes = Set("AVRO_KV", "AVRO_RECORD", "KIJI_TABLE")

  require(
      possibleStoreTypes.contains(storeType),
      "storeType must be one of %s, instead was %s".format(possibleStoreTypes, storeType))
}
