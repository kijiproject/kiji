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

package org.kiji.modeling.impl

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.mapreduce.kvstore.{ KeyValueStoreReader => JKeyValueStoreReader }
import org.kiji.modeling.KeyValueStore

/**
 * Implementation-wise, a KijiExpress key-value store is Scala-friendly face on the Java key-value
 * stores provided by the KijiMR library.  This class composes together a KijiMR key-value store and
 * functions that convert keys and values.  These key and value conversion function allow  the
 * KeyValueStore to provides keys and values of Scala-convenient types, and have them automatically
 * converted to the underlying key-value store's key and value types.
 *
 * @tparam K is the type of key users will specify when accessing the key-value store.
 * @tparam V is the type of value users will retrieve when accessing the key-value store.
 * @tparam UK is the underlying KijiMR kv-store's key type.
 * @tparam UV is the underlying KijiMR kv-store's value type.
 * @param kvStoreReader a KijiMR key-value store that will back this KijiExpress key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] class ForwardingKeyValueStore[K, V, UK, UV](
    kvStoreReader: JKeyValueStoreReader[UK, UV],
    keyConverter: K => UK,
    valueConverter: UV => V
) extends KeyValueStore[K, V] {
  require(kvStoreReader != null)
  require(keyConverter != null)
  require(valueConverter != null)

  override private[modeling] def close(): Unit = kvStoreReader.close()

  override def get(key: K): Option[V] = {
    require(key != null, "A null key was used to access a value from a KeyValueStore.")
    Option(kvStoreReader.get(keyConverter(key))).map(valueConverter)
  }

  override def containsKey(key: K): Boolean =  kvStoreReader.containsKey(keyConverter(key))
}

