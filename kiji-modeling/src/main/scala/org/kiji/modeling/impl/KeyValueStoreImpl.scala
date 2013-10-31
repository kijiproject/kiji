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

import org.apache.avro.generic.GenericRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.AvroValue
import org.kiji.express.EntityId
import org.kiji.modeling.KeyValueStore
import org.kiji.mapreduce.kvstore.{ KeyValueStoreReader => JKeyValueStoreReader }
import org.kiji.schema.KijiRowKeyComponents

/**
 * A KijiExpress key-value store backed by a Kiji table.
 *
 * @param kvStoreReader is an opened KijiMR key-value store used to back the new KijiExpress
 *     key-value store.
 * @tparam V is the type of value users will retrieve when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] final class KijiTableKeyValueStore[V](
    kvStoreReader: JKeyValueStoreReader[_ <: KijiRowKeyComponents, _ <: Any])
    extends KeyValueStore[EntityId, V](kvStoreReader)
    with EntityIdScalaToJavaKeyConverter
    with AvroJavaToScalaValueConverter[V]

/**
 * A KijiExpress key-value store backed by a KijiMR `AvroRecordKeyValueStore`. The KijiMR
 * key-value store should not have a reader schema configured.
 *
 * @param kvStoreReader is an opened KijiMR key-value store used to back the new KijiExpress
 *     key-value store.
 * @tparam K is the type of key users will use to access the key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] final class AvroRecordKeyValueStore[K](
    kvStoreReader: JKeyValueStoreReader[_ <: Any, _ <: GenericRecord])
    extends KeyValueStore[K, AvroValue](kvStoreReader)
    with AvroScalaToJavaKeyConverter[K]
    with AvroJavaToScalaValueConverter[AvroValue]

/**
 * A KijiExpress key-value store backed by a KijiMR `AvroKVRecordKeyValueStore`. The
 * KijiMR key-value store should not have a reader schema configured.
 *
 * @param kvStoreReader is an opened KijiMR key-value store used to back the new KijiExpress
 *     key-value store.
 * @tparam K is the type of key users will specify when accessing a key-value store.
 * @tparam V is the type of value users will retrieve when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] final class AvroKVRecordKeyValueStore[K,V](
    kvStoreReader: JKeyValueStoreReader[_ <: Any, _ <: Any])
    extends KeyValueStore[K, V](kvStoreReader)
    with AvroScalaToJavaKeyConverter[K]
    with AvroJavaToScalaValueConverter[V]

/**
 * A KijiExpress key-value store backed by a KijiMR `TextFileKeyValueStore`.
 *
 * @param kvStoreReader is an opened KijiMR key-value store used to back the new KijiExpress
 *     key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] final class TextFileKeyValueStore(
    kvStoreReader: JKeyValueStoreReader[String, String])
    extends KeyValueStore[String, String](kvStoreReader) {
  protected override def keyConversion(keyWithScalaType: String): Any = keyWithScalaType

  protected override def valueConversion(valueWithJavaType: Any): String = {
    valueWithJavaType.asInstanceOf[String]
  }
}
