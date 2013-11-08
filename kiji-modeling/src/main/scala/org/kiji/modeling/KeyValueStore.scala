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

package org.kiji.modeling

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.generic.IndexedRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.EntityId
import org.kiji.mapreduce.kvstore.KeyValueStoreReader
import org.kiji.mapreduce.kvstore.lib.{AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{AvroRecordKeyValueStore => JAvroRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{KijiTableKeyValueStore => JKijiTableKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{TextFileKeyValueStore => JTextFileKeyValueStore}
import org.kiji.modeling.impl.ForwardingKeyValueStore
import org.kiji.schema.KijiRowKeyComponents

/**
 * A map from keys to values backed by a data store. KijiExpress end-users can configure and use
 * key-value stores as part of a step in a KijiExpress modeling workflow. Key-value stores can
 * provide "side data" to steps of a modeling workflow (such as a model computed in another
 * system), and/or be used to pass data generated in one modeling workflow step to another.
 *
 * End-users can configure key-value stores for use in a modeling workflow through the JSON
 * configuration of a modeling workflow. See [[org.kiji.modeling.config.ModelDefinition]]
 * and [[org.kiji.modeling.config.ModelEnvironment]] for more information. Once
 * configured, a key-value store can be obtained within implementations of modeling workflow
 * steps (like [[org.kiji.modeling.Extractor]] and [[org.kiji.modeling.Scorer]]
 * by using the method `#keyValueStoreSpecs`.
 *
 * A KijiExpress key-value store is in an opened state when constructed,
 * and must be closed after use. KijiExpress end-users are not responsible for opening or closing
 * key-value stores; rather, KijiExpress is responsible for closing a key-value store for
 * end-users after the execution of a modeling workflow step.
 *
 * Developers should see the `ForwardingKeyValueStore` for implementing this trait over an existing
 * KijiMR key-value store.
 *
 * Users should see the companion object for creating instances of this trait based on existing
 * KijiMR key-value store implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait KeyValueStore[K,V] {
/**
 * Retrieves the value associated with a key from this key-value store.
 *
 * The key specified must be non-`null`. This method will return `null` if there is no value
 * associated with the key specified. Use the method `#containsKey` to determine whether a key
 * is associated with a value in a key-value store.
 *
 * @param key whose value should be retrieved from the key-value store. Must be non-`null`.
 * @return the value associated with the specified key, or `null` if no such value exists.
 */
  def apply(key: K): V = {
    get(key).getOrElse(
        throw new NoSuchElementException(("No value exists in this KeyValueStore for key '%s' " +
            "Use either the method get() on this KeyValueStore to retrieve optional values, or " +
            "first check that the key is defined using the method containsKey() before using " +
            "this method to lookup the key.").format(key.toString)))
  }

/**
 * Retrieves an optional value associated with a key from this key-value store.
 *
 * The key specified must be non-`null`. This method will return `None` if there is no value
 * associated with the key specified. Use the method `#containsKey` to determine whether a key
 * is associated with a value in a key-value store.
 *
 * @param key whose value should be retrieved from the key-value store. Must be non-`null`.
 * @return an optional value associated with the specified key.
 */
  def get(key: K): Option[V]

/**
 * Returns the value associated with a key, or a default value if the key is not contained in
 * the map.
 *
 * @param key whose value should be retrieved, if possible.
 * @param default value to return if key is not contained in the key value store.
 */
  def getOrElse(key: K, default: V): V =  get(key).getOrElse(default)

  /**
   * Determines if the specified key is associated with a value by the key-value store.
   *
   * @param key that may be associated with a value by the key-value store. Must be non-`null`.
   * @return `true` if the specified key is associated with a value by the key-value store,
   *     `false` otherwise.
   */
  def containsKey(key: K): Boolean

  /**
   * Closes all resources used by this key-value store.
   */
  private[modeling] def close(): Unit
}

/**
 * A factory for key-value stores backed by specific KijiMR key-value store implementations.
 */
private[modeling] object KeyValueStore {

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR key-value store reader.  This
   * KeyValueStore does no conversion between keys and values.
   *
   * @param kvStoreReader of underlying KijiMR key-value store.
   * @tparam K type of keys
   * @tparam V type of values
   * @return KeyValueStore backed by a KijiMR key-value store.
   */
  def apply[K, V](
      kvStoreReader: KeyValueStoreReader[K, V]
  ): KeyValueStore[K, V] = {
    new ForwardingKeyValueStore[K, V, K, V](kvStoreReader, identity, identity)
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR key-value store backed by a
   * Kiji table. Such a key-value store allows users to access the latest value in a column of a
   * Kiji table, by using the qualified column name as key.
   *
   * @param kvStoreReader of backing `KijiTableKeyValueStore`.
   * @tparam V is the type of value retrieved from the key-value store.
   * @return a KijiExpress key-value store backed by a Kiji table.
   */
  def kijiTableKeyValueStore[V](
      kvStoreReader: KeyValueStoreReader[KijiRowKeyComponents, V]
  ): KeyValueStore[EntityId, V] = {
    new ForwardingKeyValueStore[EntityId, V, KijiRowKeyComponents, V](
        kvStoreReader,
        eidConverter,
        identity)
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR key-value store backed by a
   * Kiji table. Such a key-value store allows users to access the latest value in a column of a
   * Kiji table, by using the qualified column name as key.
   *
   * @param kvStore of backing `KijiTableKeyValueStore`.
   * @tparam V is the type of value retrieved from the key-value store.
   * @return a KijiExpress key-value store backed by a Kiji table.
   */
  def apply[V](
      kvStore: JKijiTableKeyValueStore[V]
  ): KeyValueStore[EntityId, V] = {

    new ForwardingKeyValueStore[EntityId, V, KijiRowKeyComponents, V](
        kvStore.open(),
        eidConverter,
        identity)
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR `AvroRecordKeyValueStore`.
   * Such a key-value store allows users to access records from an Avro store file,
   * by distinguishing a record field as the key field. Note that the KijiMR key-value store
   * used to create the KijiExpress key-value store should not have a reader schema configured.
   *
   * @param kvStore of backing `AvroRecordKeyValueStore`.
   * @tparam K is the type of key used to access values from the key-value store.
   * @return a KijiExpress key-value store backed by a KijiMR `AvroRecordKeyValueStore`.
   */
  def apply[K, V <: IndexedRecord](
      kvStore: JAvroRecordKeyValueStore[K, V]
  ): KeyValueStore[K, V] = {
    new ForwardingKeyValueStore[K, V, K, V](kvStore.open(), identity, identity)
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR `AvroKVRecordKeyValueStore`.
   * Such a key-value store allows users to access values contained in a field of records in an
   * Avro store file, by distinguishing a record field as the key field. Note that the KijiMR
   * key-value store used to create the KijiExpress key-value store should not have a reader
   * schema configured.
   *
   * @param kvStore from KijiMR that will back the KijiExpress key-value store.
   * @tparam K is the type of key used to access values from the key-value store.
   * @tparam V is the type of value retrieved from the key-value store.
   * @return a KijiExpress key-value store backed by a KijiMR `AvroKVRecordKeyValueStore`.
   */
  def apply[K,V](
      kvStore: JAvroKVRecordKeyValueStore[K, V]
  ): KeyValueStore[K, V] = {
    new ForwardingKeyValueStore[K, V, K, V](kvStore.open(), identity, identity)
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR `TextFileKeyValueStore`.
   * Such a key-value store allows users to access values contained in a text file by distinguishing
   * keys from values with a text delimiter.
   *
   * @param kvStore from KijiMR that will back the KijiExpress key-value store.
   * @return a KijiExpress key-value store backed by a KijiMR `TextFileKeyValueStore`.
   */
  def apply(
      kvStore: JTextFileKeyValueStore
  ): KeyValueStore[String, String] = {
    new ForwardingKeyValueStore[String, String, String, String](kvStore.open(), identity, identity)
  }

  /**
   * Converts an [[org.kiji.express.EntityId]] to a java KV-store compatible
   * [[org.kiji.schema.KijiRowKeyComponents]].
   *
   * @param eid to convert.
   * @return the equivalent `KijiRowKeyComponents`.
   */
  private def eidConverter(eid: EntityId): KijiRowKeyComponents = {
    // KijiRowKeyComponents can use Java byte array, Java String, Java Long,
    // or Java Integer as components. As long as we ensure all components are AnyRef,
    // then Scala Array[Byte], Scala String, Scala Long, and Scala Int are usable to create a
    // KijiRowKeyComponents. We do this conversion then create the KijiRowKeyComponents.
    KijiRowKeyComponents.fromComponentsList(eid.components.asJava)
  }
}
