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

import org.apache.avro.generic.GenericRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.AvroValue
import org.kiji.express.EntityId
import org.kiji.modeling.impl.AvroKVRecordKeyValueStore
import org.kiji.modeling.impl.AvroRecordKeyValueStore
import org.kiji.modeling.impl.JavaToScalaValueConverter
import org.kiji.modeling.impl.KijiTableKeyValueStore
import org.kiji.modeling.impl.ScalaToJavaKeyConverter
import org.kiji.modeling.impl.TextFileKeyValueStore
import org.kiji.mapreduce.kvstore.lib.{AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{AvroRecordKeyValueStore => JAvroRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{KijiTableKeyValueStore => JKijiTableKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{TextFileKeyValueStore => JTextFileKeyValueStore}
import org.kiji.mapreduce.kvstore.{KeyValueStoreReader => JKeyValueStoreReader}

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
 * Implementation-wise, a KijiExpress key-value store is Scala-friendly face on the Java
 * key-value stores provided by the KijiMR library. A KijiExpress key-value store implements
 * the traits [[org.kiji.modeling.impl.ScalaToJavaKeyConverter]] and
 * [[org.kiji.modeling.impl.JavaToScalaValueConverter]], which provide a means for
 * converting
 * keys specified as Scala types to equivalent Java types (used to access the underlying
 * KijiMR key-value store), and values specified as Java types to equivalent Scala types
 * (used to make values retrieved from KijiMR user).
 *
 * Developers should see `KeyValueStoreConverters.scala` for predefined key-value converters,
 * and `KeyValueStoreImpl.scala` for concrete implementations of key-value stores backed by
 * various KijiMR key-value stores.
 *
 * @param javaKeyValueStoreReader a KijiMR key-value store that will back this KijiExpress key-value
 *     store.
 * @tparam K is the type of key users will specify when accessing the key-value store.
 * @tparam V is the type of value users will retrieve when accessing the key-value store.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
abstract class KeyValueStore[K, V] protected[modeling] (
    javaKeyValueStoreReader: JKeyValueStoreReader[_,_])
    extends ScalaToJavaKeyConverter[K]
    with JavaToScalaValueConverter[V] {
  /** A reader for the KijiMR key-value store backing this KijiExpress key-value store. */
  private val kvStoreReader: JKeyValueStoreReader[Any, Any] = javaKeyValueStoreReader
      .asInstanceOf[JKeyValueStoreReader[Any, Any]]

  /**
   * Closes all resources used by this key-value store.
   */
  private[modeling] def close() {
    // scalastyle:off null
    if (kvStoreReader != null) {
      kvStoreReader.close()
    }
    // scalastyle:on null
  }

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
    val retrievedValue: Option[V] = get(key)
    if (retrievedValue.isDefined) {
      retrievedValue.get
    } else {
      throw new NoSuchElementException(("No value exists in this KeyValueStore for key '%s' " +
          "Use either the method get() on this KeyValueStore to retrieve optional values, or " +
          "first check that the key is defined using the method containsKey() before using " +
          "this method to lookup the key.").format(key.toString))
    }
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
  def get(key: K): Option[V] = {
    // scalastyle:off null
    if (key == null) {
      throw new IllegalArgumentException("A null key was used to access a value from a "
          + "KeyValueStore")
    }
    val rawValue: Any = kvStoreReader.get(keyConversion(key))
    if (rawValue == null) None else Some(valueConversion(rawValue))
    // scalastyle:on null
  }

  /**
   * Determines if the specified key is associated with a value by the key-value store.
   *
   * @param key that may be associated with a value by the key-value store. Must be non-`null`.
   * @return `true` if the specified key is associated with a value by the key-value store,
   *     `false` otherwise.
   */
  def containsKey(key: K): Boolean = {
    // scalastyle:off null
    if (key == null) {
      throw new IllegalArgumentException("A null key was used when checking if a key is a " +
          "associated with a value in a key-value store.")
    }
    // scalastyle:on null
    kvStoreReader.containsKey(keyConversion(key))
  }
}

/**
 * A factory for key-value stores backed by specific KijiMR key-value store implementations.
 */
private[modeling] object KeyValueStore {
  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR key-value store backed by a
   * Kiji table. Such a key-value store allows users to access the latest value in a column of a
   * Kiji table, by using the qualified column name as key.
   *
   * @param kvStore from KijiMR that will back the KijiExpress key-value store.
   * @tparam V is the type of value retrieved from the key-value store.
   * @return a KijiExpress key-value store backed by a Kiji table.
   */
  def apply[V](
      kvStore: JKijiTableKeyValueStore[_ <: Any]
  ): KeyValueStore[EntityId, V] = {
    new KijiTableKeyValueStore[V](kvStore.open())
  }

  /**
   * Creates a new KijiExpress key-value store backed by a KijiMR `AvroRecordKeyValueStore`.
   * Such a key-value store allows users to access records from an Avro store file,
   * by distinguishing a record field as the key field. Note that the KijiMR key-value store
   * used to create the KijiExpress key-value store should not have a reader schema configured.
   *
   * @param kvStore from KijiMR that will back the KijiExpress key-value store.
   * @tparam K is the type of key used to access values from the key-value store.
   * @return a KijiExpress key-value store backed by a KijiMR `AvroRecordKeyValueStore`.
   */
  def apply[K](
      kvStore: JAvroRecordKeyValueStore[_ <: Any, _ <: GenericRecord]
  ): KeyValueStore[K, AvroValue] = {
    new AvroRecordKeyValueStore[K](kvStore.open())
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
      kvStore: JAvroKVRecordKeyValueStore[_ <: Any, _ <: Any]
  ): KeyValueStore[K, V] = {
    new AvroKVRecordKeyValueStore[K, V](kvStore.open())
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
    new TextFileKeyValueStore(kvStore.open())
  }
}
