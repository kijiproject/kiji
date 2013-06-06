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

package org.kiji.express.modeling

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.AvroUtil
import org.kiji.express.EntityId
import org.kiji.schema.KijiURI

/**
 * Converts keys specified as Scala types to equivalent keys as Java types compatible with the
 * KijiMR key-value store API.
 *
 * Key converters are implemented by KijiExpress key-value stores (which are just a face on the
 * KijiMR key-value store API). The key-value store uses the converter to lookup keys
 * specified as Scala types in a KijiMR key-value store.
 *
 * @tparam K is the type of key users will specify when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] trait ScalaToJavaKeyConverter[K] {
  /**
   * Converts a key specified as a Scala type to an equivalent key as a Java type compatible with
   * the KijiMR key-value store API.
   *
   * @param keyWithScalaType is the key to convert.
   * @return the key, as a Java type suitable for use with a KijiMR key-value store.
   */
  protected def keyConversion(keyWithScalaType: K): Any
}

/**
 * Converts values specified as Java types to equivalent values as Scala types.
 *
 * Value converters are implemented by KijiExpress key-value stores (which are just a face on the
 * KijiMR key-value store API). The key-value store uses the converter to transform values
 * retrieved from a KijiMR key-value store to a native Scala type.
 *
 * @tparam V is the type of value users will retrieve when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] trait JavaToScalaValueConverter[V] {
  /**
   * Converts a value specified as a Java type to an equivalent value as a native Scala type.
   *
   * @param valueWithJavaType the value to convert.
   * @return the value, as a native Scala type.
   */
  protected def valueConversion(valueWithJavaType: Any): V
}

/**
 * Converts KijiExpress entity ids to KijiMR entity ids, suitable for use with a
 * `KijiTableKeyValueStore`.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] trait EntityIdScalaToJavaKeyConverter extends ScalaToJavaKeyConverter[Seq[Any]] {
  /**
   * URI addressing the Kiji table that these entityIds belong to. This must be overridden in any
   * implementing classes.
   */
  def tableUri: KijiURI

  /**
   * Converts a KijiExpress [[org.kiji.express.EntityId]] to a KijiMR entity id,
   * suitable for use with a `KijiTableKeyValueStore`.
   *
   * @param keyWithScalaType is the KijiExpress entity id to convert.
   * @return is the equivalent KijiMR entity id.
   */
  override protected def keyConversion(keyWithScalaType: Seq[Any]): Any = {
    EntityId.fromComponents(tableUri, keyWithScalaType).toJavaEntityId()
  }
}

/**
 * Converts keys specified as Scala/KijiExpress types to equivalent keys with types compatible
 * with the Java Avro API, suitable for use with an Avro-backed KijiMR key-value store.
 *
 * @tparam K is the type of key users will specify when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] trait AvroScalaToJavaKeyConverter[K] extends ScalaToJavaKeyConverter[K] {
  /**
   * Converts a key specified as a Scala/KijiExpress type to an equivalent key with type
   * compatible with the Java Avro API.
   *
   * @param keyWithScalaType is the key to convert.
   * @return the key, as a Java/Avro type suitable for use with a KijiMR key-value store.
   */
  override protected def keyConversion(keyWithScalaType: K): Any = {
    AvroUtil.encodeToJava(keyWithScalaType)
  }
}

/**
 * Converts values specified as Java/Avro types to equivalent values with native Scala types.
 *
 * @tparam V is the type of value users will retrieve when accessing a key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] trait AvroJavaToScalaValueConverter[V] extends JavaToScalaValueConverter[V] {
  /**
   * Converts a value specified as a Java/Avro type to an equivalent value with a native Scala
   * type.
   *
   * @param valueWithJavaType the value to convert.
   * @return the value, as a native Scala type.
   */
  override protected def valueConversion(valueWithJavaType: Any): V = {
    AvroUtil.decodeGenericFromJava(valueWithJavaType).asInstanceOf[V]
  }
}
