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

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.EntityId
import org.kiji.express.util.AvroUtil
import org.kiji.schema.KijiRowKeyComponents

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
@Inheritance.Sealed
private[modeling] trait ScalaToJavaKeyConverter[K] {
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
@Inheritance.Sealed
private[modeling] trait JavaToScalaValueConverter[V] {
  /**
   * Converts a value specified as a Java type to an equivalent value as a native Scala type.
   *
   * @param valueWithJavaType the value to convert.
   * @return the value, as a native Scala type.
   */
  protected def valueConversion(valueWithJavaType: Any): V
}

/**
 * Converts a sequence of components for a Kiji entity id to an instance of
 * `KijiRowKeyComponents` that can be used as a key to a Kiji table key-value store.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[modeling] trait EntityIdScalaToJavaKeyConverter
    extends ScalaToJavaKeyConverter[EntityId] {
  /**
   * Converts a sequence of entity id components to an instance of `KijiRowKeyComponents`,
   * suitable for use with a `KijiTableKeyValueStore`
   *
   * @param keyWithScalaType is an EntityId to convert to a scala type.
   * @return is the equivalent `KijiRowKeyComponents`.
   */
  override protected def keyConversion(keyWithScalaType: EntityId): Any = {
    // KijiRowKeyComponents can use Java byte array, Java String, Java Long,
    // or Java Integer as components. As long as we ensure all components are AnyRef,
    // then Scala Array[Byte], Scala String, Scala Long, and Scala Int are usable to create a
    // KijiRowKeyComponents. We do this conversion then create the KijiRowKeyComponents.
    val components = keyWithScalaType
        .components
        .asJava
    KijiRowKeyComponents.fromComponentsList(components)
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
@Inheritance.Sealed
private[modeling] trait AvroScalaToJavaKeyConverter[K] extends ScalaToJavaKeyConverter[K] {
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
@Inheritance.Sealed
private[modeling] trait AvroJavaToScalaValueConverter[V] extends JavaToScalaValueConverter[V] {
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
