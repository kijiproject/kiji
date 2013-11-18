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

package org.kiji.express.flow.framework.serialization

import com.esotericsoftware.kryo.io.Output
import com.twitter.bijection.Injection
import com.twitter.chill.KryoBase
import com.twitter.chill.KryoInjectionInstance
import org.objenesis.strategy.StdInstantiatorStrategy

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance

/**
 * Provides a constructor function for a
 * [[org.kiji.express.flow.framework.serialization.KijiLocker]].
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
// TODO (EXP-295): Should these maybe be Framework?
object KijiLocker {
  def apply[T <: AnyRef](t: T): KijiLocker[T] = new KijiLocker(t)
}

/**
 * A clone of Chill's [[com.twitter.chill.MeatLocker]] with serialization provided by our custom
 * [[org.kiji.express.flow.framework.serialization.KryoKiji]].
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
// TODO (EXP-295): Should these maybe be Framework?
class KijiLocker[T <: AnyRef](@transient private var t: T) extends java.io.Serializable {

  /**
   * Creates an [[com.twitter.bijection.Injection]] for converting objects between their
   * serialized byte for and back.
   *
   * @return an Object <-> Array[Byte] Injection.
   */
  private def injection: Injection[AnyRef, Array[Byte]] = {
    val kryo = {
      val kryo = new KryoBase
      kryo.setRegistrationRequired(false)
      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy)
      new KryoKiji().decorateKryo(kryo)
      kryo
    }
    new KryoInjectionInstance(kryo, new Output( 1 << 10, 1 << 24))
  }

  /**
   * Serialized value of t.
   */
  private val tBytes: Array[Byte] = injection(t)

  /**
   * Retrieve the value wrapped by this
   * [[org.kiji.express.flow.framework.serialization.KijiLocker]].
   *
   * @return the value
   */
  def get: T = {
    if(t == null) {
      // we were serialized
      t = injection
          .invert(tBytes)
          .getOrElse(throw new RuntimeException("Deserialization failed in KijiLocker."))
          .asInstanceOf[T]
    }
    t
  }
}

