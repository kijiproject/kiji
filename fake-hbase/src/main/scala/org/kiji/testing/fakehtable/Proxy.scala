/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.testing.fakehtable

import org.easymock.internal.ClassInstantiatorFactory

import net.sf.cglib.proxy.Callback
import net.sf.cglib.proxy.Enhancer
import net.sf.cglib.proxy.Factory
import net.sf.cglib.proxy.MethodInterceptor

/** Equivalent of java.lang.reflect.Proxy that allows proxying concrete classes. */
object Proxy {
  /**
   * Creates a proxy to a given concrete class.
   *
   * @param klass Concrete class to proxy.
   * @param handler Handler processing the method calls.
   * @return a new proxy for the specified class.
   */
  def create[T](klass: Class[_], handler: MethodInterceptor): T = {
    // Don't ask me how this work...
    val enhancer = new Enhancer()
    enhancer.setSuperclass(klass)
    enhancer.setInterceptDuringConstruction(true)
    enhancer.setCallbackType(handler.getClass)
    val proxyClass: Class[_] = enhancer.createClass()
    val proxy = ClassInstantiatorFactory.getInstantiator().newInstance(proxyClass)
        .asInstanceOf[Factory]
    proxy.setCallbacks(Array[Callback](handler))
    return proxy.asInstanceOf[T]
  }
}