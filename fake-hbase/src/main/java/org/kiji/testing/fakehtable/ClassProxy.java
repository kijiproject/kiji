/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.testing.fakehtable;

import org.easymock.internal.ClassInstantiatorFactory;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;

/** Equivalent of java.lang.reflect.Proxy that allows proxying concrete classes. */
class ClassProxy {
  /** Utility class may not be instantiated. */
  private ClassProxy() {
  }

  /**
   * Creates a proxy to a given concrete class.
   *
   * @param klass Concrete class to proxy.
   * @param handler Handler processing the method calls.
   * @return a new proxy for the specified class.
   * @throws InstantiationException on error.
   */
  @SuppressWarnings("unchecked")
  public static <T> T create(Class<?> klass, MethodInterceptor handler)
      throws InstantiationException {
    // Don't ask me how this work...
    final Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(klass);
    enhancer.setInterceptDuringConstruction(true);
    enhancer.setCallbackType(handler.getClass());
    final Class<?> proxyClass = enhancer.createClass();
    final Factory proxy =
        (Factory) ClassInstantiatorFactory.getInstantiator().newInstance(proxyClass);
    proxy.setCallbacks(new Callback[] { handler });
    return (T) proxy;
  }
}