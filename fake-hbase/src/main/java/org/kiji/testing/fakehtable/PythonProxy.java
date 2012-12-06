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

package org.kiji.testing.fakehtable;

import java.lang.reflect.InvocationTargetException;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * Forwards method calls to an arbitrary handler.
 *
 * This is part of the ugly mess that allows to provide a fake HBaseAdmin implementation that
 * appears as an instance of the concrete class HBaseAdmin. Because interfaces are for bunnies...
 *
 * This class apparently needs to be a Java class, and cannot be Scala, otherwise the forwarded
 * method invocation fails with an illegal parameter error.
 *
 * @param <T> class of the handler.
 */
public class PythonProxy<T> implements MethodInterceptor {
  /** Target handler for intercepted/proxied method calls. */
  private final T mTarget;

  /**
   * Initialises the proxy.
   *
   * @param target Target handler for intercepted/proxied method calls.
   */
  public PythonProxy(T target) {
    mTarget = target;
  }

  @Override
  public Object intercept(
      Object self,
      java.lang.reflect.Method method,
      Object[] args,
      MethodProxy proxy)
      throws Throwable {
    // Forwards the method call to the underlying handler, through reflection:
    try {
      return mTarget.getClass()
          .getMethod(method.getName(), method.getParameterTypes())
          .invoke(mTarget, args);
    } catch (InvocationTargetException ite) {
      throw ite.getCause();
    }
  }
}
