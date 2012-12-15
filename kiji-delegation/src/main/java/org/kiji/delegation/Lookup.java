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

package org.kiji.delegation;

import java.util.Iterator;

/**
 * Allows you to lookup provider implementations of interfaces or abstract classes.
 * This is used for late binding / dependency injection throughout the Kiji project.
 * You can use this mechanism to discover runtime-provided implementations of
 * necessary interfaces or services.
 *
 * <p>To use this system:</p>
 * <ol>
 *   <li>Create an interface you want to dynamically bind (<tt>public interface IFoo</tt>).
 *       You can also use an abstract class here.</li>
 *   <li>Create one or more implementations of this interface.
 *       (<tt>public class MyFoo implements IFoo</tt>) This probably happens in another
 *       module that depends on the <tt>IFoo</tt> API.</li>
 *   <li>The module declaring <tt>MyFoo</tt> includes a file named
 *   <tt>src/main/resources/META-INF/services/IFoo</tt>. It contains the word <tt>MyFoo</tt>
 *       on a single line. If you have multiple <tt>IFoo</tt> implementations in your
 *       module, list them all, one per line.</li>
 *   <li>At runtime, a class that needs a concrete implementation of <tt>IFoo</tt> could
 *       run: <tt>IFoo fooInstance = Lookups.get(IFoo.class).lookup()</tt>.</li>
 *   <li>You will get an unchecked {@link NoSuchProviderException} if no suitable <tt>IFoo</tt>
 *       instance could be found.</li>
 * </ol>
 *
 * @param <T> the type that this Lookup instance provides.
 */
public abstract class Lookup<T> implements Iterable<T> {

  /**
   * Create a Lookup instance. Package-protected c'tor; clients should use the
   * get() factory methods to get an appropriate instance.
   */
  Lookup() {
  }

  /**
   * Lookup a provider instance for the specified clazz and return an instance.
   *
   * @return a provider instance for clazz we discover.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  public abstract T lookup();

  /**
   * @return an iterator of all provider instances for the specified class.
   */
  public abstract Iterator<T> iterator();

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface.
   *
   * @param <U> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <U> Lookup<U> get(Class<U> clazz) {
    return get(clazz, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface.
   *
   * @param <U> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the specified classloader.
   */
  public static <U> Lookup<U> get(Class<U> clazz, ClassLoader classLoader) {
    return new BasicLookup<U>(clazz, classLoader);
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a priority method to establish which is the best
   * provider. See {@link PriorityProvider}.
   *
   * @param <U> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <U extends PriorityProvider> Lookup<U> getPriority(
      Class<U> clazz) {
    return getPriority(clazz, Thread.currentThread().getContextClassLoader());
  }


  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a priority method to establish which is the best
   * provider. See {@link PriorityProvider}.
   *
   * @param <U> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <U extends PriorityProvider> Lookup<U> getPriority(
      Class<U> clazz, ClassLoader classLoader) {
    return new PriorityLookup<U>(clazz, classLoader);
  }
}
