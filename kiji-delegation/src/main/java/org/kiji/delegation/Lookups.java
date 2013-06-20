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

package org.kiji.delegation;

import org.kiji.annotations.ApiAudience;

/**
 * Factory methods that create {@link Lookup} instances that allow
 * you to lookup provider implementations of interfaces or abstract classes.
 */
@ApiAudience.Framework
public final class Lookups {
  /**
   * Private constructor for utility class.
   */
  private Lookups() {
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <T> Lookup<T> get(Class<T> clazz) {
    return get(clazz, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the specified classloader.
   */
  public static <T> Lookup<T> get(Class<T> clazz, ClassLoader classLoader) {
    return new BasicLookup<T>(clazz, classLoader);
  }

  /**
   * Creates a BasicLookup instance that can resolve providers for the specified class
   * or interface. For use by other Lookups in this package.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the specified classloader.
   */
  static <T> BasicLookup<T> getBasic(Class<T> clazz, ClassLoader classLoader) {
    return new BasicLookup<T>(clazz, classLoader);
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a priority method to establish which is the best
   * provider. See {@link PriorityProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <T extends PriorityProvider> Lookup<T> getPriority(
      Class<T> clazz) {
    return getPriority(clazz, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a priority method to establish which is the best
   * provider. See {@link PriorityProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <T extends PriorityProvider> Lookup<T> getPriority(
      Class<T> clazz, ClassLoader classLoader) {
    return new PriorityLookup<T>(clazz, classLoader);
  }


  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface by using a name unique to each implementation. See {@link NamedProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <T extends NamedProvider> NamedLookup<T> getNamed(
      Class<T> clazz) {
    return getNamed(clazz, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a name unique to each implementation. See {@link NamedProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the specified classLoader.
   */
  public static <T extends NamedProvider> NamedLookup<T> getNamed(
      Class<T> clazz, ClassLoader classLoader) {
    return new NamedLookup<T>(clazz, classLoader);
  }


  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a configuration file or resouce. See {@link ConfiguredProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @return a new Lookup instance that uses the current thread's context classloader.
   */
  public static <T> Lookup<T> getConfigured(Class<T> clazz) {
    return getConfigured(clazz, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a lookup instance that can resolve providers for the specified class
   * or interface, by using a configuration file or resouce. See {@link ConfiguredProvider}.
   *
   * @param <T> the type that this Lookup instance should provide.
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use to resolve service lookups.
   * @return a new Lookup instance that uses the specified classLoader.
   */
  public static <T> Lookup<T> getConfigured(Class<T> clazz, ClassLoader classLoader) {
    return new ConfiguredLookup<T>(clazz, classLoader);
  }

}
