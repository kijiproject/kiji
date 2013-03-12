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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * A {@link Lookup} instance that selects an instance of an abstract
 * service or interface based on its declaration of a static name that
 * it is bound to.
 *
 * <p>For example, a suite of tools could implement a common
 * base tool class that declares a name for each tool (through the
 * {@link NamedProvider} interface).</p>
 *
 * See the {@link NamedProvider} API for more details.
 *
 * @param <T> the interface type this Lookup instance is creating; this
 * must subclass {@link NamedProvider}.
 */
@ApiAudience.Private
public final class NamedLookup<T extends NamedProvider> extends Lookup<T> {
  private static final Logger LOG = LoggerFactory.getLogger(NamedLookup.class);

  /** Underlying Lookup instance that does the heavy lifting. */
  private final BasicLookup<T> mLookup;

  /**
   * Create a NamedLookup instance. Package-private c'tor; clients should use the
   * Lookup.getNamed() factory methods.
   *
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use.
   */
  NamedLookup(Class<T> clazz, ClassLoader classLoader) {
    mLookup = Lookups.getBasic(clazz, classLoader);
  }

  /**
   * Lookup a provider instance for the specified clazz. Returns an arbitrary
   * instance.
   *
   * @return an arbitrary instance of the interface.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup() {
    return lookup(Collections.<String, String>emptyMap());
  }

  /**
   * Lookup a provider instance for the specified clazz. Returns an arbitrary
   * instance.
   *
   * @param runtimeHints parameters that describe the environment. These
   *     are ignored.
   * @return an arbitrary instance of the interface.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup(Map<String, String> runtimeHints) {
    return mLookup.lookup(runtimeHints);
  }

  /**
   * Looks up a provider instance by name.
   *
   * @param name the name of the provider implementation to load.
   * @return the implementation that declares that name through the NamedProvider
   *     interface.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found with that name.
   */
  public T lookup(String name) {
    if (null == name) {
      throw new NoSuchProviderException("Cannot lookup a provider with null name "
          + "when doing lookup for class " + mLookup.getLookupClass().getName());
    }

    for (T provider : this) {
      if (name.equals(provider.getName())) {
        return provider;
      }
    }

    throw new NoSuchProviderException("No provider found with name '" + name
        + "' when doing lookup for class " + mLookup.getLookupClass().getName());
  }

  /**
   * Checks to see if multiple providers declare the same name.
   *
   * @return true if multiple provider implementations return the same name
   *     from getName().
   */
  public boolean hasDuplicates() {
    Set<String> names = new HashSet<String>();
    for (T provider : this) {
      String name = provider.getName();
      if (!names.add(name)) {
        LOG.debug("Duplicate name: '" + name + "' when doing lookup for class "
            + mLookup.getLookupClass().getName());
        return true;
      }
    }

    return false;
  }

  /**
   * @return an iterator of all valid provider instances for the specified class.
   */
  @Override
  public Iterator<T> iterator() {
    return iterator(Collections.<String, String>emptyMap());
  }

  /**
   * Returns an iterator of all valid provider instances for the specified class.
   *
   * @param runtimeHints parameters that describe the environment. These
   *     are ignored.
   * @return an iterator of all valid provider instances for the specified class.
   */
  @Override
  public Iterator<T> iterator(Map<String, String> runtimeHints) {
    return mLookup.iterator(runtimeHints);
  }
}
