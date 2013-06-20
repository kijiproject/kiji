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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kiji.annotations.ApiAudience;

/**
 * A {@link Lookup} instance that selects the best instance of an abstract
 * service or interface based on its declaration of its fitness for the
 * runtime environment, as returned through its priority.
 *
 * See the {@link PriorityProvider} API for more details.
 *
 * @param <T> the interface type this Lookup instance is creating; this
 * must subclass {@link PriorityProvider}.
 */
@ApiAudience.Private
final class PriorityLookup<T extends PriorityProvider> extends Lookup<T> {
  /** Underlying Lookup instance that does the heavy lifting. */
  private final Lookup<T> mLookup;

  /**
   * Create a PriorityLookup instance. Package-private c'tor; clients should use the
   * Lookup.getPriorityLookup() factory methods.
   *
   * @param clazz the abstract class or interface to lookup a provider for.
   * @param classLoader the classloader to use.
   */
  PriorityLookup(Class<T> clazz, ClassLoader classLoader) {
    mLookup = Lookups.get(clazz, classLoader);
  }

  /**
   * Lookup a provider instance for the specified clazz and return the highest-priority
   * instance we can create.
   *
   * @return the highest-priority provider instance for clazz we discover.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup() {
    return lookup(Collections.<String, String>emptyMap());
  }

  /**
   * Lookup a provider instance for the specified clazz and return the highest-priority
   * instance we can create.
   *
   * @param runtimeHints parameters that describe the environment so the factories
   *     can evaluate their relative priorities more precisely.
   * @return the highest-priority provider instance for clazz we discover.
   * @throws NoSuchProviderException if no runtime provider for the interface
   *     can be found.
   */
  @Override
  public T lookup(Map<String, String> runtimeHints) {
    int bestPriority = 0;
    T bestProvider = null;
    for (T provider : mLookup) {
      int thisPriority = provider.getPriority(runtimeHints);
      if (thisPriority > bestPriority) {
        bestPriority = thisPriority;
        bestProvider = provider;
      }
    }

    return bestProvider;
  }

  /**
   * @return an iterator of all valid provider instances for the specified class,
   * sorted by priority. This filters out any provider instances with a priority
   * of 0 or less.
   */
  @Override
  public Iterator<T> iterator() {
    return iterator(Collections.<String, String>emptyMap());
  }

  /**
   * Returns an iterator of all valid provider instances for the specified class,
   * sorted by priority. This filters out any provider instances with a priority
   * of 0 or less.
   *
   * @param runtimeHints parameters that describe the environment so the factories
   *     can evaluate their relative priorities more precisely.
   * @return an iterator of all valid provider instances for the specified class,
   * sorted by priority.
   */
  @Override
  public Iterator<T> iterator(Map<String, String> runtimeHints) {
    final Map<T, Integer> sortingMap = new HashMap<T, Integer>();
    final List<T> lst = new ArrayList<T>();

    // Match up providers with priorities, retaining only ones with priorities > 0.
    for (T provider : mLookup) {
      int priority = provider.getPriority(runtimeHints);
      if (priority > 0) {
        sortingMap.put(provider, priority);
        lst.add(provider);
      }
    }

    // Perform an external sort on the list, using their priorities.
    // Note that Collections.sort() puts the items in ascending order
    // according to the comparator. Our comparator is thus reversed
    // with respect to provider priorities, so that we get the items
    // in descending order.
    final Comparator<T> comparator = new Comparator<T>() {

      /** {@inheritDoc} */
      @Override
      public int compare(T provider1, T provider2) {
        if (provider1 == provider2) {
          return 0;
        }

        int priority1 = sortingMap.get(provider1);
        int priority2 = sortingMap.get(provider2);
        if (priority1 > priority2) {
          return -1;
        } else if (priority1 == priority2) {
          return 0;
        } else {
          assert priority1 < priority2;
          return 1;
        }
      }

      /** {@inheritDoc} */
      @Override
      public boolean equals(Object obj) {
        return obj == this;
      }

      /** {@inheritDoc} */
      @Override
      public int hashCode() {
        // Method provided to prevent checkstyle warning, but we never expect
        // to hash this comparator in a clever way. They're all the same.
        return 42;
      }
    };
    Collections.sort(lst, comparator);
    return lst.iterator();
  }
}
