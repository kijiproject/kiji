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

import java.util.Map;

import org.kiji.annotations.ApiAudience;

/**
 * A service that can be selected based on its relative priority.
 *
 * <p>{@link Lookup} is often used to select a "factory service" that actually
 * creates an instance of a real class. This level of indirection is often
 * necessary if you do not want to actually classload multiple providers of the
 * "true" service into the same JVM, because they may both compete for resources,
 * or trigger further classloading of incompatible class definitions.</p>
 *
 * <p>Where one or more factory services may all potentially create incompatible
 * versions of a true interface, an arbitration mechanism (called a <em>priority</em>)
 * is used to select the best factory service available at runtime.</p>
 *
 * <p>This interface is used by the {@link PriorityLookup} implementation.
 * All factory service implementations (which implement PriorityProvider)
 * are loaded by the PriorityLookup instance; they are each queried for their
 * priority -- how confident they are that they can provide the best implementation
 * for the current runtime environment. The instance that volunteers the highest priority
 * is then selected. This factory instance can then classload and create the true
 * service instance using its own API.</p>
 */
@ApiAudience.Public
public interface PriorityProvider {
  /**
   * Returns the priority that this instance believes it has. Higher priorities
   * indicate a better fit for the current runtime environment. The instance
   * may inspect as much of the runtime environment as it can to establish its fitness;
   * classes may also provide a collection of runtime "hints" that describe the
   * environment more precisely.
   *
   * <p>A priority of 0 or less indicates that the instance will not be used. The
   * highest priority instance is used.</p>
   *
   * @param runtimeHints hints provided by the client that establish the parameters
   *     of the current runtime environment.
   * @return a value greater than or equal to zero indicating the confidence that this
   *     is the best instance to use.
   */
  int getPriority(Map<String, String> runtimeHints);
}
