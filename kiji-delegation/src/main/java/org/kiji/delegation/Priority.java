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
 * Canonical priority levels for use by PriorityProvider implementations.
 */
@ApiAudience.Framework
public final class Priority {
  /** Private constructor; utility class should not be instantiated. */
  private Priority() {
  }

  /** Maximum legal priority value. Trumps all other providers. */
  public static final int MAX = Integer.MAX_VALUE;

  /**
   * A priority level indicating that this provider should be preferred
   * except in extenuating circumstances.
   */
  public static final int VERY_HIGH = 50000;

  /**
   * A priority level indicating that a service provider is better-suited
   * than most providers indicating "Normal" priority.
   */
  public static final int HIGH = 2000;

  /** The priority level that most instances should return. */
  public static final int NORMAL = 1000;

  /**
   * A priority level indicating that a service provider can fulfill the
   * runtime requirements, but should defer to other providers advertising
   * normal priority level and capability.
   */
  public static final int LOW = 500;

  /**
   * A priority level indicating that a service provider has minimal
   * ability to fulfill the runtime requirements, and may only do so partially.
   */
  public static final int VERY_LOW = 100;

  /** The lowest priority that could still be active. */
  public static final int MIN = 1;

  /** A priority value indicating that a provider should not be used. */
  public static final int DISABLED = 0;
}
