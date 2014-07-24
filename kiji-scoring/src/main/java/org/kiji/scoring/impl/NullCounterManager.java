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
package org.kiji.scoring.impl;

import java.util.Collections;
import java.util.Set;

import org.kiji.annotations.ApiAudience;
import org.kiji.scoring.CounterManager;

/** A counter manager which stores no counters. */
@ApiAudience.Private
public final class NullCounterManager implements CounterManager {

  private static final NullCounterManager SINGLETON = new NullCounterManager();

  /**
   * Get the singleton NullCounterManager.
   *
   * @return the singleton NullCounterManager.
   */
  public static NullCounterManager get() {
    return SINGLETON;
  }

  /** Private constructor. Use {@link #get()}. */
  private NullCounterManager() { }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final Enum<?> counter,
      final long value
  ) { }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final String group,
      final String name,
      final long value
  ) { }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final Enum<?> counter
  ) {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final String group,
      final String name
  ) {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getGroups() {
    return Collections.emptySet();
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getCountersInGroup(
      final String group
  ) {
    return Collections.emptySet();
  }
}
