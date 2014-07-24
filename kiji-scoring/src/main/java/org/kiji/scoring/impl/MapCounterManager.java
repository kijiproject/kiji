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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.scoring.CounterManager;

/** A counter manager which stores counter values in a Map. */
@ApiAudience.Private
public final class MapCounterManager implements CounterManager {

  /**
   * Create a new MapCounterManager.
   *
   * @return a new MapCounterManager.
   */
  public static MapCounterManager create() {
    return new MapCounterManager();
  }

  private final Map<String, Map<String, Long>> mCounters = Maps.newHashMap();

  /** Private constructor. Use {@link #create()}. */
  private MapCounterManager() { }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final Enum<?> counter,
      final long value
  ) {
    synchronized (mCounters) {
      incrementCounter(counter.getDeclaringClass().getName(), counter.name(), value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final String group,
      final String name,
      final long value
  ) {
    synchronized (mCounters) {
      final Map<String, Long> groupMap = mCounters.get(group);
      if (null != groupMap) {
        final Long oldValue = groupMap.get(name);
        if (null != oldValue) {
          final Long newValue = oldValue + value;
          groupMap.put(name, newValue);
        } else {
          groupMap.put(name, value);
        }
      } else {
        final Map<String, Long> newGroupMap = Maps.newHashMap();
        newGroupMap.put(name, value);
        mCounters.put(group, newGroupMap);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final Enum<?> counter
  ) {
    synchronized (mCounters) {
      return getCounterValue(counter.getDeclaringClass().getName(), counter.name());
    }
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final String group,
      final String name
  ) {
    synchronized (mCounters) {
      final Map<String, Long> groupMap = mCounters.get(group);
      if (null != groupMap) {
        return groupMap.get(name);
      } else {
        return null;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getGroups() {
    synchronized (mCounters) {
      return mCounters.keySet();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getCountersInGroup(
      final String group
  ) {
    synchronized (mCounters) {
      final Map<String, Long> groupMap = mCounters.get(group);
      if (null != groupMap) {
        return groupMap.keySet();
      } else {
        return Collections.emptySet();
      }
    }
  }
}
