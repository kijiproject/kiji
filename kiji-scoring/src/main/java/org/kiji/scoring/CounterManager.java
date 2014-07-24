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
package org.kiji.scoring;

import java.util.Set;

/**
 * Manages incrementing and storing the value of counters.
 *
 * This interface allows KijiFreshnessPolicy and ScoreFunction instances to log counters in a
 * context agnostic way. Specific implementations are provided for policies and ScoreFunctions
 * running in a FreshKijiTableReader, in a MapReduce job via a Hadoop Mapper and in a MapReduce job
 * via a KijiProducer.
 */
public interface CounterManager {
  /**
   * Increment the value of the named counter by the given value.
   *
   * @param counter Name of the counter to increment.
   * @param value Value by which to increment the counter.
   */
  void incrementCounter(Enum<?> counter, long value);

  /**
   * Increment the value of the named counter by the given value.
   *
   * <p>
   *   This method is optional. Some implementations may throw
   *   {@link UnsupportedOperationException}.
   * </p>
   *
   * @param group Group of the counter to increment.
   * @param name Name of the counter to increment.
   * @param value Value by which to increment the counter.
   */
  void incrementCounter(String group, String name, long value);

  /**
   * Get the current value of the named counter.
   *
   * <p>
   *   This method is optional. Some implementations may throw
   *   {@link UnsupportedOperationException}.
   * </p>
   *
   * @param counter Name of the counter for which to get the current value.
   * @return The current value of the named counter or null if the counter has never been set.
   */
  Long getCounterValue(Enum<?> counter);

  /**
   * Get the current value of the named counter.
   *
   * <p>
   *   This method is optional. Some implementations may throw
   *   {@link UnsupportedOperationException}.
   * </p>
   *
   * @param group Group of the counter for which to get the current value.
   * @param name Name of the counter for which to get the current value.
   * @return The current value of the named counter or null if the counter has never been set.
   */
  Long getCounterValue(String group, String name);

  /**
   * Get all counter groups tracked by this CounterManager.
   *
   * <p>
   *   This method is optional. Some implementations may throw
   *   {@link UnsupportedOperationException}.
   * </p>
   *
   * @return all counter groups tracked by this CounterManager.
   */
  Set<String> getGroups();

  /**
   * Get the names of all counters tracked by this CounterManager in the given group.
   *
   * <p>
   *   This method is optional. Some implementations may throw
   *   {@link UnsupportedOperationException}.
   * </p>
   *
   * @param group The name of the group from which to get the list of counters.
   * @return the names of all counters tracked by this CounterManager in the given group.
   */
  Set<String> getCountersInGroup(String group);
}
