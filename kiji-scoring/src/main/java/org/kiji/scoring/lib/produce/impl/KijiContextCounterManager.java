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
package org.kiji.scoring.lib.produce.impl;

import java.util.Set;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiContext;
import org.kiji.scoring.CounterManager;

/** A counter manger which stores counters in a KijiContext. */
@ApiAudience.Private
public final class KijiContextCounterManager implements CounterManager {

  /**
   * Create a new KijiContextCounterManager.
   *
   * @param context KijiContext in which to store counters.
   * @return a new KijiContextCounterManager.
   */
  public static KijiContextCounterManager create(
      final KijiContext context
  ) {
    return new KijiContextCounterManager(context);
  }

  private final KijiContext mContext;

  /**
   * Private constructor. Use {@link #create(org.kiji.mapreduce.KijiContext)}.
   *
   * @param context KijiContext in which to store counters.
   */
  private KijiContextCounterManager(
      final KijiContext context
  ) {
    mContext = context;
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final Enum<?> counter,
      final long value
  ) {
    mContext.incrementCounter(counter, value);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final String group,
      final String name,
      final long value
  ) {
    throw new UnsupportedOperationException(String.format(
        "%s does not support incrementCounter(String, String, long)", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final Enum<?> counter
  ) {
    throw new UnsupportedOperationException(String.format(
        "%s does not support getCounterValue(Enum)", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final String group,
      final String name
  ) {
    throw new UnsupportedOperationException(String.format(
        "%s does not support getCounterValue(String, String)", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getGroups() {
    throw new UnsupportedOperationException(String.format(
        "%s does not support getGroups()", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getCountersInGroup(
      final String group
  ) {
    throw new UnsupportedOperationException(String.format(
        "%s does not support getCountersInGroup(String)", getClass().getName()));
  }
}
