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
package org.kiji.scoring.batch.impl;

import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;

import org.kiji.annotations.ApiAudience;
import org.kiji.scoring.CounterManager;

/**
 * A counter manager which stores counters in a Hadoop Mapper.Context.
 *
 * @param <INK> Type of the input keys of the Mapper.
 * @param <INV> Type of the input values of the Mapper.
 * @param <OUTK> Type of the output keys of the Mapper.
 * @param <OUTV> Type of the output values of the Mapper.
 */
@ApiAudience.Private
public final class MapperContextCounterManager<INK, INV, OUTK, OUTV> implements CounterManager {

  /**
   * Create a new MapperContextCounterManager.
   *
   * @param context Mapper.Context with which to store counters.
   * @param <INK> Type of the input keys of the Mapper.
   * @param <INV> Type of the input values of the Mapper.
   * @param <OUTK> Type of the output keys of the Mapper.
   * @param <OUTV> Type of the output values of the Mapper.
   * @return a new MapperContextCounterManager.
   */
  public static <INK, INV, OUTK, OUTV> MapperContextCounterManager create(
      final Mapper<INK, INV, OUTK, OUTV>.Context context
  ) {
    return new MapperContextCounterManager(context);
  }

  private final Mapper<INK, INV, OUTK, OUTV>.Context mContext;

  /**
   * Private constructor. Use {@link #create(org.apache.hadoop.mapreduce.Mapper.Context)}.
   *
   * @param context Mapper.Context with which to store counters.
   */
  private MapperContextCounterManager(
      final Mapper<INK, INV, OUTK, OUTV>.Context context
  ) {
    mContext = context;
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final Enum<?> counter,
      final long value
  ) {
    mContext.getCounter(counter).increment(value);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final String group,
      final String name,
      final long value
  ) {
    mContext.getCounter(group, name).increment(value);
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final Enum<?> counter
  ) {
    return mContext.getCounter(counter).getValue();
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final String group,
      final String name
  ) {
    return mContext.getCounter(group, name).getValue();
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getGroups() {
    throw new UnsupportedOperationException(
        String.format("%s does not support getGroups().", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getCountersInGroup(
      final String group
  ) {
    throw new UnsupportedOperationException(
        String.format("%s does not support getCountersInGroup(String).", getClass().getName()));
  }
}
