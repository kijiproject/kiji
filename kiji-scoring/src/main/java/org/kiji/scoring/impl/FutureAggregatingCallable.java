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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;

/**
 * Callable which collects the return values from a list of Futures into a list of those values.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 *
 * @param <T> type of the value returned by the Futures.
 */
@ApiAudience.Private
final class FutureAggregatingCallable<T> implements Callable<List<T>> {

  private final ImmutableList<Future<T>> mFutures;

  /**
   * Initialize a new FutureAggregatingCallable.
   *
   * @param futures asynchronously calculated values to be collected.
   */
  public FutureAggregatingCallable(
      final List<Future<T>> futures
  ) {
    mFutures = ImmutableList.copyOf(futures);
  }

  /** {@inheritDoc} */
  @Override
  public List<T> call() throws Exception {
    final List<T> collectedResults = Lists.newArrayList();
    for (Future<T> future : mFutures) {
      collectedResults.add(ScoringUtils.getFromFuture(future));
    }
    return collectedResults;
  }
}
