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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.scoring.CounterManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;

/**
 * Encapsulation of all state necessary to perform freshening for a single column.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class Freshener implements ReferenceCountable<Freshener> {

  private final KijiFreshnessPolicy mPolicy;
  private final ScoreFunction<?> mScoreFunction;
  private final KeyValueStoreReaderFactory mFactory;
  private final KijiColumnName mAttachedColumn;
  private final AtomicInteger mRetainCounter = new AtomicInteger(1);
  private final Map<String, String> mParameters;
  private final CounterManager mCounterManager;

  /**
   * Initialize a new Freshener.
   *
   * @param policy the KijiFreshnessPolicy which governs this Freshener.
   * @param scoreFunction the ScoreFunction which generates scores for this Freshener.
   * @param factory the KVStoreReaderFactory which services the policy and score function.
   * @param attachedColumn the column to which this Freshener is attached.
   * @param parameters configuration parameters retrieved from the Freshener record which are
   *     accessible to the freshness policy and score function via
   *     {@link org.kiji.scoring.FreshenerContext#getParameters()} method.
   * @param counterManager CounterManager with which to create FreshenerContexts.
   */
  public Freshener(
      final KijiFreshnessPolicy policy,
      final ScoreFunction<?> scoreFunction,
      final KeyValueStoreReaderFactory factory,
      final KijiColumnName attachedColumn,
      final Map<String, String> parameters,
      final CounterManager counterManager
  ) {
    mPolicy = policy;
    mScoreFunction = scoreFunction;
    mFactory = factory;
    mAttachedColumn = attachedColumn;
    mParameters = parameters;
    mCounterManager = counterManager;
  }

  /**
   * Get the freshness policy used by this Freshener.
   *
   * @return the freshness policy used by this Freshener.
   */
  public KijiFreshnessPolicy getFreshnessPolicy() {
    return mPolicy;
  }

  /**
   * Get the ScoreFunction used by this Freshener.
   *
   * @return the ScoreFunction used by this Freshener.
   */
  public ScoreFunction<?> getScoreFunction() {
    return mScoreFunction;
  }

  /**
   * Get the KeyValueStoreReaderFactory from this Freshener.
   *
   * @return the KeyValueStoreReaderFactory from this Freshener.
   */
  public KeyValueStoreReaderFactory getKVStoreReaderFactory() {
    return mFactory;
  }

  /**
   * Get the attached column of this Freshener.
   *
   * @return the attached column of this Freshener.
   */
  public KijiColumnName getAttachedColumn() {
    return mAttachedColumn;
  }

  /**
   * Get the parameters of this Freshener.
   *
   * @return the parameters of this Freshener.
   */
  public Map<String, String> getParameters() {
    return mParameters;
  }

  /** {@inheritDoc} */
  @Override
  public Freshener retain() {
    final int counter = mRetainCounter.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain closed Freshener: %s retain counter was %s.",
        toString(), counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    final int counter = mRetainCounter.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release closed Freshener: %s retain counter is now %s.",
        toString(), counter);
    if (counter == 0) {
      close();
    }
  }

  /**
   * Cleanup contained resources.  Should only be called by {@link #release()}
   *
   * @throws IOException in case of an error cleaning up the score function.
   */
  private void close() throws IOException {
    final InternalFreshenerContext cleanupContext =
        InternalFreshenerContext.create(mAttachedColumn, mParameters, mCounterManager, mFactory);
    mScoreFunction.cleanup(cleanupContext);
    mPolicy.cleanup(cleanupContext);
    mFactory.close();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("attached_column", mAttachedColumn.toString())
        .add("policy_class", mPolicy.getClass().getName())
        .add("score_function_class", mScoreFunction.getClass().getName())
        .add("parameters", mParameters)
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mPolicy, mScoreFunction, mFactory, mAttachedColumn, mParameters);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(
      final Object obj
  ) {
    if (obj == null) {
      return false;
    }
    if (!Objects.equal(this.getClass(), obj.getClass())) {
      return false;
    }
    final Freshener that = (Freshener) obj;
    return Objects.equal(this.mPolicy, that.mPolicy)
        && Objects.equal(this.mScoreFunction, that.mScoreFunction)
        && Objects.equal(this.mFactory, that.mFactory)
        && Objects.equal(this.mAttachedColumn, that.mAttachedColumn)
        && Objects.equal(this.mParameters, that.mParameters);
  }
}
