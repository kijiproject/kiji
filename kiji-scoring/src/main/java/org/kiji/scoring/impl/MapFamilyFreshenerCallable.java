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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.ScoreFunction.TimestampedValue;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

/**
 * Callable which performs freshening for all requested qualifiers in a specific map-type family.
 * Returns a boolean indicating if any writes were committed.
 *
 * <p>
 *   This class is package private to be used by FresheningRequestContext. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class MapFamilyFreshenerCallable implements Callable<Boolean> {

  /**
   * Return value if the completion of this Freshener caused a write to Kiji which indicates to
   * the framework that there is new data to be read from the table. This is returned if the
   * ScoreFunction is run while partial freshening is enabled and when the last Freshener finishes
   * for a request with partial freshening disabled if any ScoreFunction ran as part of that
   * request.
   */
  private static final boolean WROTE = true;

  /**
   * Return value if the completion of this Freshener did not cause a write to Kiji which
   * indicates to the framework that there is no new data to be read from the table. This is
   * returned if a KijiFreshnessPolicy returns fresh while partial freshening is enabled and when
   * the last Freshener finishes for a request with partial freshening disabled if all policies
   * returned fresh.
   */
  private static final boolean DID_NOT_WRITE = false;

  /** Callable for scoring a single qualifier in the family. */
  private static final class ScoreCallable implements Callable<Boolean> {

    private final Freshener mFreshener;
    private final KijiRowData mDataToScore;
    private final KijiColumnName mStaleQualifier;
    private final FreshenerContext mContext;
    private final FresheningRequestContext mRequestContext;

    /**
     * Initialize a new ScoreCallable.
     *
     * @param freshener Freshener attached to the map family of this callable.
     * @param dataToScore KijiRowData representing the combined inputs of all qualifiers.
     * @param staleQualifier Single qualifiers to be scored by this callable.
     * @param context Context for this qualifier.
     * @param requestContext Context of the entire request.
     */
    private ScoreCallable(
        final Freshener freshener,
        final KijiRowData dataToScore,
        final KijiColumnName staleQualifier,
        final FreshenerContext context,
        final FresheningRequestContext requestContext
    ) {
      mFreshener = freshener;
      mDataToScore = dataToScore;
      mStaleQualifier = staleQualifier;
      mContext = context;
      mRequestContext = requestContext;
    }

    /** {@inheritDoc} */
    @Override
    public Boolean call() throws Exception {
      final SingleBuffer buffer;
      if (mRequestContext.allowsPartial()) {
        buffer = mRequestContext.openUniqueBuffer();
      } else {
        buffer = mRequestContext.getRequestBuffer();
      }
      final TimestampedValue<?> score = mFreshener.getScoreFunction().score(mDataToScore, mContext);
      buffer.put(
          mRequestContext.getEntityId(),
          mStaleQualifier.getFamily(),
          mStaleQualifier.getQualifier(),
          score.getTimestamp(),
          score.getValue());
      final int remainingFresheners = mRequestContext.finishFreshener(mStaleQualifier, WROTE);
      if (mRequestContext.allowsPartial()) {
        buffer.flush();
        return WROTE;
      } else {
        if (0 == remainingFresheners) {
          mRequestContext.getRequestBuffer().flush();
          return WROTE;
        } else {
          return DID_NOT_WRITE;
        }
      }
    }
  }

  private final FresheningRequestContext mRequestContext;
  private final KijiColumnName mFamily;
  private final Future<KijiRowData> mClientDataFuture;
  private final Map<KijiColumnName, FreshenerContext> mQualifiersContexts;

  /**
   * Initialize a new MapFamilyFreshenerCallable.
   *
   * @param requestContext Context of the request in which this map family freshening occurs.
   * @param family Family to refresh.
   * @param clientDataFuture Asynchronously retrieved data fulfilling the client's data request.
   */
  public MapFamilyFreshenerCallable(
      final FresheningRequestContext requestContext,
      final KijiColumnName family,
      final Future<KijiRowData> clientDataFuture
  ) {
    mRequestContext = requestContext;
    mFamily = family;
    mClientDataFuture = clientDataFuture;
    mQualifiersContexts = Maps.newHashMap();
    final Freshener freshener = mRequestContext.getFresheners().get(family);
    for (KijiColumnName qualifier
        : ScoringUtils.getMapFamilyQualifiers(requestContext.getClientDataRequest(), family)) {
      mQualifiersContexts.put(qualifier, InternalFreshenerContext.create(
          mRequestContext.getClientDataRequest(),
          qualifier,
          freshener.getParameters(),
          mRequestContext.getParameterOverrides(),
          freshener.getKVStoreReaderFactory()));
    }
  }

  /**
   * Get the KijiRowData to check for freshness.
   *
   * <p>
   *   Only one KijiRowData will be used to check for freshness for all qualifiers in the family.
   * </p>
   *
   * @param freshener Freshener attached to the map family of this callable.
   * @param context Context for the Freshener.
   *     {@link org.kiji.scoring.FreshenerContext#getAttachedColumn()} will return the map family.
   * @return Asynchronously retrieved KijiRowData to check for freshness.
   * @throws IOException in case of an error retrieving the KijiRowData.
   */
  private Future<KijiRowData> getDataToCheck(
      final Freshener freshener,
      final FreshenerContext context
  ) throws IOException {
    if (freshener.getFreshnessPolicy().shouldUseClientDataRequest(context)) {
      return mClientDataFuture;
    } else {
      final KijiTableReader reader = ScoringUtils.getPooledReader(mRequestContext.getReaderPool());
      try {
        KijiDataRequest policyRequest = KijiDataRequest.empty();
        for (Map.Entry<KijiColumnName, FreshenerContext> qualifierContext
            : mQualifiersContexts.entrySet()) {
          policyRequest = policyRequest.merge(
              freshener.getFreshnessPolicy().getDataRequest(qualifierContext.getValue()));
        }
        return ScoringUtils.getFuture(
            mRequestContext.getExecutorService(),
            new TableReadCallable(
                mRequestContext.getReaderPool(),
                mRequestContext.getEntityId(),
                policyRequest));
      } finally {
        reader.close();
      }
    }
  }

  /**
   * Get the KijiRowData to use as input to
   * {@link org.kiji.scoring.ScoreFunction#score(org.kiji.schema.KijiRowData,
   * org.kiji.scoring.FreshenerContext)}.
   *
   * <p>
   *   Only one KijiRowData will be used to score all qualifiers in the family. This row data will
   *   represent the combined data requests of
   *   {@link org.kiji.scoring.ScoreFunction#getDataRequest(org.kiji.scoring.FreshenerContext)} for
   *   each qualifier.
   * </p>
   *
   * @param freshener Freshener attached to the map family of this callable.
   * @param staleQualifiers Qualifiers to score and their contexts.
   * @return a KijiRowData representing the combined inputs of all stale qualifiers in the family.
   * @throws IOException in case of an error retrieving the KijiRowData.
   */
  private KijiRowData getDataToScore(
      final Freshener freshener,
      final Map<KijiColumnName, FreshenerContext> staleQualifiers
  ) throws IOException {
    KijiDataRequest request = KijiDataRequest.empty();
    for (Map.Entry<KijiColumnName, FreshenerContext> staleQualifier : staleQualifiers.entrySet()) {
      request = request.merge(
          freshener.getScoreFunction().getDataRequest(staleQualifier.getValue()));
    }
    final KijiTableReader reader = ScoringUtils.getPooledReader(mRequestContext.getReaderPool());
    try {
      return reader.get(mRequestContext.getEntityId(), request);
    } finally {
      reader.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Boolean call() throws Exception {
    final Freshener freshener = mRequestContext.getFresheners().get(mFamily);

    final KijiRowData dataToCheck = ScoringUtils.getFromFuture(getDataToCheck(
        freshener,
        InternalFreshenerContext.create(
            mRequestContext.getClientDataRequest(),
            mFamily,
            freshener.getParameters(),
            mRequestContext.getParameterOverrides(),
            freshener.getKVStoreReaderFactory())));
    final Map<KijiColumnName, FreshenerContext> staleQualifiers = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, FreshenerContext> qualifierContext
        : mQualifiersContexts.entrySet()) {
      if (!freshener.getFreshnessPolicy().isFresh(dataToCheck, qualifierContext.getValue())) {
        staleQualifiers.put(qualifierContext.getKey(), qualifierContext.getValue());
      } else {
        mRequestContext.finishFreshener(qualifierContext.getKey(), DID_NOT_WRITE);
      }
    }
    if (staleQualifiers.isEmpty()) {
      return DID_NOT_WRITE;
    } else {
      final KijiRowData dataToScore = getDataToScore(freshener, staleQualifiers);
      final List<Future<Boolean>> qualifierFutures = Lists.newArrayList();
      for (Map.Entry<KijiColumnName, FreshenerContext> staleQualifier
          : staleQualifiers.entrySet()) {
        final Future<Boolean> qualifierFuture = ScoringUtils.getFuture(
            mRequestContext.getExecutorService(),
            new ScoreCallable(
                freshener,
                dataToScore,
                staleQualifier.getKey(),
                staleQualifier.getValue(),
                mRequestContext));
        qualifierFutures.add(qualifierFuture);
      }
      return ScoringUtils.getFromFuture(ScoringUtils.getFuture(
          mRequestContext.getExecutorService(),
          new FutureAggregatingCallable<Boolean>(qualifierFutures))).contains(WROTE);
    }
  }
}
