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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;

/**
 * Callable which performs freshening for a single isolated column. This callable does not persist
 * the refreshed value, but instead returns it directly.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 *
 * @param <T> type of the value returned by the Freshener run in this Callable.
 */
@ApiAudience.Private
final class IsolatedFreshenerCallable<T> implements Callable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedFreshenerCallable.class);

  private final Freshener mFreshener;
  private final Future<KijiRowData> mDataToCheckFuture;
  private final InternalFreshenerContext mContext;
  private final String mRequestId;
  private final Future<KijiRowData> mClientDataFuture;
  private final KijiTableReaderPool mReaderPool;
  private final EntityId mEntityId;

  /**
   * Initialize a new IsolatedFreshenerCallable. This callable can be used to asynchronously
   * calculate a single score.
   *
   * @param freshener Freshener to run in this Callable.
   * @param dataToCheckFuture Future representing the KijiRowData to check for freshness.
   * @param context FreshenerContext to expose to Freshener phases.
   * @param requestId unique identifier for the request which triggered this freshening.
   * @param clientDataFuture Future containing the data requested by the user before freshening.
   * @param readerPool Pool of readers from which to get any readers necessary for running the
   *     Freshener.
   * @param entityId EntityId of the row to freshen.
   */
  public IsolatedFreshenerCallable(
      final Freshener freshener,
      final Future<KijiRowData> dataToCheckFuture,
      final InternalFreshenerContext context,
      final String requestId,
      final Future<KijiRowData> clientDataFuture,
      final KijiTableReaderPool readerPool,
      final EntityId entityId
  ) {
    mFreshener = freshener;
    mDataToCheckFuture = dataToCheckFuture;
    mContext = context;
    mRequestId = requestId;
    mClientDataFuture = clientDataFuture;
    mReaderPool = readerPool;
    mEntityId = entityId;
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public T call() throws Exception {
    final KijiRowData dataToCheck = ScoringUtils.getFromFuture(mDataToCheckFuture);
    final boolean isFresh = mFreshener.getFreshnessPolicy().isFresh(dataToCheck, mContext);
    if (isFresh) {
      LOG.debug("{} Freshener attached to: {} returned fresh and will not run its ScoreFunction",
          mRequestId, mFreshener.getAttachedColumn());
      final KijiColumnName columnName = mFreshener.getAttachedColumn();
      return ScoringUtils.getFromFuture(mClientDataFuture).getMostRecentValue(
          columnName.getFamily(), columnName.getQualifier());
    } else {
      LOG.debug("{} Freshener attached to: {} returned stale and will run its ScoreFunction",
          mRequestId, mFreshener.getAttachedColumn());
      final KijiTableReader reader = ScoringUtils.getPooledReader(mReaderPool);
      try {
        return (T) mFreshener.getScoreFunction().score(
            reader.get(mEntityId, mFreshener.getScoreFunction().getDataRequest(mContext)),
            mContext).getValue();
      } finally {
        reader.close();
      }

    }
  }
}
