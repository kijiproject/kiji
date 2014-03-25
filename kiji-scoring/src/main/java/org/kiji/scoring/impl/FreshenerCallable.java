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
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.ScoreFunction.TimestampedValue;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

/**
 * Callable which performs freshening for a specific column in the context of a specific get
 * request. Returns a boolean indicating whether any writes were committed.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class FreshenerCallable implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(FreshenerCallable.class);

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

  private final FresheningRequestContext mRequestContext;
  private final KijiColumnName mAttachedColumn;
  private final Future<KijiRowData> mRowDataToCheckFuture;

  /**
   * Initialize a new FreshenerCallable.
   *
   * @param requestContext all state necessary to perform freshening specific to this request.
   * @param attachedColumn the column to which this Freshener is attached.
   * @param rowDataToCheckFuture asynchronously collected KijiRowData to be checked by
   *     {@link org.kiji.scoring.KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData,
   *     org.kiji.scoring.FreshenerContext)}
   */
  public FreshenerCallable(
      final FresheningRequestContext requestContext,
      final KijiColumnName attachedColumn,
      final Future<KijiRowData> rowDataToCheckFuture
  ) {
    mRequestContext = requestContext;
    mAttachedColumn = attachedColumn;
    mRowDataToCheckFuture = rowDataToCheckFuture;
  }

  /** {@inheritDoc} */
  @Override
  public Boolean call() throws Exception {
    final Freshener freshener = mRequestContext.getFresheners().get(mAttachedColumn);
    try {
      final FreshenerContext freshenerContext =
          mRequestContext.getFreshenerContexts().get(mAttachedColumn);
      final KijiRowData dataToCheck = ScoringUtils.getFromFuture(mRowDataToCheckFuture);
      final boolean isFresh = freshener.getFreshnessPolicy().isFresh(dataToCheck, freshenerContext);
      if (isFresh) {
        LOG.debug(
            "{} Freshener attached to: {} returned fresh and will not run its ScoreFunction",
            mRequestContext.getRequestId(), mAttachedColumn);
        if (!mRequestContext.allowsPartial()
            && 0 == mRequestContext.finishFreshener(mAttachedColumn, false)) {
          // If this is the last thread, check for writes, flush, and indicate that data was
          // written
          if (mRequestContext.hasReceivedWrites()) {
            mRequestContext.getRequestBuffer().flush();
            return WROTE;
          } else {
            return DID_NOT_WRITE;
          }
        } else {
          // If partial freshening is on or this is not the last thread to finish,
          // this did not write.
          return DID_NOT_WRITE;
        }
      } else {
        LOG.debug("{} Freshener attached to: {} returned stale and will run its ScoreFunction",
            mRequestContext.getRequestId(), mAttachedColumn);
        final SingleBuffer buffer;
        if (mRequestContext.allowsPartial()) {
          buffer = mRequestContext.openUniqueBuffer();
        } else {
          buffer = mRequestContext.getRequestBuffer();
        }
        final KijiTableReader reader =
            ScoringUtils.getPooledReader(mRequestContext.getReaderPool());
        final TimestampedValue<?> score;
        try {
          score = freshener.getScoreFunction().score(
              reader.get(
                  mRequestContext.getEntityId(),
                  freshener.getScoreFunction().getDataRequest(freshenerContext)),
              freshenerContext
          );
        } finally {
          reader.close();
        }
        buffer.put(
            mRequestContext.getEntityId(),
            mAttachedColumn.getFamily(),
            mAttachedColumn.getQualifier(),
            score.getTimestamp(),
            score.getValue());
        mRequestContext.freshenerWrote();
        final int remainingFresheners = mRequestContext.finishFreshener(mAttachedColumn, true);
        if (mRequestContext.allowsPartial()) {
          // If partial freshening is enabled, flush the buffer immediately and indicate that data
          // was written.
          buffer.flush();
          return WROTE;
        } else {
          if (0 == remainingFresheners) {
            // If this is the last thread to finish, flush the request buffer and indicate that
            // data was written.
            mRequestContext.getRequestBuffer().flush();
            return WROTE;
          } else {
            // If this is not the last thread to finish, indicate that no data was written.
            return DID_NOT_WRITE;
          }
        }
      }
    } finally {
      freshener.release();
    }
  }
}
