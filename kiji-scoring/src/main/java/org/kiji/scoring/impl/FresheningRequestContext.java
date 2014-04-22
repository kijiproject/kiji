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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;
import org.kiji.scoring.FreshKijiTableReader.Builder.StatisticGatheringMode;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;
import org.kiji.scoring.statistics.FreshenerSingleRunStatistics;

/**
 * All state necessary to process a freshening 'get' request.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class FresheningRequestContext {
  private static final Logger LOG = LoggerFactory.getLogger(FresheningRequestContext.class);

  /** Unique identifier for the request served by this context. */
  private final String mId;
  /** Time in milliseconds since the epoch at which this request started. */
  private final long mStartTime;
  /**
   * Fresheners applicable to this request. These Fresheners should be released when they are no
   * longer needed.
   */
  private final ImmutableMap<KijiColumnName, Freshener> mFresheners;
  /**
   * Parameter overrides specified take from the
   * {@link org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions} associated with this request.
   */
  private final Map<String, String> mParameterOverrides;
  /** A pool of KijiTableReaders to use for retrieving data to check for freshness and score. */
  private final KijiTableReaderPool mReaderPool;
  /** The row to which this request applies. */
  private final EntityId mEntityId;
  /** The data request which should be refreshed before returning. */
  private final KijiDataRequest mClientDataRequest;
  /** A Future representing the current state of the requested data before freshening. */
  private final Future<KijiRowData> mClientDataFuture;
  /**
   * The buffer into which Fresheners will write their results. This writer should not be closed
   * during the request.
   */
  private final MultiBufferedWriter mBufferedWriter;
  /**
   * A single view into the MultiBufferedWriter to be shared by the entire request. This is only
   * used if mAllowPartial is false.
   */
  private final SingleBuffer mRequestBuffer;
  /** Whether this request allows partial freshening. */
  private final boolean mAllowPartial;
  /** The set of Fresheners which have not finished. */
  private final Map<KijiColumnName, KijiFreshenerRecord> mFreshenersRemaining;
  /** What level of statistics should be gathered about Fresheners run as part of this request. */
  private final StatisticGatheringMode mStatisticGatheringMode;
  /** Statistics about individual completed Fresheners. */
  private final BlockingQueue<FreshenerSingleRunStatistics> mFreshenerSingleRunStatistics;
  /** Executor to get Futures within this request. */
  private final ExecutorService mExecutorService;
  /**
   * Whether any Freshener has written into a buffer for this request. This value may only move
   * from false to true.
   */
  private boolean mHasReceivedWrites = false;
  /**
   * Whether the request managed by this context has timed out. This value may only move from
   * false to true.
   */
  private boolean mHasTimedOut = false;

  /**
   * Initialize a new FresheningRequestContext.
   *
   * @param id unique identifier for the request served by this context.
   * @param startTime the time in milliseconds since the epoch at which this request started.
   * @param fresheners Fresheners which should be run to fulfill this request.
   * @param parameterOverrides Request-time parameters which will be overlayed on the parameters
   *     from the Freshener records.
   * @param freshenerRecords Freshener records for Fresheners applicable to this request.
   * @param readerPool the pool of KijiTableReaders to which to delegate table reads.
   * @param entityId the row from which to read.
   * @param dataRequest the section of the row which should be refreshed.
   * @param clientDataFuture a Future representing the current state of the requested data before
   *     freshening.
   * @param bufferedWriter the MultiBufferedWriter used by this context to buffer and commit
   *     writes.
   * @param allowPartial whether this context allows partial freshening.
   * @param statisticGatheringMode what level of statistics should be gathered about Fresheners
   *     run as part of this request.
   * @param statisticsQueue Queue for communicating statistics about completed Fresheners to the
   *     statistics gathering thread. This queue is thread safe and ordering of statistics in the
   *     queue does not matter.
   * @param executorService ExecutorService to use for creating Futures within this request.
   */
  // CSOFF: ParameterNumber
  public FresheningRequestContext(
      final String id,
      final long startTime,
      final ImmutableMap<KijiColumnName, Freshener> fresheners,
      final Map<String, String> parameterOverrides,
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> freshenerRecords,
      final KijiTableReaderPool readerPool,
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final Future<KijiRowData> clientDataFuture,
      final MultiBufferedWriter bufferedWriter,
      final boolean allowPartial,
      final StatisticGatheringMode statisticGatheringMode,
      final BlockingQueue<FreshenerSingleRunStatistics> statisticsQueue,
      final ExecutorService executorService
  ) {
    // CSON: ParameterNumber
    mId = id;
    mStartTime = startTime;
    mFresheners = fresheners;
    mReaderPool = readerPool;
    mEntityId = entityId;
    mClientDataRequest = dataRequest;
    mClientDataFuture = clientDataFuture;
    mParameterOverrides = parameterOverrides;
    mBufferedWriter = bufferedWriter;
    mAllowPartial = allowPartial;
    mStatisticGatheringMode = statisticGatheringMode;
    mFreshenerSingleRunStatistics = statisticsQueue;
    mExecutorService = executorService;
    mFreshenersRemaining = getInitialFresheners(freshenerRecords);
    if (mAllowPartial) {
      // Each Freshener will have its own buffer when partial freshening is enabled, so the
      // request buffer is not needed.
      mRequestBuffer = null;
    } else {
      // Each Freshener may write only one value, so initialize the size of the buffer to the
      // number of Fresheners.
      mRequestBuffer = bufferedWriter.openSingleBuffer(mFreshenersRemaining.size());
    }
  }

  /**
   * Get the mapping of all qualified columns to associated Freshener records which will be run as
   * part of this request.
   *
   * @param freshenerRecords mapping from column to freshener record associated with that column.
   * @return the input mapping from column to freshener record with any map families replaced by the
   *     requested qualified columns from that family.
   */
  private Map<KijiColumnName, KijiFreshenerRecord> getInitialFresheners(
      final Map<KijiColumnName, KijiFreshenerRecord> freshenerRecords
  ) {
    final Map<KijiColumnName, KijiFreshenerRecord> collectedColumns = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> record : freshenerRecords.entrySet()) {
      if (record.getKey().isFullyQualified()) {
        collectedColumns.put(record.getKey(), record.getValue());
      } else {
        for (KijiColumnName qualifier : ScoringUtils.getMapFamilyQualifiers(
            mClientDataRequest, record.getKey())) {
          collectedColumns.put(qualifier, record.getValue());
        }
      }
    }
    return collectedColumns;
  }

  /**
   * Get the unique Id of this request.
   *
   * @return the unique Id of this request.
   */
  public String getRequestId() {
    return mId;
  }

  /**
   * Get the Fresheners applicable to this request.
   *
   * @return the Fresheners applicable to this request.
   */
  public ImmutableMap<KijiColumnName, Freshener> getFresheners() {
    return mFresheners;
  }

  /**
   * Get the parameter overrides for this request.
   *
   * @return the parameter overrides for this request.
   */
  public Map<String, String> getParameterOverrides() {
    return mParameterOverrides;
  }

  /**
   * Get the reader pool which services this request.
   *
   * @return the reader pool which services this request.
   */
  public KijiTableReaderPool getReaderPool() {
    return mReaderPool;
  }

  /**
   * Get the EntityId associated with this request.
   *
   * @return the EntityId associated with this request.
   */
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Get the data request which triggered this freshening.
   *
   * @return the data request which triggered this freshening.
   */
  public KijiDataRequest getClientDataRequest() {
    return mClientDataRequest;
  }

  /**
   * Get the Future representing asynchronously retrieve stale data based on the client request.
   *
   * @return the Future representing asynchronously retrieve stale data based on the client request.
   */
  public Future<KijiRowData> getClientDataFuture() {
    return mClientDataFuture;
  }

  /**
   * Get the buffer for this entire request. Used only when {@link #allowsPartial()} is false.
   *
   * @return the buffer for this entire request.
   */
  public SingleBuffer getRequestBuffer() {
    return mRequestBuffer;
  }

  /**
   * Whether this request allows partial writes.
   *
   * @return whether this request allows partial writes.
   */
  public boolean allowsPartial() {
    return mAllowPartial;
  }

  /**
   * Get the ExecutorService with which to run asynchronous tasks for this request.
   *
   * @return the ExecutorService with which to run asynchronous tasks for this request.
   */
  public ExecutorService getExecutorService() {
    return mExecutorService;
  }

  /**
   * Whether this context has received writes.
   *
   * @return Whether this context has received writes.
   */
  public boolean hasReceivedWrites() {
    return mHasReceivedWrites;
  }

  /**
   * Signal the context that a Freshener has finished.
   *
   * @param attachedColumn the column to which the finishing Freshener is attached.
   * @param scoreFunctionRan whether a ScoreFunction was run for the Freshener which finished.
   * @return the number of unfinished Fresheners.
   */
  public int finishFreshener(
      final KijiColumnName attachedColumn,
      final boolean scoreFunctionRan
  ) {
    if (scoreFunctionRan) {
      mHasReceivedWrites = true;
    }
    final long finishTime = System.nanoTime();
    if (StatisticGatheringMode.NONE != mStatisticGatheringMode) {
      mFreshenerSingleRunStatistics.add(FreshenerSingleRunStatistics.create(
          mHasTimedOut,
          finishTime - mStartTime,
          scoreFunctionRan,
          mFreshenersRemaining.get(attachedColumn)));
    }
    final int remaining;
    synchronized (mFreshenersRemaining) {
      mFreshenersRemaining.remove(attachedColumn);
      remaining = mFreshenersRemaining.size();
    }
    if (remaining < 0) {
      throw new InternalKijiError("More Fresheners have finished than were started.");
    } else {
      return remaining;
    }
  }

  /**
   * Get a new unique SingleBuffer to be used by a single Freshener.
   *
   * @return a new unique SingleBuffer to be used by a single Freshener.
   */
  public SingleBuffer openUniqueBuffer() {
    // Because this unique buffer will be used by only a single Freshener which can write only a
    // single value, set the initial buffer size to 1.
    return mBufferedWriter.openSingleBuffer(1);
  }

  /**
   * Marks this request as having timed out. Fresheners which finished after this method is called
   * will be recorded as having timed out.
   */
  public void timeOut() {
    mHasTimedOut = true;
  }

  /**
   * Gets cached stale data from the clientDataFuture if available. Falls back to reading from the
   * table if the clientDataFuture is not finished. Data is not guaranteed to be stale, but is
   * guaranteed to conform to the atomicity constraints set by partial freshening (i.e. if partial
   * freshening is disabled, this will return entirely fresh or entire stale data.)
   *
   * @param reader the reader to use to read new data if the clientDataFuture is not finished.
   * @return cached stale data if possible, otherwise the current state of the table. Returned data
   *     will conform to the atomicity guarantees provided by partial freshening, but this may
   *     return fresh data in some race conditions.
   * @throws IOException in case of an error reading from the table.
   */
  private KijiRowData getStaleData(
      final KijiTableReader reader
  ) throws IOException {
    if (mClientDataFuture.isDone()) {
      return ScoringUtils.getFromFuture(mClientDataFuture);
    } else {
      // If clientDataFuture is not ready to be retrieved we can only attempt to read data from the
      // table.  This data will still include exclusively fresh or stale data.
      return reader.get(mEntityId, mClientDataRequest);
    }
  }

  /**
   * Checks request context for writes and retrieves refreshed data from the table or cached stale
   * data from the clientDataFuture as appropriate.
   *
   * @return data from the specified row conforming to the specified data request.  Data will be
   *     fresh or stale depending on the state of Fresheners running for this request.
   * @throws java.io.IOException in case of an error reading from the table.
   */
  public KijiRowData checkAndRead() throws IOException {
    final KijiTableReader reader = ScoringUtils.getPooledReader(mReaderPool);
    try {
      if (mAllowPartial && mHasReceivedWrites) {
        // If any writes have been cached, read from the table.
        LOG.debug("{} allows partial freshening and data was written. Reading from the table.",
            mId);
        return reader.get(mEntityId, mClientDataRequest);
      } else {
        // If no writes have been cached or allowPartial is false, return stale data.
        LOG.debug("{} does not allow partial freshening or no values cached. Returning stale data.",
            mId);
        return getStaleData(reader);
      }
    } finally {
      reader.close();
    }
  }

  /**
   * Get a Future for each Freshener from the request context which returns a boolean indicating
   * whether the Freshener wrote a value to the table necessitating a reread.
   *
   * @return a Future for each Freshener from the request context.
   * @throws IOException in case of an error getting a reader from the pool.
   */
  public ImmutableList<Future<Boolean>> getFuturesForFresheners() throws IOException {
    final List<Future<Boolean>> collectedFutures =
        Lists.newArrayListWithCapacity(mFresheners.size());

    for (Map.Entry<KijiColumnName, Freshener> entry : mFresheners.entrySet()) {
      if (entry.getKey().isFullyQualified()) {
        final Future<Boolean> future = ScoringUtils.getFuture(mExecutorService,
            new QualifiedFreshenerCallable(this, entry.getKey(), mClientDataFuture));
        collectedFutures.add(future);
      } else {
        final Future<Boolean> future = ScoringUtils.getFuture(mExecutorService,
            new MapFamilyFreshenerCallable(this, entry.getKey(), mClientDataFuture));
        collectedFutures.add(future);
      }
    }
    return ImmutableList.copyOf(collectedFutures);
  }
}
