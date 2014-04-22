/**
 * (c) Copyright 2013 WibiData, Inc.
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
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.KijiTableReaderPool;
import org.kiji.schema.KijiTableReaderPool.Builder.WhenExhaustedAction;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReader.Builder.StatisticGatheringMode;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.statistics.FreshKijiTableReaderStatistics;
import org.kiji.scoring.statistics.FreshenerSingleRunStatistics;

/**
 * Local implementation of FreshKijiTableReader.
 *
 * <p>
 *   InternalFreshKijiTableReader employs {@link Future}s to perform asynchronous and parallel
 *   computation. When a reader receives a request it launches a Future for each column in that
 *   request which has a Freshener attached. These Futures return a booleans which indicate whether
 *   they committed any data to Kiji before returning, which allows the reader to read from the
 *   table again before returning to provide the freshened data, or simply return the stale data
 *   cached earlier without another round trip to the table. Freshener Futures are tracked by a
 *   single aggregating Future which collects the boolean return values and itself returns a boolean
 *   which indicates whether any Freshener Future returned true. If all Fresheners finish within the
 *   allotted timeout, the return value of this aggregating Future is used to determine which data
 *   should be returned to the user. If any Freshener has not finished within the allotted timeout,
 *   the reader determines what data to return to the user by consulting the Context object specific
 *   to this request.
 * </p>
 *
 * <p>
 *   InternalFreshKijiTableReader is thread safe. Thread safety is accomplished primarily through
 *   the use of static methods with no side effects and compartmentalizing state into Context
 *   objects which persist only for the duration of a single request. Nearly all top level state in
 *   the reader is immutable and the state that is not is modified atomically through a single
 *   volatile reference which uses reference counting to ensure safety.
 * </p>
 *
 * <p>
 *   InternalFreshKijiTableReader optionally gathers metrics about the performance of its
 *   Fresheners. These metrics are collected by a background thread running within the reader using
 *   a producer/consumer queue system. Each Freshener Future commits basic information about its own
 *   performance to a queue which the {@link StatisticsGathererThread} collects and aggregates into
 *   statistics by Freshener.
 * </p>
 *
 * <p>
 *   Important inner classes:
 *   <ul>
 *     <li>
 *       {@link RereadableState}: Immutable container for state which is required by each request.
 *       A reader holds a single instance of this class at a time and all requests use a snapshot
 *       view of one such instance to ensure internal consistency. This instance may be replaced by
 *       a called to {@link #rereadFreshenerRecords()} or
 *       {@link #rereadFreshenerRecords(java.util.List)}.
 *     </li>
 *     <li>
 *       {@link StatisticsGathererThread}: Optionally collects performance metrics from completed
 *       Fresheners and aggregates them by Freshener run. These statistics can be accessed via
 *       {@link #getStatistics()}. This option is controlled via FreshKijiTableReader
 *       {@link Builder}'s withStatisticsGathering method.
 *     </li>
 *     <li>
 *       {@link RereadTask}: A TimerTask which optionally periodically calls
 *       {@link #rereadFreshenerRecords()} to ensure the reader is operating on the most recently
 *       attached Fresheners. This option is controled via FreshKijiTableReader {@link Builder}'s
 *       withAutomaticReread method.
 *     </li>
 *   </ul>
 * </p>
 */
@ApiAudience.Private
public final class InternalFreshKijiTableReader implements FreshKijiTableReader {

  private static final Logger LOG = LoggerFactory.getLogger(InternalFreshKijiTableReader.class);

  // -----------------------------------------------------------------------------------------------
  // Inner classes.
  // -----------------------------------------------------------------------------------------------

  /**
   * Container class for all state which can be modified by a call to
   * {@link #rereadFreshenerRecords()} or {@link #rereadFreshenerRecords(java.util.List)}.
   */
  private static final class RereadableState implements ReferenceCountable<RereadableState> {

    private static final String RETAIN_ERROR_PREFIX = "Cannot retain closed RereadableState:";
    private final ImmutableList<KijiColumnName> mColumnsToFreshen;
    private final ImmutableMap<KijiColumnName, KijiFreshenerRecord> mFreshenerRecords;
    private final ImmutableMap<KijiColumnName, Freshener> mFresheners;
    private final AtomicInteger mRetainCounter = new AtomicInteger(1);

    /**
     * Initialize a new RereadableState.
     *
     * @param columnsToFreshen the columns which may be refreshed by the reader which holds this
     *     RereadableState.
     * @param freshenerRecords the KijiFreshenerRecords for the columnsToFreshen if there are any
     *     attached.
     * @param fresheners the cached Freshener objects which perform freshening.
     */
    private RereadableState(
        final List<KijiColumnName> columnsToFreshen,
        final Map<KijiColumnName, KijiFreshenerRecord> freshenerRecords,
        final Map<KijiColumnName, Freshener> fresheners
    ) {
      mColumnsToFreshen = ImmutableList.copyOf(columnsToFreshen);
      mFreshenerRecords = ImmutableMap.copyOf(freshenerRecords);
      mFresheners = ImmutableMap.copyOf(fresheners);
    }

    /** {@inheritDoc} */
    @Override
    public RereadableState retain() {
      final int counter = mRetainCounter.getAndIncrement();
      Preconditions.checkState(counter >= 1,
          RETAIN_ERROR_PREFIX + " %s retain counter was %s.",
          toString(), counter);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public void release() throws IOException {
      final int counter = mRetainCounter.decrementAndGet();
      Preconditions.checkState(counter >= 0,
          "Cannot release closed RereadableState: %s retain counter is now %s.",
          toString(), counter);
      if (counter == 0) {
        close();
      }
    }

    /**
     * Release underlying resources.
     *
     * @throws IOException in case of an error releasing resources.
     */
    private void close() throws IOException {
      for (Freshener freshener : mFresheners.values()) {
        freshener.release();
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** TimerTask for periodically calling {@link FreshKijiTableReader#rereadFreshenerRecords()}. */
  private final class RereadTask extends TimerTask {

    private final long mRereadPeriod;

    /**
     * Initialize a new RereadTask for a given reader.
     *
     * @param rereadPeriod the time in milliseconds to wait between rereads.
     */
    public RereadTask(
        final long rereadPeriod
    ) {
      mRereadPeriod = rereadPeriod;
    }

    /**
     * Get the recurrence period of this timer task.
     *
     * @return the recurrence period of this timer task.
     */
    public long getRereadPeriod() { return mRereadPeriod; }

    /** {@inheritDoc} */
    @Override
    public void run() {
      try {
        rereadFreshenerRecords();
      } catch (IOException ioe) {
        LOG.warn("Failed to reread Freshener records for FreshKijiTableReader: {}.  Failure "
            + "occurred at {}. Will attempt again in {} milliseconds",
            InternalFreshKijiTableReader.this, scheduledExecutionTime(), mRereadPeriod);
      }
    }
  }

  /** Daemon thread which collects and aggregates FreshenerStatistics. */
  private final class StatisticsGathererThread extends Thread {

    /** TimerTask for periodically logging gathered statistics. */
    private final class StatisticsLoggerTask extends TimerTask {

      private final long mLoggingInterval;

      /**
       * Initialize a new StatisticsLoggerTask.
       *
       * @param loggingInterval time in milliseconds between log messages.
       */
      private StatisticsLoggerTask(
          final long loggingInterval
      ) {
        mLoggingInterval = loggingInterval;
      }

      /**
       * Get the logging interval for this logger.
       *
       * @return the logging interval for this logger.
       */
      public long getLoggingInterval() {
        return mLoggingInterval;
      }

      /** {@inheritDoc} */
      @Override
      public void run() {
        LOG.info("{}", mAggregatedStatistics);
      }
    }

    private final StatisticsLoggerTask mStatisticsLoggerTask;
    private final FreshKijiTableReaderStatistics mAggregatedStatistics =
        FreshKijiTableReaderStatistics.create(mStatisticGatheringMode);
    private volatile boolean mShutdown = false;

    /**
     * Initialize a new StatisticsGathererThread.
     *
     * @param loggingInterval the time in milliseconds between automatic logging of gathered
     *     statistics.  0 indicates no automatic logging.
     */
    private StatisticsGathererThread(
        final long loggingInterval
    ) {
      if (0 < loggingInterval) {
        final Timer timer = new Timer();
        mStatisticsLoggerTask = new StatisticsLoggerTask(loggingInterval);
        LOG.debug("{} starting automatic statistics logging timer with period: {}",
            mReaderUID, loggingInterval);
        timer.scheduleAtFixedRate(mStatisticsLoggerTask, loggingInterval, loggingInterval);
      } else if (0 == loggingInterval) {
        mStatisticsLoggerTask = null;
      } else {
        throw new IllegalArgumentException(String.format(
            "Statistics logging interval cannot be less than 0, found: %d", loggingInterval));
      }
    }

    /** Collect and save a FreshenerSingleRunStatistics from the reader's StatisticsQueue. */
    private void collectStats() {
      final List<FreshenerSingleRunStatistics> stats = Lists.newArrayList();
      mStatisticsQueue.drainTo(stats);
      // This switch is redundant right now because this thread is only created if the mode is ALL
      // but future modes will require it.
      switch (mStatisticGatheringMode) {
        case ALL: {
          for (FreshenerSingleRunStatistics stat : stats) {
            mAggregatedStatistics.addFreshenerRunStatistics(stat);
          }
          return;
        }
        case NONE: return;
        default:
      }
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      while (!mShutdown) {
        collectStats();
      }
      // When shutdown, collect final stats then quit.
      collectStats();
    }

    /** Stop gathering statistics. */
    public void shutdown() {
      mShutdown = true;
      if (null != mStatisticsLoggerTask) {
        mStatisticsLoggerTask.cancel();
      }
    }

    /**
     * Get statistics gathered by this thread.
     *
     * @return statistics gathered by this thread.
     */
    public FreshKijiTableReaderStatistics getStatistics() {
      return mAggregatedStatistics;
    }

    /**
     * Get the logging interval for statistics gathered by this Thread.
     *
     * @return the logging interval for statistics gathered by this Thread.
     */
    public long getLoggingInterval() {
      if (null != mStatisticsLoggerTask) {
        return mStatisticsLoggerTask.getLoggingInterval();
      } else {
        // 0 indicates no logging.
        return 0;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Static methods.
  // -----------------------------------------------------------------------------------------------

  /**
   * Filters a map of KijiFreshenerRecords to include only those records whose columns are
   * contained in the columnsToFreshen list.  If a column in columnsToFreshen does not occur in the
   * records map, it will not be included in the returned map.
   *
   * <p>
   *   Specifying a column family in columnsToFreshen will collect all records for qualified columns
   *   in that family.
   * </p>
   *
   * @param columnsToFreshen a list of columns whose records should be extracted from the map.
   * @param allRecords a map containing all records for a table which will be filtered to include
   *    only the specified columns. This map is not modified by this method.
   * @return an ImmutableMap of column names from the columnsToFreshen list and associated
   *    KijiFreshenerRecords.
   */
  private static ImmutableMap<KijiColumnName, KijiFreshenerRecord> filterRecords(
      final Map<KijiColumnName, KijiFreshenerRecord> allRecords,
      final List<KijiColumnName> columnsToFreshen
  ) {
    if (null == columnsToFreshen || columnsToFreshen.isEmpty()) {
      // If no columns are specified, all records should be instantiated.
      return ImmutableMap.copyOf(allRecords);
    } else {
      final Map<KijiColumnName, KijiFreshenerRecord> collectedRecords = Maps.newHashMap();
      for (KijiColumnName column : columnsToFreshen) {
        if (column.isFullyQualified()) {
          final KijiFreshenerRecord record = allRecords.get(column);
          if (null != record) {
            collectedRecords.put(column, record);
          } else {
            // check the family.
            final KijiColumnName family = new KijiColumnName(column.getFamily(), null);
            final KijiFreshenerRecord familyRecord = allRecords.get(family);
            if (null != familyRecord) {
              collectedRecords.put(family, familyRecord);
            }
          }
        } else {
          final KijiFreshenerRecord record = allRecords.get(column);
          if (null != record) {
            collectedRecords.put(column, record);
          } else {
            // Collect all records for columns in the family.
            for (Map.Entry<KijiColumnName, KijiFreshenerRecord> recordEntry
                : allRecords.entrySet()) {
              if (column.getFamily().equals(recordEntry.getKey().getFamily())) {
                collectedRecords.put(recordEntry.getKey(), recordEntry.getValue());
              }
            }
          }
        }
      }
      return ImmutableMap.copyOf(collectedRecords);
    }
  }

  /**
   * Create a map of Fresheners from a map of KijiFreshenerRecords.  Freshener components
   * are proactively created.
   *
   * @param readerUID unique identifier for the reader which called this method. Used for logging.
   * @param records the records from which to create Fresheners.
   * @return a mapping from KijiColumnNames to associated Fresheners.
   * @throws IOException in case of an error setting up a KijiFreshnessPolicy or ScoreFunction.
   */
  private static ImmutableMap<KijiColumnName, Freshener> createFresheners(
      final String readerUID,
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records
  ) throws IOException {
    return fillFresheners(readerUID, records, ImmutableMap.<KijiColumnName, Freshener>of());
  }

  /**
   * Fills a partial map of Fresheners by creating new Fresheners for each record not already
   * reflected by the fresheners map.
   *
   * @param readerUID unique identifier for the reader which called this method. Used for logging.
   * @param records a map of records for which to create Fresheners.
   * @param oldFresheners a partially filled map of Fresheners to be completed with new Fresheners
   *    built from the records map.
   * @return a map of Fresheners for each KijiFreshenerRecord in records.
   * @throws IOException in case of an error setting up a KijiFreshnessPolicy or ScoreFunction.
   */
  private static ImmutableMap<KijiColumnName, Freshener> fillFresheners(
      final String readerUID,
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records,
      final ImmutableMap<KijiColumnName, Freshener> oldFresheners
  ) throws IOException {
    final Map<KijiColumnName, Freshener> fresheners = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> entry : records.entrySet()) {
      if (!oldFresheners.containsKey(entry.getKey())) {
        // If there is not already a Freshener for this record, make one.

        final KijiFreshenerRecord record = entry.getValue();

        // Create the FreshenerSetupContext
        final InternalFreshenerContext context =
            InternalFreshenerContext.create(entry.getKey(), record.getParameters());

        // Instantiate the policy and score function.
        final KijiFreshnessPolicy policy =
            ScoringUtils.policyForName(record.getFreshnessPolicyClass());
        final ScoreFunction<?> scoreFunction =
            ScoringUtils.scoreFunctionForName(record.getScoreFunctionClass());

        // Create the KVStoreReaderFactory from the required stores of the score function and
        // policy, and add the factory to the Freshener context.
        final KeyValueStoreReaderFactory factory =
            ScoringUtils.createKVStoreReaderFactory(context, scoreFunction, policy);
        context.setKeyValueStoreReaderFactory(factory);

        // Setup the policy and score function.
        policy.setup(context);
        scoreFunction.setup(context);

        // Build the Freshener from initialized components.
        final Freshener freshener = new Freshener(
            policy,
            scoreFunction,
            factory,
            entry.getKey(),
            entry.getValue().getParameters());
        LOG.debug("{} loading new Freshener: {}", readerUID, freshener);
        fresheners.put(entry.getKey(), freshener);
      } else {
        // If there is already a Freshener for this key, save it.
        final Freshener oldFreshener = oldFresheners.get(entry.getKey());
        LOG.debug("{} preserving old Freshener: {}", readerUID, oldFreshener);
        fresheners.put(entry.getKey(), oldFreshener);
      }
    }

    return ImmutableMap.copyOf(fresheners);
  }

  /**
   * Filter a map of Fresheners down to only those attached to columns in a given list. Columns
   * which do not have Fresheners attached will not be reflected in the return value of this
   * method. An empty return indicates that no Fresheners are attached to the given columns.
   *
   * @param columnsToFreshen a list of columns for which to get Fresheners.
   * @param fresheners a map of all available fresheners. This map will not be modified by this
   *     method.
   * @return all Fresheners attached to columns in columnsToFresh available in fresheners.
   */
  private static ImmutableMap<KijiColumnName, Freshener> filterFresheners(
      final ImmutableList<KijiColumnName> columnsToFreshen,
      final ImmutableMap<KijiColumnName, Freshener> fresheners
  ) {
    final Map<KijiColumnName, Freshener> collectedFresheners = Maps.newHashMap();
    for (KijiColumnName column : columnsToFreshen) {
      if (column.isFullyQualified()) {
        final Freshener freshener = fresheners.get(column);
        if (null != freshener) {
          collectedFresheners.put(column, freshener);
        } else {
          final KijiColumnName family = new KijiColumnName(column.getFamily(), null);
          final Freshener familyFreshener = fresheners.get(family);
          if (null != familyFreshener) {
            collectedFresheners.put(family, familyFreshener);
          }
        }
      } else {

        for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
          if (freshenerEntry.getKey().getFamily().equals(column.getFamily())) {
            collectedFresheners.put(freshenerEntry.getKey(), freshenerEntry.getValue());
          }
        }
      }
    }
    return ImmutableMap.copyOf(collectedFresheners);
  }

  /**
   * Get a list of column names from a KijiDataRequest.
   *
   * @param request the request from which to get columns.
   * @return a list of column names from a KijiDataRequest.
   */
  private static ImmutableList<KijiColumnName> getColumnsFromRequest(
      final KijiDataRequest request
  ) {
    final List<KijiColumnName> collectedColumns = Lists.newArrayList();
    for (Column column : request.getColumns()) {
      collectedColumns.add(new KijiColumnName(column.getName()));
    }
    return ImmutableList.copyOf(collectedColumns);
  }

  /**
   * Remove any columns from requestColumns which are also included in disabledColumns. Does not
   * modify the input collections, returns a new list containing only those columns from
   * requestColumns which are not disabled.
   *
   * @param requestColumns all columns from a KijiDataRequest.
   * @param disabledColumns columns for which freshening is disabled.
   * @return a new collection containing all requested columns which are not disabled.
   */
  private static ImmutableList<KijiColumnName> removeDisabledColumns(
      final ImmutableList<KijiColumnName> requestColumns,
      final Set<KijiColumnName> disabledColumns
  ) {
    if (disabledColumns == FreshKijiTableReader.FreshRequestOptions.DISABLE_ALL_COLUMNS) {
      return ImmutableList.of();
    }
    final List<KijiColumnName> collectedColumns = Lists.newArrayList();
    for (KijiColumnName column : requestColumns) {
      if (!disabledColumns.contains(column)) {
        collectedColumns.add(column);
      }
    }
    return ImmutableList.copyOf(collectedColumns);
  }

  /**
   * Get a Future for a single Freshener. This directly returns the value returned by the Freshener.
   *
   * @param requestId unique identifier for the request which triggered this freshening.
   * @param freshener Freshener to run in the returned Future.
   * @param freshenerContext Context to expose to the Freshener phases.
   * @param clientDataFuture Future containing the data requested by the user before freshening.
   * @param executorService ExecutorService from which to get Futures.
   * @param readerPool Pool of readers from which to get any readers necessary for running the
   *     Freshener.
   * @param entityId EntityId of the row to freshen.
   * @param <T> type of the value returned by the Freshener.
   * @return a Future representing the return value of the given Freshener.
   */
  private static <T> Future<T> getFutureForFreshener(
      final String requestId,
      final Freshener freshener,
      final InternalFreshenerContext freshenerContext,
      final Future<KijiRowData> clientDataFuture,
      final ExecutorService executorService,
      final KijiTableReaderPool readerPool,
      final EntityId entityId
  ) {
    final Future<KijiRowData> rowDataToCheckFuture;
    if (freshener.getFreshnessPolicy().shouldUseClientDataRequest(freshenerContext)) {
      rowDataToCheckFuture = clientDataFuture;
    } else {
      rowDataToCheckFuture = ScoringUtils.getFuture(executorService, new TableReadCallable(
          readerPool, entityId, freshener.getFreshnessPolicy().getDataRequest(freshenerContext)));
    }
    return ScoringUtils.getFuture(executorService, new IsolatedFreshenerCallable<T>(
        freshener,
        rowDataToCheckFuture,
        freshenerContext,
        requestId,
        clientDataFuture,
        readerPool,
        entityId));
  }

  /**
   * Get a Future for each EntityId in entityIds which represents the return value of a
   * {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} request made against
   * the given FreshKijiTableReader with the given KijiDataRequest.
   *
   * @param entityIds the rows to freshen.
   * @param dataRequest the data to retrieve from each row.
   * @param freshReader the FreshKijiTableReader to use to perform each freshening read.
   * @param options options applicable to all requests made asynchronously in returned futures.
   * @param executorService ExecutorService from which to get Futures.
   * @return a list of Futures corresponding to the values of freshening data on each row in
   *     entityIds.
   */
  private static ImmutableList<Future<KijiRowData>> getFuturesForEntities(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final FreshKijiTableReader freshReader,
      final FreshRequestOptions options,
      final ExecutorService executorService
  ) {
    final List<Future<KijiRowData>> collectedFutures = Lists.newArrayList();

    for (EntityId entityId : entityIds) {
      collectedFutures.add(ScoringUtils.getFuture(executorService, new FreshTableReadCallable(
          freshReader, entityId, dataRequest, options)));
    }

    return ImmutableList.copyOf(collectedFutures);
  }

  // -----------------------------------------------------------------------------------------------
  // State.
  // -----------------------------------------------------------------------------------------------

  /** All possible reader states. */
  private enum LifecycleState {
    INITIALIZING, OPEN, CLOSING, CLOSED
  }

  /** The current state of the reader (e.g. OPEN, CLOSED). */
  private final AtomicReference<LifecycleState> mState =
      new AtomicReference<LifecycleState>(LifecycleState.INITIALIZING);

  /**
   * Unique identifier for this FreshKijiTableReader. Composed of the
   * {@link org.kiji.schema.util.JvmId}, system identity hashcode, and the time at which the reader
   * was created.
   */
  private final String mReaderUID;
  /** The table from which the reader reads. */
  private final KijiTable mTable;
  /** A pool of KijiTableReaders used for retrieving data to check for freshness and score. */
  private final KijiTableReaderPool mReaderPool;
  /** The default time in milliseconds to wait for a freshening request to complete. */
  private final long mTimeout;
  /** A timer task which periodically calls this reader's {@link #rereadFreshenerRecords()}. */
  private final RereadTask mRereadTask;
  /** Whether this reader allows partially freshened data to be written. */
  private final boolean mAllowPartial;
  /** The buffered writer through which all fresheners run by this reader commit to the table. */
  private final MultiBufferedWriter mBufferedWriter;
  /** The KijiFreshnessManager used to retrieve updated Freshener records. */
  private final KijiFreshnessManager mFreshnessManager;
  /** Level of statistics gathering (e.g. ALL, NONE). */
  private final StatisticGatheringMode mStatisticGatheringMode;
  /** Thread responsible for gathering and aggregating statistics. */
  private final StatisticsGathererThread mStatisticsGathererThread;
  /** Queue through which statistics about completed Fresheners are passed to the gatherer. */
  private final BlockingQueue<FreshenerSingleRunStatistics> mStatisticsQueue =
      new LinkedBlockingQueue<FreshenerSingleRunStatistics>();
  /** Unique ID generator for differentiating requests in logs. */
  private final UniqueIdGenerator mUniqueIdGenerator = new UniqueIdGenerator();
  /** ExecutorService from which to get Futures. */
  private final ExecutorService mExecutorService;
  /** All mutable state which may be modified by a called to {@link #rereadFreshenerRecords()}. */
  private volatile RereadableState mRereadableState;

  /**
   * Initializes a new InternalFreshKijiTableReader.
   *
   * @param table the KijiTable from which this reader will read and to which it will write.
   * @param timeout the time in milliseconds the reader should wait before returning stale data.
   * @param rereadPeriod the time in milliseconds between automatically rereading policy records.
   *     A value of 0 indicates no automatic rereads.
   * @param allowPartial whether to allow returning partially freshened data when available.
   * @param columnsToFreshen the set of columns which this reader will attempt to freshen.
   * @param statisticGatheringMode specifies what statistics to gather.
   * @param statisticsLoggingInterval time in milliseconds between automatic logging of statistics.
   *     0 indicates no automatic logging.
   * @param executorService ExecutorService to use for getting Futures.
   * @param overrides ColumnReaderSpec overrides which will change the default behavior when reading
   *     the associated columns. These overrides will affect reads made to provide KijiRowData to
   *     Fresheners as well as reads made by the user.
   * @param alternatives ColumnReaderSpec alternatives which will add supported behavior when
   *     reading the associated columns without changing the default read behavior. These
   *     alternatives will be available for reads made by Fresheners as well as reads made by the
   *     user.
   * @param onDecoderCacheMiss Behavior when the reader fails to find a cell decoder corresponding
   *     to a ColumnReaderSpec override specified in a KijiDataRequest.
   * @throws IOException in case of an error reading from the meta table or setting up a
   *     KijiFreshnessPolicy or ScoreFunction.
   */
  // CSOFF: ParameterNumberCheck
  public InternalFreshKijiTableReader(
      final KijiTable table,
      final long timeout,
      final long rereadPeriod,
      final boolean allowPartial,
      final List<KijiColumnName> columnsToFreshen,
      final StatisticGatheringMode statisticGatheringMode,
      final long statisticsLoggingInterval,
      final ExecutorService executorService,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives,
      final OnDecoderCacheMiss onDecoderCacheMiss
  ) throws IOException {
    // CSON: ParameterNumberCheck
    mReaderUID = String.format("%s;InternalFreshKijiTableReader@%s@%s",
        JvmId.get(), System.identityHashCode(this), System.currentTimeMillis());

    mTable = table;
    mReaderPool = KijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .withExhaustedAction(WhenExhaustedAction.BLOCK)
        .withMaxActive(FreshenerThreadPool.DEFAULT_THREAD_POOL_SIZE)
        .withColumnReaderSpecOverrides(overrides)
        .withColumnReaderSpecAlternatives(alternatives)
        .withOnDecoderCacheMissBehavior(onDecoderCacheMiss)
        .build();
    mBufferedWriter = new MultiBufferedWriter(mTable);
    mTimeout = timeout;
    mAllowPartial = allowPartial;
    mFreshnessManager = KijiFreshnessManager.create(mTable.getKiji());
    final List<KijiColumnName> innerColumnsToFreshen = (null != columnsToFreshen)
        ? columnsToFreshen : Lists.<KijiColumnName>newArrayList();
    final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records = filterRecords(
        mFreshnessManager.retrieveFreshenerRecords(mTable.getName()), innerColumnsToFreshen);
    mRereadableState = new RereadableState(
        innerColumnsToFreshen,
        records,
        createFresheners(mReaderUID, records));
    mStatisticGatheringMode = statisticGatheringMode;

    mStatisticsGathererThread = startStatisticsGatherer(statisticsLoggingInterval);
    mRereadTask = startPeriodicRereader(rereadPeriod);

    mExecutorService = executorService;

    LOG.debug("Opening reader with UID: {}", mReaderUID);
    // Retain the table once everything else has succeeded.
    mTable.retain();
    // Opening the reader must be the last line of the constructor.
    open();
  }

  /**
   * Progress the reader state from Initializing to Open.
   *
   * @throws IllegalStateException if the reader is not already initializing.
   */
  private void open() {
    Preconditions.checkState(
        mState.compareAndSet(LifecycleState.INITIALIZING, LifecycleState.OPEN),
        String.format(
            "Cannot finish initializing and open a reader which is not initializing. LifecycleState"
            + " was: %s",
            mState.get()));
  }

  /**
   * Progress the reader state from open to closing.
   *
   * @throws IllegalStateException if the reader is not already open.
   */
  private void beginClosing() {
    Preconditions.checkState(
        mState.compareAndSet(LifecycleState.OPEN, LifecycleState.CLOSING),
        String.format(
            "Cannot begin closing a reader which is not open.  LifecycleState was: %s.",
            mState.get()));
  }

  /**
   * Progress the reader state from closing to closed.
   *
   * @throws IllegalStateException if the reader is not already closing.
   */
  private void finishClosing() {
    Preconditions.checkState(
        mState.compareAndSet(LifecycleState.CLOSING, LifecycleState.CLOSED),
        String.format(
            "Cannot finish closing a reader which is not closing.  LifecycleState was: %s.",
            mState.get()));
  }

  /**
   * Ensure that the reader is in a given state.
   *
   * @param required the state in which the reader must be.
   * @throws IllegalStateException if the required and actual states do not match.
   */
  private void requireState(
      LifecycleState required
  ) {
    final LifecycleState actual = mState.get();
    Preconditions.checkState(actual == required,
        String.format("Required state was: %s, but found %s.", required, actual));
  }

  /**
   * Start a new StatisticsGathererThread if the reader's statistics gathering mode is not NONE.
   *
   * @param statisticsLoggingInterval time in milliseconds between logging statistics. 0 indicates
   *     no automatic logging.
   * @return a new StatisticsGathererThread, already started.
   */
  private StatisticsGathererThread startStatisticsGatherer(
      final long statisticsLoggingInterval
  ) {
    final StatisticsGathererThread gatherer;
    if (StatisticGatheringMode.NONE != mStatisticGatheringMode) {
      gatherer = new StatisticsGathererThread(statisticsLoggingInterval);
      LOG.debug("{} starting statistics gathering thread.", mReaderUID);
      gatherer.start();
    } else {
      gatherer = null;
    }
    return gatherer;
  }

  /**
   * Start a new periodic reread task with the given period.
   *
   * @param rereadPeriod the period in milliseconds between rereads.
   * @return a new periodic reread task with the given period, already started.
   */
  private RereadTask startPeriodicRereader(
      final long rereadPeriod
  ) {
    final RereadTask task;
    if (rereadPeriod > 0) {
      final Timer rereadTimer = new Timer();
      task = new RereadTask(rereadPeriod);
      LOG.debug("{} starting automatic reread timer with period: {}.",
          mReaderUID, rereadPeriod);
      rereadTimer.scheduleAtFixedRate(task, rereadPeriod, rereadPeriod);
    } else if (rereadPeriod == 0) {
      task = null;
    } else {
      throw new IllegalArgumentException(
          String.format("Reread time must be >= 0, found: %d", rereadPeriod));
    }
    return task;
  }

  /**
   * Attempts to get and retain the reader's RereadableState until retain succeeds.
   *
   * @return a retained RereadableState which should be released when it is no longer needed.
   */
  private RereadableState getRereadableState() {
    RereadableState state = null;
    while (null == state) {
      try {
        state = mRereadableState.retain();
      } catch (IllegalStateException ise) {
        if (!ise.getMessage().startsWith(RereadableState.RETAIN_ERROR_PREFIX)) {
          throw ise;
        }
      }
    }
    return state;
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface.
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) throws IOException {
    return get(
        entityId, dataRequest, FreshKijiTableReader.FreshRequestOptions.withTimeout(mTimeout));
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final FreshRequestOptions options
  ) throws IOException {
    requireState(LifecycleState.OPEN);
    // Get the start time for the request.
    final long startTime = System.nanoTime();

    final String id = String.format("%s#%s", mReaderUID, mUniqueIdGenerator.getNextUniqueId());
    LOG.debug("{} starting with EntityId: {} data request: {} request options: {}",
        id, entityId, dataRequest, options);

    final KijiTableReader requestReader = ScoringUtils.getPooledReader(mReaderPool);
    try {
      final ImmutableList<KijiColumnName> requestColumns =
          removeDisabledColumns(getColumnsFromRequest(dataRequest), options.getDisabledColumns());

      final ImmutableMap<KijiColumnName, Freshener> fresheners;
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records;
      // Get a retained snapshot of the rereadable state.
      final RereadableState rereadableState = getRereadableState();
      try {
        // Collect the Fresheners and Records applicable to this request.
        fresheners = filterFresheners(requestColumns, rereadableState.mFresheners);
        records = filterRecords(rereadableState.mFreshenerRecords, requestColumns);
        // If there are no Fresheners attached to the requested columns, return the requested data.
        if (fresheners.isEmpty()) {
          return requestReader.get(entityId, dataRequest);
        } else {
          // Retain the Fresheners so that they cannot be cleaned up while in use.
          for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
            freshenerEntry.getValue().retain();
          }
        }
      } finally {
        rereadableState.release();
      }

      LOG.debug("{} will run Fresheners: {}", id, fresheners.values());

      final Future<KijiRowData> clientDataFuture = ScoringUtils.getFuture(
          mExecutorService, new TableReadCallable(mReaderPool, entityId, dataRequest));

      final FresheningRequestContext requestContext = new FresheningRequestContext(
          id,
          startTime,
          fresheners,
          options.getParameters(),
          records,
          mReaderPool,
          entityId,
          dataRequest,
          clientDataFuture,
          mBufferedWriter,
          mAllowPartial,
          mStatisticGatheringMode,
          mStatisticsQueue,
          mExecutorService);

      final ImmutableList<Future<Boolean>> futures = requestContext.getFuturesForFresheners();

      final Future<List<Boolean>> superFuture =
          ScoringUtils.getFuture(mExecutorService, new FutureAggregatingCallable<Boolean>(futures));

      // If the options specify timeout of -1 this indicates we should use the configured timeout.
      final long timeout = (-1 == options.getTimeout()) ? mTimeout : options.getTimeout();
      try {
        if (ScoringUtils.getFromFuture(superFuture, timeout).contains(true)) {
          // If all Fresheners return in time and at least one has written a new value, read from
          // the table.
          LOG.debug("{} completed on time and data was written.", id);
          return requestReader.get(entityId, dataRequest);
        } else {
          // If all Fresheners return in time, but none have written new values, do not read from
          // the table.
          LOG.debug("{} completed on time and no data was written.", id);
          try {
            return ScoringUtils.getFromFuture(clientDataFuture, 0L);
          } catch (TimeoutException te) {
            // If client data is not immediately available, read from the table.
            return requestReader.get(entityId, dataRequest);
          }
        }
      } catch (TimeoutException te) {
        requestContext.timeOut();
        // If superFuture times out, read partially freshened data from the table or return the
        // cached data based on whether partial freshness is allowed.
        LOG.debug("{} timed out, checking for partial writes.", id);
        return requestContext.checkAndRead();
      }
    } finally {
      // Return the reader to the pool.
      requestReader.close();
    }
  }

  /**
   * Freshens data in a single column as needed before returning. If freshening is not complete
   * within the configured timeout, will return stale data.
   *
   * @param entityId EntityId of the row to query.
   * @param family Kiji family of the column from which to retrieve a possibly refreshed value.
   * @param qualifier Kiji qualifier of the column from which to tretireve a possibly refreshed
   *     value.
   * @param options options which affect the behavior of this freshening request only.
   * @param <T> type of the value to retrieved.
   * @return the data requested after freshening.
   * @throws IOException in case of an error reading from the table.
   */
  public <T> T get(
      final EntityId entityId,
      final String family,
      final String qualifier,
      final FreshRequestOptions options
  ) throws IOException {
    requireState(LifecycleState.OPEN);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final KijiDataRequest dataRequest = KijiDataRequest.create(family, qualifier);

    final String id = String.format("%s#%s", mReaderUID, mUniqueIdGenerator.getNextUniqueId());
    LOG.debug("{} starting with EntityId: {} data request: {} request options: {}",
        id, entityId, dataRequest, options);

    final KijiTableReader requestReader = ScoringUtils.getPooledReader(mReaderPool);
    try {
      final Freshener freshener;
      final RereadableState rereadableState = getRereadableState();
      try {
        freshener = rereadableState.mFresheners.get(columnName);
        if (null == freshener) {
          return requestReader.get(entityId, dataRequest).getMostRecentValue(family, qualifier);
        } else {
          freshener.retain();
        }
      } finally {
        rereadableState.release();
      }

      LOG.debug("{} will run Freshener: {}", id, freshener);

      final InternalFreshenerContext freshenerContext = InternalFreshenerContext.create(
          dataRequest,
          columnName,
          freshener.getParameters(),
          options.getParameters(),
          freshener.getKVStoreReaderFactory());

      final Future<KijiRowData> clientDataFuture = ScoringUtils.getFuture(
          mExecutorService, new TableReadCallable(mReaderPool, entityId, dataRequest));

      final Future<T> freshenerFuture = getFutureForFreshener(
          id,
          freshener,
          freshenerContext,
          clientDataFuture,
          mExecutorService,
          mReaderPool,
          entityId);

      final long timeout = (-1 == options.getTimeout()) ? mTimeout : options.getTimeout();
      try {
        final T retval = ScoringUtils.getFromFuture(freshenerFuture, timeout);
        LOG.debug("{} completed on time, asynchronously writing data.");
        // TODO(SCORE-163) do not ignore the return value of this call.
        mExecutorService.submit(new TableWriteRunnable<T>(
            mBufferedWriter.openSingleBuffer(1), entityId, family, qualifier, retval));
        return retval;
      } catch (TimeoutException te) {
        LOG.debug("{} timed out, returning stale data.", id);
        // TODO(SCORE-163) do not ignore the return value of this call.
        mExecutorService.submit(new TableWriteRunnable<T>(
            mBufferedWriter.openSingleBuffer(1), entityId, family, qualifier, freshenerFuture));
        return ScoringUtils.getFromFuture(clientDataFuture).getMostRecentValue(family, qualifier);
      }
    } finally {
      requestReader.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest
  ) throws IOException {
    return bulkGet(
        entityIds, dataRequest, FreshKijiTableReader.FreshRequestOptions.withTimeout(mTimeout));
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final FreshRequestOptions options
  ) throws IOException {
    requireState(LifecycleState.OPEN);

    LOG.debug("{} starting bulk get request.", mReaderUID);

    final ImmutableList<Future<KijiRowData>> futures =
        getFuturesForEntities(entityIds, dataRequest, this, options, mExecutorService);

    final Future<List<KijiRowData>> superDuperFuture = ScoringUtils.getFuture(
        mExecutorService, new FutureAggregatingCallable<KijiRowData>(futures));

    try {
      return ScoringUtils.getFromFuture(superDuperFuture, options.getTimeout());
    } catch (TimeoutException te) {
      // If the request times out, read from the table.
      final KijiTableReader reader = ScoringUtils.getPooledReader(mReaderPool);
      try {
        return reader.bulkGet(entityIds, dataRequest);
      } finally {
        reader.close();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      final KijiDataRequest dataRequest
  ) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      final KijiDataRequest dataRequest,
      final KijiScannerOptions scannerOptions
  ) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public FreshKijiTableReaderStatistics getStatistics() {
    return mStatisticsGathererThread.getStatistics();
  }

  /** {@inheritDoc} */
  @Override
  public void rereadFreshenerRecords() throws IOException {
    rereadFreshenerRecords(mRereadableState.mColumnsToFreshen);
  }

  /** {@inheritDoc} */
  @Override
  public void rereadFreshenerRecords(
      final List<KijiColumnName> columnsToFreshen
  ) throws IOException {
    requireState(LifecycleState.OPEN);
    LOG.debug("{} rereading Freshener records with columnsToFreshen: {}",
        mReaderUID, columnsToFreshen);
    // Collect and filter the current state of the meta table.
    final ImmutableMap<KijiColumnName, KijiFreshenerRecord> newRecords = filterRecords(
        mFreshnessManager.retrieveFreshenerRecords(mTable.getName()), columnsToFreshen);

    final Map<KijiColumnName, Freshener> oldFresheners = Maps.newHashMap();

    final RereadableState oldState = mRereadableState;
    // Retain old Fresheners if they are still valid. Invalid Fresheners will be closed by the old
    // RereadableState when it is closed.
    for (Map.Entry<KijiColumnName, Freshener> entry : oldState.mFresheners.entrySet()) {
      final KijiColumnName column = entry.getKey();
      final Freshener oldFreshener = entry.getValue();
      final KijiFreshenerRecord oldRecord = oldState.mFreshenerRecords.get(column);
      if (newRecords.containsKey(column) && newRecords.get(column).equals(oldRecord)) {
        // The Freshener record is unchanged, so retain the Freshener to transfer ownership from the
        // old RereadableState to the new one.
        LOG.debug("{} retaining still valid Freshener: {}", mReaderUID, oldFreshener);
        oldFreshener.retain();
        oldFresheners.put(column, oldFreshener);
      }
    }

    // Swap the new RereadableState into place and release the old one. When the old one closes, it
    // will release all the Fresheners it held, which will close any that are no longer valid and
    // not in use.
    mRereadableState = new RereadableState(
        columnsToFreshen,
        newRecords,
        fillFresheners(mReaderUID, newRecords, ImmutableMap.copyOf(oldFresheners)));
    oldState.release();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // beginClosing() must be the first line of close().
    beginClosing();

    if (null != mStatisticsGathererThread) {
      mStatisticsGathererThread.shutdown();
    }

    if (null != mRereadTask) {
      mRereadTask.cancel();
    }

    mFreshnessManager.close();
    try {
      mReaderPool.close();
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
    mBufferedWriter.close();
    mRereadableState.release();
    mTable.release();

    // finishClosing() must be the last line of close().
    finishClosing();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(InternalFreshKijiTableReader.class)
        .add("table", mTable)
        .add("timeout", mTimeout)
        .add("automatic_reread_period",
            (null != mRereadTask) ? mRereadTask.getRereadPeriod() : "no_automatic_reread")
        .add("allows_partial_freshening", mAllowPartial)
        .add("freshens_columns", Joiner.on(", ").join(mRereadableState.mColumnsToFreshen))
        .add("statistics_gathering_mode", mStatisticGatheringMode)
        .add("statistics_logging_period", (null != mStatisticsGathererThread)
            ? mStatisticsGathererThread.getLoggingInterval() : "no_statistics_gathered")
        .addValue(mState)
        .toString();
  }
}
