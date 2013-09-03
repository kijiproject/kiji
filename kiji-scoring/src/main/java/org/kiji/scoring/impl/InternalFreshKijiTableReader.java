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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;
import org.kiji.scoring.impl.InternalFreshKijiTableReader.ReaderState.State;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;

/**
 * Local implementation of FreshKijiTableReader.
 */
@ApiAudience.Private
public final class InternalFreshKijiTableReader implements FreshKijiTableReader {

  private static final Logger LOG = LoggerFactory.getLogger(InternalFreshKijiTableReader.class);

  // -----------------------------------------------------------------------------------------------
  // Inner classes.
  // -----------------------------------------------------------------------------------------------

  /**
   * Class for ensuring a predictable state transition for this reader. This class is not
   * thread safe and instances of it should be kept private within the reader for which it manages
   * state.
   */
  public static final class ReaderState {

    private final AtomicReference<State> mState = new AtomicReference<State>(State.INITIALIZING);

    /** All possible reader states. */
    public enum State {
      INITIALIZING, INITIALIZED, OPEN, CLOSING, CLOSED
    }

    /**
     * Progress the reader state directly from Initializing to Open.
     *
     * @throws IllegalStateException if the reader is not already initializing.
     */
    public void finishInitializingAndOpen() {
      Preconditions.checkState(
          mState.compareAndSet(State.INITIALIZING, State.OPEN), String.format(
          "Cannot finish initializing and open a reader which is not initializing. State was: %s",
          mState.get()));
    }

    /**
     * Progress the reader state from initialized to open.
     *
     * @throws IllegalStateException if the reader is not already initialized.
     */
    public void open() {
      Preconditions.checkState(
          mState.compareAndSet(State.INITIALIZED, State.OPEN), String.format(
          "Cannot open a reader which is not initialized.  State was: %s.", mState.get()));
    }

    /**
     * Progress the reader state from open to closing.
     *
     * @throws IllegalStateException if the reader is not already open.
     */
    public void beginClosing() {
      Preconditions.checkState(
          mState.compareAndSet(State.OPEN, State.CLOSING), String.format(
          "Cannot begin closing a reader which is not open.  State was: %s.", mState.get()));
    }

    /**
     * Progress the reader state from closing to closed.
     *
     * @throws IllegalStateException if the reader is not already closing.
     */
    public void finishClosing() {
      Preconditions.checkState(
          mState.compareAndSet(State.CLOSING, State.CLOSED), String.format(
          "Cannot finish closing a reader which is not closing.  State was: %s.", mState.get()));
    }

    /**
     * Ensure that the reader is in a given state.
     *
     * @param required the state in which the reader must be.
     * @throws IllegalStateException if the required and actual states do not match.
     */
    public void requireState(
        State required
    ) {
      final State actual = mState.get();
      Preconditions.checkState(actual == required, String.format(
          "Required state was: %s, but found %s.", required, actual));
    }

    /**
     * Get the current state of this reader.
     *
     * @return the current state of this reader.
     */
    public State getState() { return mState.get(); }
  }

  /** Encapsulation of all state necessary to perform freshening for a single column. */
  private static final class Freshener implements ReferenceCountable<Freshener> {

    private final KijiFreshnessPolicy mPolicy;
    private final KijiProducer mProducer;
    private final KeyValueStoreReaderFactory mFactory;
    private final KijiColumnName mAttachedColumn;
    private final AtomicInteger mRetainCounter = new AtomicInteger(1);

    /**
     * Initialize a new Freshener.
     *
     * @param policy the KijiFreshnessPolicy which governs this Freshener.
     * @param producer the KijiProducer which generates scores for this Freshener.
     * @param factory the KVStoreReaderFactory which services the policy and producer.
     * @param attachedColumn the column to which this Freshener is attached.
     */
    public Freshener(
        final KijiFreshnessPolicy policy,
        final KijiProducer producer,
        final KeyValueStoreReaderFactory factory,
        final KijiColumnName attachedColumn
    ) {
      mPolicy = policy;
      mProducer = producer;
      mFactory = factory;
      mAttachedColumn = attachedColumn;
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
     * @throws IOException in case of an error cleaning up the producer.
     */
    private void close() throws IOException {
      mProducer.cleanup(
          KijiFreshProducerContext.create(mAttachedColumn, null, mFactory, null));
      mFactory.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("attached column", mAttachedColumn.toString())
          .add("policy class", mPolicy.getClass().getName())
          .add("policy state", mPolicy.serialize())
          .add("producer class", mProducer.getClass().getName())
          .toString();
    }
  }

  /**
   * Container class for all state which can be modified by a call to {@link #rereadPolicies()} or
   * {@link #rereadPolicies(java.util.List)}.
   */
  private static final class RereadableState {

    private final ImmutableList<KijiColumnName> mColumnsToFreshen;
    private final ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> mPolicyRecords;
    private final ImmutableMap<KijiColumnName, Freshener> mFresheners;

    /**
     * Initialize a new RereadableState.
     *
     * @param columnsToFreshen the columns which may be refreshed by the reader which holds this
     *     RereadableState.
     * @param policyRecords the KijiFreshnessPolicyRecords for the columnsToFreshen if there are any
     *     attached.
     * @param fresheners the cached Freshener objects which perform freshening.
     */
    private RereadableState(
        final List<KijiColumnName> columnsToFreshen,
        final Map<KijiColumnName, KijiFreshnessPolicyRecord> policyRecords,
        final Map<KijiColumnName, Freshener> fresheners
    ) {
      mColumnsToFreshen = (null == columnsToFreshen) ? null
          : ImmutableList.copyOf(columnsToFreshen);
      mPolicyRecords = ImmutableMap.copyOf(policyRecords);
      mFresheners = ImmutableMap.copyOf(fresheners);
    }
  }

  /** All state necessary to process a freshening 'get' request. */
  private static final class FresheningRequestContext {

    /** Fresheners applicable to this request. */
    private final ImmutableMap<KijiColumnName, Freshener> mFresheners;
    /** A regular KijiTableReader to retrieve data for freshness checking and scorer inputs. */
    private final KijiTableReader mReader;
    /** The row to which this request applies. */
    private final EntityId mEntityId;
    /**
     * The Configuration of the Kiji instance in which this request lives to be used for configuring
     * Fresheners.
     */
    private final Configuration mConf;
    /** The data request which should be refreshed before returning. */
    private final KijiDataRequest mClientDataRequest;
    /** A Future representing the current state of the requested data before freshening. */
    private final Future<KijiRowData> mClientDataFuture;
    /** The buffer into which Fresheners will write their results. */
    private final MultiBufferedWriter mBufferedWriter;
    /**
     * A single view into the MultiBufferedWriter to be shared by the entire request. This is only
     * used if mAllowPartial is false.
     */
    private final SingleBuffer mRequestBuffer;
    /** Whether this request allows partial freshening. */
    private final boolean mAllowPartial;
    /**
     * A counter representing the number of unfinished Fresheners.  The request will end when this
     * counter reaches 0.
     */
    private final AtomicInteger mFreshenersRemaining;
    /**
     * Whether any Freshener has written into a buffer for this request. This value may only move
     * from false to true.
     */
    private boolean mHasReceivedWrites = false;

    /**
     * Initialize a new FresheningRequestContext.
     *
     * @param fresheners Fresheners which should be run to fulfill this request.
     * @param reader the regular KijiTableReader to which to delegate table reads.
     * @param entityId the row from which to read.
     * @param dataRequest the section of the row which should be refreshed.
     * @param conf the Configuration of the Kiji instance in which this read is being processed.
     * @param clientDataFuture a Future representing the current state of the requested data before
     *     freshening.
     * @param bufferedWriter the MultiBufferedWriter used by this context to buffer and commit
     *     writes.
     * @param allowPartial whether this context allows partial freshening.
     */
    // CSOFF: Parameter count
    public FresheningRequestContext(
        final ImmutableMap<KijiColumnName, Freshener> fresheners,
        final KijiTableReader reader,
        final EntityId entityId,
        final KijiDataRequest dataRequest,
        final Configuration conf,
        final Future<KijiRowData> clientDataFuture,
        final MultiBufferedWriter bufferedWriter,
        final boolean allowPartial
    ) {
      // CSON
      mReader = reader;
      mEntityId = entityId;
      mClientDataRequest = dataRequest;
      mConf = conf;
      mClientDataFuture = clientDataFuture;
      mFresheners = fresheners;
      mBufferedWriter = bufferedWriter;
      mRequestBuffer = bufferedWriter.openSingleBuffer();
      mAllowPartial = allowPartial;
      mFreshenersRemaining = new AtomicInteger(mFresheners.size());
    }

    /**
     * Signal the context that a Freshener has finished.  Only used when partial freshening is
     * disabled.
     *
     * @return the number of unfinished Fresheners.
     */
    public int finishFreshener() {
      final int remaining = mFreshenersRemaining.decrementAndGet();
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
      return mBufferedWriter.openSingleBuffer();
    }

    /**
     * Set whether any writes have been cached for this request.  Value may only change from false
     * to true. Once true, calls to setHasReceivedWrites(false) will be ignored.
     *
     * @param hasReceivedWrites whether a write was cached for this request.
     */
    public void setHasReceivedWrites(
        final boolean hasReceivedWrites
    ) {
      mHasReceivedWrites = mHasReceivedWrites || hasReceivedWrites;
    }
  }

  /** Callable which performs a read from a table.  Used in a Future to read asynchronously, */
  private static final class TableReadCallable implements Callable<KijiRowData> {

    private final KijiTableReader mReader;
    private final EntityId mEntityId;
    private final KijiDataRequest mDataRequest;

    /**
     * Initialize a new TableReadCallable.
     *
     * @param reader the KijiTableReader to use to perform the read.
     * @param entityId the EntityId of the row from which to read data.
     * @param dataRequest the KijiDataRequest defining the data to read from the row.
     */
    public TableReadCallable(
        final KijiTableReader reader,
        final EntityId entityId,
        final KijiDataRequest dataRequest
    ) {
      mReader = reader;
      mEntityId = entityId;
      mDataRequest = dataRequest;
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData call() throws Exception {
      return mReader.get(mEntityId, mDataRequest);
    }
  }

  /**
   * Callable which performs freshening for a specific column in the context of a specific get
   * request. Returns a boolean indicating whether any writes were committed.
   */
  private static final class FreshenerCallable implements Callable<Boolean> {

    private final FresheningRequestContext mRequestContext;
    private final KijiColumnName mAttachedColumn;
    private final Future<KijiRowData> mRowDataToCheckFuture;

    /**
     * Initialize a new FreshenerCallable.
     *
     * @param requestContext all state necessary to perform freshening specific to this request.
     * @param attachedColumn the column to which this Freshener is attached.
     * @param rowDataToCheckFuture asynchronously collected KijiRowData to be checked by
     *     {@link KijiFreshnessPolicy#isFresh(
     *     org.kiji.schema.KijiRowData, org.kiji.scoring.PolicyContext)}
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
      final Freshener freshener = mRequestContext.mFresheners.get(mAttachedColumn);
      final PolicyContext policyContext = new InternalPolicyContext(
          mRequestContext.mClientDataRequest,
          mAttachedColumn,
          mRequestContext.mConf,
          freshener.mFactory);
      final KijiRowData clientData = getFromFuture(mRowDataToCheckFuture);
      final boolean isFresh = freshener.mPolicy.isFresh(clientData, policyContext);
      if (isFresh) {
        if (!mRequestContext.mAllowPartial && 0 == mRequestContext.finishFreshener()) {
          // If this is the last thread, check for writes, flush, and indicate a reread.
          return checkAndFlush(mRequestContext);
        } else {
          // If partial freshening is on or this is not the last thread to finish, do not reread.
          return Boolean.FALSE;
        }
      } else {
        final SingleBuffer buffer;
        if (mRequestContext.mAllowPartial) {
          buffer = mRequestContext.openUniqueBuffer();
        } else {
          buffer = mRequestContext.mRequestBuffer;
        }
        final KijiFreshProducerContext context = KijiFreshProducerContext.create(
            mAttachedColumn,
            mRequestContext.mEntityId,
            freshener.mFactory,
            buffer);
        freshener.mProducer.produce(mRequestContext.mReader.get(
            mRequestContext.mEntityId,
            freshener.mProducer.getDataRequest()),
            context);
        freshener.release();
        context.finish();
        mRequestContext.setHasReceivedWrites(context.hasReceivedWrites());
        if (mRequestContext.mAllowPartial) {
          // Check for writes, flush, and indicate a reread.
          return checkAndFlush(context);
        } else {
          if (0 == mRequestContext.finishFreshener()) {
            // If this is the last thread to finish, check for writes, flush, and indicate a reread.
            return checkAndFlush(mRequestContext);
          } else {
            // If this is not the last thread to finish, do not reread.
            return Boolean.FALSE;
          }
        }
      }
    }
  }

  /**
   * Callable which collects the return values from a list of Futures into a list of those values.
   */
  private static final class FutureAggregatingCallable<T> implements Callable<List<T>> {

    private final ImmutableList<Future<T>> mFutures;

    /**
     * Initialize a new FutureAggregatingCallable.
     *
     * @param futures asynchronously calculated values to be collected.
     */
    public FutureAggregatingCallable(
        final ImmutableList<Future<T>> futures
    ) {
      mFutures = futures;
    }

    /** {@inheritDoc} */
    @Override
    public List<T> call() throws Exception {
      final List<T> collectedResults = Lists.newArrayList();
      for (Future<T> future : mFutures) {
        collectedResults.add(getFromFuture(future));
      }
      return collectedResults;
    }
  }

  /** TimerTask for periodically calling {@link FreshKijiTableReader#rereadPolicies()}. */
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
        rereadPolicies();
      } catch (IOException ioe) {
        LOG.warn("Failed to reread freshness policies on FreshKijiTableReader: {}.  Failure "
            + "occurred at {}. Will attempt again in {} milliseconds",
            mReader, scheduledExecutionTime(), mRereadPeriod);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Static methods.
  // -----------------------------------------------------------------------------------------------

  /**
   * Gets an instance of a KijiFreshnessPolicy from a String class name.
   *
   * @param policy The name of the freshness policy class to instantiate.
   * @param conf the Configuration with which to configure the producer after creation.
   * @return An instance of the named policy.
   */
  private static KijiFreshnessPolicy policyForName(
      final String policy,
      final Configuration conf
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policy).asSubclass(KijiFreshnessPolicy.class), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "KijiFreshnessPolicy class %s was not found on the classpath", policy));
    }
  }

  /**
   * Gets an instance of a producer from a String class name.
   *
   * @param producer The name of the producer class to instantiate.
   * @param conf the Configuration with which to configure the producer after creation.
   * @return An instance of the named producer.
   */
  private static KijiProducer producerForName(
      final String producer,
      final Configuration conf
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(producer).asSubclass(KijiProducer.class), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "Producer class %s was not found on the classpath", producer));
    }
  }

  /**
   * Filters a map of KijiFreshnessPolicyRecords to include only those records whose columns are
   * contained in the columnsToFreshen list.  If a column in columnsToFreshen does not occur in the
   * records map, it will not be included in the returned map.
   *
   * <p>
   *   Specifying any qualified column from a map type family will collect the record for that
   *   entire family, which in turn will enable freshening for all columns in that family. If a
   *   record exists for a single qualified column in a map type family, and not for the entire
   *   family, only the record for that qualified column will be collected.
   * </p>
   *
   * @param columnsToFreshen a list of columns whose records should be extracted from the map.
   * @param allRecords a map containing all records for a table which will be filtered to include
   *    only the specified columns.
   * @return an ImmutableMap of column names from the columnsToFreshen list and associated
   *    KijiFreshnessPolicyRecords.
   */
  private static ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> filterRecords(
      final Map<KijiColumnName, KijiFreshnessPolicyRecord> allRecords,
      final List<KijiColumnName> columnsToFreshen
  ) {
    if (null == columnsToFreshen || columnsToFreshen.isEmpty()) {
      // If no columns are specified, all records should be instantiated.
      return ImmutableMap.copyOf(allRecords);
    } else {
      final Map<KijiColumnName, KijiFreshnessPolicyRecord> collectedRecords = Maps.newHashMap();
      for (KijiColumnName column : columnsToFreshen) {
        final KijiFreshnessPolicyRecord record = allRecords.get(column);
        if (null != record) {
          // There is a record for this exact column, include it.
          collectedRecords.put(column, record);
        } else {
          if (column.isFullyQualified()) {
            // A fully qualified column without a record requires a warning.
            LOG.warn("Fully qualified column '{}' without Freshener attached included in "
                + "columnsToFreshen. No freshening will be run for this column until a Freshener is"
                + " attached.", column);
          } else {
            // A family column without a record collects all qualified columns within that family.
            boolean qualifiedRecordFound = false;
            for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry
                : allRecords.entrySet()) {
              if (entry.getKey().getFamily().equals(column.getFamily())) {
                qualifiedRecordFound = true;
                collectedRecords.put(entry.getKey(), entry.getValue());
              }
            }
            if (!qualifiedRecordFound) {
              // If no records are found within the family, log a warning.
              LOG.warn("Family '{}' without Freshener attached and with no Fresheners attached to "
                  + "qualified columns included in columnsToFreshen. No freshening will be run for "
                  + "this family until a Freshener is attached to it or within it.", column);
            }
          }
        }
      }
      return ImmutableMap.copyOf(collectedRecords);
    }
  }

  /**
   * Create a map of Fresheners from a map of KijiFreshnessPolicyRecords.  Freshener components
   * are proactively created.
   *
   * @param records the records from which to create Fresheners.
   * @param conf the Configuration with which to create the producer and policy objects.
   * @return a mapping from KijiColumnNames to associated Fresheners.
   * @throws IOException in case of an error setting up a producer.
   */
  private static ImmutableMap<KijiColumnName, Freshener> createFresheners(
      final ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> records,
      final Configuration conf
  ) throws IOException {
    return fillFresheners(records, conf, ImmutableMap.<KijiColumnName, Freshener>of());
  }

  /**
   * Fills a partial map of Fresheners by creating new Fresheners for each record not already
   * reflected by the fresheners map.
   *
   * @param records a map of records for which to create Fresheners.
   * @param conf the Configuration with which to create producer and policy objects.
   * @param oldFresheners a partially filled map of Fresheners to be completed with new Fresheners
   *    built from the records map.
   * @return a map of Fresheners for each KijiFreshnessPolicyRecord in records.
   * @throws IOException in case of an error setting up a producer.
   */
  private static ImmutableMap<KijiColumnName, Freshener> fillFresheners(
      final ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> records,
      final Configuration conf,
      final ImmutableMap<KijiColumnName, Freshener> oldFresheners
  ) throws IOException {
    final Map<KijiColumnName, Freshener> fresheners = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : records.entrySet()) {
      if (!oldFresheners.containsKey(entry.getKey())) {
        // If there is not already a Freshener for this record, make one.

        // Instantiate the policy and load its state.
        final KijiFreshnessPolicy policy =
            policyForName(entry.getValue().getFreshnessPolicyClass(), conf);
        policy.deserialize(entry.getValue().getFreshnessPolicyState());

        // Instantiate the producer.
        final KijiProducer producer =
            producerForName(entry.getValue().getProducerClass(), conf);

        // Populate the KVStores map and instantiate the KVStoreReaderFactory from the map.
        final Map<String, KeyValueStore<?, ?>> kvMap = Maps.newHashMap();
        kvMap.putAll(producer.getRequiredStores());
        kvMap.putAll(policy.getRequiredStores());
        final KeyValueStoreReaderFactory factory = KeyValueStoreReaderFactory.create(kvMap);

        // Setup the producer.
        producer.setup(KijiFreshProducerContext.create(entry.getKey(), null, factory, null));

        // Build the Freshener from initialized components.
        final Freshener freshener = new Freshener(policy, producer, factory, entry.getKey());
        fresheners.put(entry.getKey(), freshener);
      } else {
        // If there is already a Freshener for this key, save it.
        fresheners.put(entry.getKey(), oldFresheners.get(entry.getKey()));
      }
    }

    return ImmutableMap.copyOf(fresheners);
  }

  /**
   * Get a future from a given callable.  This method uses the singleton Executor to run threads
   * responsible for carrying out the operation of the Future.
   *
   * @param callable the callable to run in the new Future.
   * @param <RETVAL> the return type of the callable and Future.
   * @return a new Future representing asynchronous execution of the given callable.
   */
  private static <RETVAL> Future<RETVAL> getFuture(
      Callable<RETVAL> callable
  ) {
    return FreshenerThreadPool.getInstance().getExecutorService().submit(callable);
  }

  /**
   * Get the value from a given Future.  This blocks until the Future is complete.
   *
   * @param future the Future from which to get the resultant value.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future
  ) {
    try {
      return future.get();
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in milliseconds) before a TimeoutException.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout
  ) throws TimeoutException {
    return getFromFuture(future, timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in units defined by timeUnit) before a TimeoutException.
   * @param timeUnit the unit of time to use for the timeout.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout,
      final TimeUnit timeUnit
  ) throws TimeoutException {
    try {
      return future.get(timeout, timeUnit);
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Checks if a ProducerContext has received writes necessitating a reread from the table and
   * flushes the buffer if needed. This method is used when partial freshening is enabled.
   *
   * @param context the context to check for outstanding writes.
   * @return whether the buffer was flushed necessitating a reread from the table.
   * @throws IOException in case of an error writing to the table.
   */
  private static boolean checkAndFlush(
      final KijiFreshProducerContext context
  ) throws IOException {
    if (context.hasReceivedWrites()) {
      context.flush();
    }
    return context.hasReceivedWrites();
  }

  /**
   * Checks if a RequestContext has received writes necessitating a reread from the table and
   * flushes the buffer if needed. This method is used when partial freshening is disabled.
   *
   * @param context the context to check for outstanding writes.
   * @return whether the buffer was flushed necessitating a reread from the table.
   * @throws IOException in case of an error writing to the table.
   */
  private static boolean checkAndFlush(
      final FresheningRequestContext context
  ) throws IOException {
    final boolean hasReceivedWrites = context.mHasReceivedWrites;
    if (hasReceivedWrites) {
      context.mRequestBuffer.flush();
    }
    return hasReceivedWrites;
  }

  /**
   * Filter a map of Fresheners down to only those attached to columns in a given list.  Columns
   * which do not have Fresheners attached will not be reflected in the return value of this
   * method.  An empty return indicates that no Fresheners are attached to the given columns.
   *
   * @param columnsToFreshen a list of columns for which to get Fresheners.
   * @param fresheners a map of all available fresheners.
   * @return all Fresheners attached to columns in columnsToFresh available in fresheners.
   */
  private static ImmutableMap<KijiColumnName, Freshener> filterFresheners(
      final ImmutableList<KijiColumnName> columnsToFreshen,
      final ImmutableMap<KijiColumnName, Freshener> fresheners
  ) {
    final Map<KijiColumnName, Freshener> collectedFresheners = Maps.newHashMap();
    for (KijiColumnName column : columnsToFreshen) {
      final Freshener freshener = fresheners.get(column);
      if (null != freshener) {
        collectedFresheners.put(column, freshener);
      } else if (column.isFullyQualified()) {
        // If the column is fully qualified, check also for a freshener for the family.
        final KijiColumnName familyName = new KijiColumnName(column.getFamily());
        final Freshener familyFreshener = fresheners.get(familyName);
        if (null != familyFreshener) {
          collectedFresheners.put(familyName, familyFreshener);
        }
      } else {
        // If the column is a family, check also for fresheners attached to invidual columns.
        for (Map.Entry<KijiColumnName, Freshener> entry : fresheners.entrySet()) {
          if (entry.getKey().getFamily().equals(column.getFamily())) {
            final Freshener qualifiedFreshener = fresheners.get(entry.getKey());
            if (null != qualifiedFreshener) {
              collectedFresheners.put(entry.getKey(), qualifiedFreshener);
            }
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
   * Get a Future for each Freshener from the request context which returns a boolean indicating
   * whether the Freshener wrote a value to the table necessitating a reread.
   *
   * @param requestContext context object representing all state relevant to a single freshening get
   *     request.
   * @return a Future for each Freshener from the request context.
   */
  private static ImmutableList<Future<Boolean>> getFuturesForFresheners(
      final FresheningRequestContext requestContext
  ) {
    final List<Future<Boolean>> collectedFutures = Lists.newArrayList();

    for (Map.Entry<KijiColumnName, Freshener> entry : requestContext.mFresheners.entrySet()) {
      final Future<KijiRowData> rowDataToCheckFuture;
      if (entry.getValue().mPolicy.shouldUseClientDataRequest()) {
        rowDataToCheckFuture = requestContext.mClientDataFuture;
      } else {
        rowDataToCheckFuture = getFuture(new TableReadCallable(
            requestContext.mReader,
            requestContext.mEntityId,
            entry.getValue().mPolicy.getDataRequest()));
      }
      final Future<Boolean> future = getFuture(new FreshenerCallable(
          requestContext,
          entry.getKey(),
          rowDataToCheckFuture));
      collectedFutures.add(future);
    }
    return ImmutableList.copyOf(collectedFutures);
  }

  /**
   * Get a Future for each EntityId in entityIds which represents the return value of a
   * {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} request made against
   * the given FreshKijiTableReader with the given KijiDataRequest.
   *
   * @param entityIds the rows to freshen.
   * @param dataRequest the data to retrieve from each row.
   * @param freshReader the FreshKijiTableReader to use to perform each freshening read.
   * @return a list of Futures corresponding to the values of freshening data on each row in
   *     entityIds.
   */
  private static ImmutableList<Future<KijiRowData>> getFuturesForEntities(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final FreshKijiTableReader freshReader
  ) {
    final List<Future<KijiRowData>> collectedFutures = Lists.newArrayList();

    for (EntityId entityId : entityIds) {
      collectedFutures.add(getFuture(new TableReadCallable(freshReader, entityId, dataRequest)));
    }

    return ImmutableList.copyOf(collectedFutures);
  }

  /**
   * Checks request context for writes and retrieves refreshed data from the table or cached stale
   * data from the clientDataFuture as appropriate.
   *
   * @param requestContext context object representing all state relevant to a single freshening get
   *     request.  May be queried for the state of cached writes, the Future representing cached
   *     stale data, the KijiTableReader with which to perform reads if necessary, and the request's
   *     entityId and data request.
   * @return data from the specified row conforming to the specified data request.  Data will be
   *     fresh or stale depending on the state of Fresheners running for this request.
   * @throws IOException in case of an error reading from the table.
   */
  private static KijiRowData checkAndRead(
      final FresheningRequestContext requestContext
  ) throws IOException {
    if (requestContext.mAllowPartial) {
      // If any writes have been cached, read from the table.
      if (requestContext.mHasReceivedWrites) {
        return requestContext.mReader.get(
            requestContext.mEntityId, requestContext.mClientDataRequest);
      }
    }
    // If no writes have been cached or allowPartial is false, return stale data.
    return getStaleData(
        requestContext.mClientDataFuture,
        requestContext.mReader,
        requestContext.mEntityId,
        requestContext.mClientDataRequest);

  }

  /**
   * Gets cached stale data from the clientDataFuture if available.  Falls back to reading from the
   * table if the clientDataFuture is not finished.  Data is not guaranteed to be stale, but is
   * guaranteed to conform to the atomicity constraints set by partial freshening (i.e. is partial
   * freshening is disabled, this will return entirely fresh or entire stale data.)
   *
   * @param clientDataFuture asynchronously collected data for the client's requested entityId and
   *     data request.
   * @param reader the reader to use to read new data if the clientDataFuture is not finished.
   * @param entityId the entityId of the row to read if the clientDataFuture is not finished.
   * @param dataRequest an enumeration of the data to retrieve from the row if the clientDataFuture
   *     is not finished.
   * @return cached stale data if possible, otherwise the current state of the table.  Returned data
   *     will conform to the atomicity guarantees provided by partial freshening, but this may
   *     return fresh data in some race conditions.
   * @throws IOException in case of an error reading from the table.
   */
  private static KijiRowData getStaleData(
      final Future<KijiRowData> clientDataFuture,
      final KijiTableReader reader,
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) throws IOException {
    if (clientDataFuture.isDone()) {
      return getFromFuture(clientDataFuture);
    } else {
      // If clientDataFuture is not ready to be retrieved we can only attempt to read data from the
      // table.  This data will still include exclusively fresh or stale data.
      return reader.get(entityId, dataRequest);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // State.
  // -----------------------------------------------------------------------------------------------

  private final ReaderState mState;
  private final KijiTable mTable;
  private final KijiTableReader mReader;
  private final long mTimeout;
  private final RereadTask mRereadTask;
  private final boolean mAllowPartial;
  private final MultiBufferedWriter mBufferedWriter;
  private final ImmutableList<KijiColumnName> mColumnsToFreshen;
  private final KijiFreshnessManager mFreshnessManager;
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
   * @throws IOException in case of an error reading from the metatable or setting up producers.
   */
  public InternalFreshKijiTableReader(
      final KijiTable table,
      final long timeout,
      final long rereadPeriod,
      final boolean allowPartial,
      final List<KijiColumnName> columnsToFreshen
  ) throws IOException {
    // Initializing the reader state must be the first line of the constructor.
    mState = new ReaderState();

    mTable = table;
    mReader = table.openTableReader();
    mBufferedWriter = new MultiBufferedWriter(mTable);
    mTimeout = timeout;
    mAllowPartial = allowPartial;
    mColumnsToFreshen = (null != columnsToFreshen)
        ? ImmutableList.copyOf(columnsToFreshen) : ImmutableList.<KijiColumnName>of();
    mFreshnessManager = KijiFreshnessManager.create(mTable.getKiji());
    final ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> records =
        filterRecords(mFreshnessManager.retrievePolicies(mTable.getName()), mColumnsToFreshen);
    mRereadableState = new RereadableState(
        columnsToFreshen,
        records,
        createFresheners(records, mTable.getKiji().getConf()));

    if (rereadPeriod > 0) {
      final Timer rereadTimer = new Timer();
      mRereadTask = new RereadTask(rereadPeriod);
      rereadTimer.scheduleAtFixedRate(mRereadTask, rereadPeriod, rereadPeriod);
    } else if (rereadPeriod == 0) {
      mRereadTask = null;
    } else {
      throw new IllegalArgumentException(
          String.format("Reread time must be >= 0, found: %d", rereadPeriod));
    }

    // Opening the reader must be the last line of the constructor.
    mState.finishInitializingAndOpen();
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
    return get(entityId, dataRequest, mTimeout);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final long timeout
  ) throws IOException {
    mState.requireState(State.OPEN);

    // Collect the Fresheners applicable to this request.
    final ImmutableMap<KijiColumnName, Freshener> fresheners =
        filterFresheners(getColumnsFromRequest(dataRequest), mRereadableState.mFresheners);
    // If there are no Fresheners attached to the requested columns, return the requested data.
    if (fresheners.isEmpty()) {
      return mReader.get(entityId, dataRequest);
    } else {
      // Retain the Fresheners so that they cannot be cleaned up while in use.
      for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
        freshenerEntry.getValue().retain();
      }
    }

    final Future<KijiRowData> clientDataFuture =
        getFuture(new TableReadCallable(mReader, entityId, dataRequest));

    final FresheningRequestContext requestContext = new FresheningRequestContext(
        fresheners,
        mReader,
        entityId,
        dataRequest,
        mTable.getKiji().getConf(),
        clientDataFuture,
        mBufferedWriter,
        mAllowPartial);

    final ImmutableList<Future<Boolean>> futures = getFuturesForFresheners(requestContext);

    final Future<List<Boolean>> superFuture =
        getFuture(new FutureAggregatingCallable<Boolean>(futures));

    try {
      if (getFromFuture(superFuture, timeout).contains(true)) {
        // If all Fresheners return in time and at least one has written a new value, read from the
        // table.
        return mReader.get(entityId, dataRequest);
      } else {
        try {
          return getFromFuture(clientDataFuture, 0L);
        } catch (TimeoutException te) {
          // If client data is not immediately available, read from the table.
          return mReader.get(entityId, dataRequest);
        }
      }
    } catch (TimeoutException te) {
      // If superFuture times out, read partially freshened data from the table or return the cached
      // data based on whether partial freshness is allowed.
      return checkAndRead(requestContext);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest
  ) throws IOException {
    return bulkGet(entityIds, dataRequest, mTimeout);
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final long timeout
  ) throws IOException {
    mState.requireState(State.OPEN);

    final ImmutableList<Future<KijiRowData>> futures =
        getFuturesForEntities(entityIds, dataRequest, this);

    final Future<List<KijiRowData>> superDuperFuture =
        getFuture(new FutureAggregatingCallable<KijiRowData>(futures));

    try {
      return getFromFuture(superDuperFuture, timeout);
    } catch (TimeoutException te) {
      // If the request times out, read from the table.
      return mReader.bulkGet(entityIds, dataRequest);
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

  /**
   * {@inheritDoc}
   *
   * <p>
   *   This implementation of FreshKijiTableReader ignores the withPreload parameter and always
   *   proactively instantiates objects needed for freshening.
   * </p>
   */
  @Override
  public void rereadPolicies() throws IOException {
    rereadPolicies(mRereadableState.mColumnsToFreshen);
  }

  @Override
  public void rereadPolicies(
      final List<KijiColumnName> columnsToFreshen
  ) throws IOException {
    mState.requireState(State.OPEN);
    // Collect and filter the current state of the meta table.
    final ImmutableMap<KijiColumnName, KijiFreshnessPolicyRecord> newRecords =
        filterRecords(mFreshnessManager.retrievePolicies(mTable.getName()), columnsToFreshen);

    final Map<KijiColumnName, Freshener> oldFresheners = Maps.newHashMap();

    for (Map.Entry<KijiColumnName, Freshener> entry : mRereadableState.mFresheners.entrySet()) {
      if (!newRecords.containsKey(entry.getKey())) {
        // If the column no longer has a freshness policy record, release the old capsule.
        entry.getValue().release();
      } else {
        if (newRecords.get(entry.getKey()) != mRereadableState.mPolicyRecords.get(entry.getKey())) {
          // If the column still has a freshness policy record and that record has changed, release
          // the old capsule.
          entry.getValue().release();
        } else {
          // If the record has not changed, keep the old Freshener.
          oldFresheners.put(entry.getKey(), entry.getValue());
        }
      }
    }

    mRereadableState = new RereadableState(
        columnsToFreshen,
        newRecords,
        fillFresheners(newRecords, mTable.getKiji().getConf(), ImmutableMap.copyOf(oldFresheners))
    );
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // beginClosing() must be the first line of close().
    mState.beginClosing();

    if (null != mRereadTask) {
      mRereadTask.cancel();
    }

    mFreshnessManager.close();
    mReader.close();
    mBufferedWriter.close();

    // finishClosing() must be the last line of close().
    mState.finishClosing();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(InternalFreshKijiTableReader.class)
        .add("table", mTable)
        .add("timeout", mTable)
        .add("automatic-reread-period",
            (mRereadTask != null) ? mRereadTask.getRereadPeriod() : "no-automatic-reread")
        .add("allows-partial-freshening", mAllowPartial)
        .add("freshens-columns", Joiner.on(", ").join(mColumnsToFreshen))
        .addValue(mState)
        .toString();
  }
}
