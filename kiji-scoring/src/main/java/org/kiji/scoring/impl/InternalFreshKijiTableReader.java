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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * Implementation of a Fresh Kiji Table Reader for HBase.
 */
@ApiAudience.Private
@ApiStability.Experimental
public final class InternalFreshKijiTableReader implements FreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(InternalFreshKijiTableReader.class);

  /** The kiji table instance. */
  private final KijiTable mTable;

  /** Default reader to which to delegate reads. */
  private final KijiTableReader mReader;

  /** Freshener thread pool executor service. */
  private final ExecutorService mExecutor;

  /** Timeout duration in milliseconds for get requests. */
  private final long mTimeout;

  /** Time between automatically rereading freshness policies from the metatable in milliseconds. */
  private final long mRereadTime;

  /** TimerTask for automatically rereading freshness policies on a schedule. */
  private final RereadTask mRereadTask;

  /** Whether to preload new freshness policies during rereadPolicies(). */
  private final boolean mPreloadOnAutoReread;

  /** Whether to return and commit partially freshened data when available. */
  private final boolean mAllowPartialFresh;

  /**
   * Map from column names to freshness policy records. Created on initialization of the
   * FreshKijiTableReader with all freshness policies for the entire table.  Only recreated when
   * the reader is closed and reopened.
   */
  private final Map<KijiColumnName, KijiFreshnessPolicyRecord> mPolicyRecords;

  /**
   * Read Write locks for protecting cached record state. This lock should always be acquired
   * <i>before</i> synchronizing on mCapsuleCache.
   */
  private final ReadWriteLock mRecordReadWriteLock = new ReentrantReadWriteLock();
  private final Lock mRecordReadLock = mRecordReadWriteLock.readLock();
  private final Lock mRecordWriteLock = mRecordReadWriteLock.writeLock();

  /**
   * Cache of FreshnessCapsules containing a KijiFreshnessPolicy, a KijiProducer, and a
   * KeyValueStoreReaderFactory.  Lazily populated as needed.
   */
  private final Map<KijiColumnName, FreshnessCapsule> mCapsuleCache;

  /** Id of the next get request. */
  private final AtomicLong mGetId;
  /**
   * Map storing producer context objects mapped from getIds to allow atomic commitment of writes.
   */
  private final Map<String, List<KijiFreshProducerContext>> mContextMap;
  /**
   * BufferedWriter objects shared across KijiFreshProducerContexts for a given request. Used when
   * mAllowPartialFresh is false.
   */
  private final Map<String, KijiBufferedWriter> mBuffers;

  /**
   * Container class for KijiFreshnessPolicy and associated KijiProducer and
   * KeyValueStoreReaderFactory.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  static final class FreshnessCapsule implements ReferenceCountable<FreshnessCapsule> {
    private final KijiFreshnessPolicy mPolicy;
    private final KijiProducer mProducer;
    private final KeyValueStoreReaderFactory mFactory;
    private final KijiColumnName mAttachedColumn;
    private final AtomicInteger mRetainCount;

    /**
     * Default Constructor.
     * @param policy the KijiFreshnessPolicy to serialize.
     * @param producer the KijiProducer to serialize.
     * @param factory the KeyValueStoreReaderFactory to serialize.
     * @param attachedColumn the column to which this FreshnessCapsule is associated in
     * mCapsuleCache.
     */
    public FreshnessCapsule(
        final KijiFreshnessPolicy policy,
        final KijiProducer producer,
        final KeyValueStoreReaderFactory factory,
        final KijiColumnName attachedColumn) {
      mPolicy = policy;
      mProducer = producer;
      mFactory = factory;
      mAttachedColumn = attachedColumn;
      mRetainCount = new AtomicInteger(1);
    }

    /**
     * Get the KijiFreshnessPolicy.
     * @return the KijiFreshnessPolicy.
     */
    public KijiFreshnessPolicy getPolicy() {
      return mPolicy;
    }

    /**
     * Get the KijiProducer.
     * @return the KijiProducer.
     */
    public KijiProducer getProducer() {
      return mProducer;
    }

    /**
     * Get the KeyValueStoreReaderFactory.
     * @return the KeyValueStoreReaderFactory.
     */
    public KeyValueStoreReaderFactory getFactory() {
      return mFactory;
    }

    /** {@inheritDoc} */
    @Override
    public FreshnessCapsule retain() {
      final int counter = mRetainCount.getAndIncrement();
      Preconditions.checkState(counter >= 1,
          "Cannot retain closed FreshnessCapsule: %s retain counter was %s.",
          toString(), counter);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public void release() throws IOException {
      final int counter = mRetainCount.decrementAndGet();
      Preconditions.checkState(counter >= 0,
          "Cannot release closed FreshnessCapsule: %s retain counter is now %s.",
          toString(), counter);
      if (counter == 0) {
        close();
      }
    }

    /**
     * Closes and cleans up all stored objects.
     * @throws IOException in case of an error cleaning up the producer.
     */
    private void close() throws IOException {
      mProducer.cleanup(KijiFreshProducerContext.create(mAttachedColumn, null, mFactory, null));
      mFactory.close();
    }
  }

  /** TimerTask for rereading freshness policies automatically on a schedule. */
  private final class RereadTask extends TimerTask {
    /** Method to run when the task executes. */
    public void run() {
      try {
        rereadPolicies(mPreloadOnAutoReread);
      } catch (IOException ioe) {
        LOG.warn("Failed to reread freshness policies.  Will attempt again in {} milliseconds",
            mRereadTime);
      }
    }
  }

  /**
   * Creates a new <code>InternalFreshKijiTableReader</code> instance that sends read requests
   * to a Kiji table and performs freshening on the returned data.  Automatically rereads freshness
   * policies from the meta table on a schedule.
   *
   * @param table the Kiji table that will be read/scored.
   * @param timeout the maximum number of milliseconds to spend trying to score data.  If the
   *   process times out, stale data will be returned by
   *   {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} calls.
   * @param rereadTime The time to wait in milliseconds between automatically rereading freshness
   * policies from the meta table.  To disable automatic rereading, set to 0.
   * @param allowPartial whether to allow returning partially freshened data when available.
   * @param preloadOnAutoReread whether to preload new freshness policies during automatic calls
   * to {@link #rereadPolicies(boolean)}.  Requires rereadTime > 0.
   * @throws IOException if an error occurs communicating with the table or meta table.
   */
  public InternalFreshKijiTableReader(
      final KijiTable table,
      final long timeout,
      final long rereadTime,
      final boolean allowPartial,
      final boolean preloadOnAutoReread)
      throws IOException {
    mTable = table;
    mPreloadOnAutoReread = preloadOnAutoReread;
    // opening a reader retains the table, so we do not need to call retain manually.
    mReader = mTable.openTableReader();
    mExecutor = FreshenerThreadPool.getInstance().getExecutorService();
    mTimeout = timeout;
    final KijiFreshnessManager manager = KijiFreshnessManager.create(table.getKiji());
    mPolicyRecords = manager.retrievePolicies(mTable.getName());
    mCapsuleCache = new HashMap<KijiColumnName, FreshnessCapsule>();
    if (rereadTime > 0) {
      final Timer rereadTimer = new Timer();
      mRereadTask = new RereadTask();
      rereadTimer.scheduleAtFixedRate(mRereadTask, rereadTime, rereadTime);
      mRereadTime = rereadTime;
    } else if (rereadTime == 0) {
      mRereadTask = null;
      mRereadTime = 0;
    } else {
      throw new IllegalArgumentException(
          String.format("Reload time must be >= 0, found: %d", rereadTime));
    }
    mAllowPartialFresh = allowPartial;
    mGetId = new AtomicLong(0);
    mContextMap = new HashMap<String, List<KijiFreshProducerContext>>();
    mBuffers = new HashMap<String, KijiBufferedWriter>();
  }

  /** {@inheritDoc} */
  @Override
  public void rereadPolicies(final boolean withPreload) throws IOException {
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> newRecords =
        KijiFreshnessManager.create(mTable.getKiji()).retrievePolicies(mTable.getName());

    mRecordWriteLock.lock();
    try {
      synchronized (mCapsuleCache) {
        final Iterator<Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord>> iterator =
            mPolicyRecords.entrySet().iterator();
        while (iterator.hasNext()) {
          final Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry = iterator.next();
          if (newRecords.containsKey(entry.getKey())
              && newRecords.get(entry.getKey()).equals(entry.getValue())) {
            newRecords.remove(entry.getKey());
          } else {
            iterator.remove();
             if (mCapsuleCache.containsKey(entry.getKey())) {
              mCapsuleCache.get(entry.getKey()).release();
              mCapsuleCache.remove(entry.getKey());
            }
          }
        }
      }
      mPolicyRecords.putAll(newRecords);
    } finally {
      mRecordWriteLock.unlock();
    }
    if (withPreload) {
      final KijiDataRequestBuilder builder = KijiDataRequest.builder();
      final ColumnsDef columns = builder.newColumnsDef();
      for (KijiColumnName key : newRecords.keySet()) {
        columns.add(key);
      }
      preload(builder.build());
    }
  }

  /**
   * Gets an instance of a KijiFreshnessPolicy from a String class name.
   *
   * @param policy The name of the freshness policy class to instantiate.
   * @return An instance of the named policy.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  KijiFreshnessPolicy policyForName(String policy) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policy).asSubclass(KijiFreshnessPolicy.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "KijiFreshnessPolicy class %s was not found on the classpath", policy));
    }
  }

  /**
   * Gets an instance of a producer from a String class name.
   *
   * @param producer The name of the producer class to instantiate.
   * @return An instance of the named producer.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  KijiProducer producerForName(String producer) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(producer).asSubclass(KijiProducer.class), mTable.getKiji().getConf());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "Producer class %s was not found on the classpath", producer));
    }
  }

  /**
   * Creates a new FreshnessCapsule from the KijiFreshnessPolicyRecord associated with a given
   * KijiColumnName key in mPolicyRecords.  Will throw an IllegalStateException if no
   * KijiFreshnessPolicyRecord can be found for the given column name key.
   *
   * @param columnName the key to mPolicyRecords.
   * @return a new FreshnessCapsule constructed from a record in mPolicyRecords.
   * @throws IOException in case of an error setting up the producer.
   */
  FreshnessCapsule makeCapsule(KijiColumnName columnName) throws IOException {
    mRecordReadLock.lock();
    final KijiFreshnessPolicy policy;
    final KijiProducer producer;
    try {
      final KijiFreshnessPolicyRecord record = mPolicyRecords.get(columnName);
      Preconditions.checkState(null != record, "There is no KijiFreshnessPolicyRecord associated "
          + "with KijiColumnName key: %s", columnName);

      // Instantiate and initialize the policies.
      policy = policyForName(record.getFreshnessPolicyClass());
      policy.deserialize(record.getFreshnessPolicyState());

      // Instantiate the producer.
      producer = producerForName(record.getProducerClass());
    } finally {
      mRecordReadLock.unlock();
    }
    // Create a kvstore reader factory for this policy and populate it with required stores.
    final Map<String, KeyValueStore<?, ?>> kvMap =
        new HashMap<String, KeyValueStore<?, ?>>();
    kvMap.putAll(producer.getRequiredStores());
    kvMap.putAll(policy.getRequiredStores());
    KeyValueStoreReaderFactory factory = KeyValueStoreReaderFactory.create(kvMap);

    // Initialize the producer.
    producer.setup(KijiFreshProducerContext.create(columnName, null, factory, null));

    // Encapsulate the policy, producer, and factory.
    return new FreshnessCapsule(policy, producer, factory, columnName);
  }

  /**
   * Synchronously gets a capsule from the cache corresponding to a given KijiColumnName.  All read
   * access to the capsule cache should use this method.  Will throw an IllegalStateException if
   * no FreshnessCapsule can be found for the given column name key.
   *
   * @param columnName the name of the column for which to get a FreshnessCapsule.
   * @return a FreshnessCapsule corresponding to the given KijiColumnName, already retained.
   */
  private FreshnessCapsule getCapsule(KijiColumnName columnName) {
    synchronized (mCapsuleCache) {
      final FreshnessCapsule capsule = mCapsuleCache.get(columnName);
      Preconditions.checkState(null != capsule, "There is no FreshnessCapsule associated "
          + " KijiColumnName key: %s", columnName);
      return capsule.retain();
    }
  }

  /**
   * Synchronously puts a capsule into the cache corresponding to the given KijiColumnName.  If
   * there is already a capsule associated with the given key, releases the previous capsule before
   * replacing it in the map.  Retains the new capsule as long as it persists in the cache.  All
   * puts to the capsule cache should use this method.
   *
   * @param columnName the name of the column to which to associate the given capsule.
   * @param capsule the capsule to associate with the given column.
   * @throws IOException in case of an error closing resources in a released capsule.
   */
  private void putCapsule(KijiColumnName columnName, FreshnessCapsule capsule) throws IOException {
    synchronized (mCapsuleCache) {
      if (mCapsuleCache.containsKey(columnName)) {
        capsule.retain();
        mCapsuleCache.get(columnName).release();
        mCapsuleCache.put(columnName, capsule);
      } else {
        capsule.retain();
        mCapsuleCache.put(columnName, capsule);
      }
    }
  }

  /**
   * Gets all freshness capsules from the local cache necessary to service a given data request.
   * Returns an empty Map if there are no policies applicable to the data request.
   *
   * @param dataRequest the data request for which to find freshness policies.
   * @return A map from column name to KijiFreshnessPolicy.
   * @throws IOException if an error occurs while setting up a producer.
   * <p/>
   * Package private for testing purposes only, should not be accessed externally.
   */
  Map<KijiColumnName, FreshnessCapsule> getCapsules(KijiDataRequest dataRequest)
      throws IOException {
    final Map<KijiColumnName, FreshnessCapsule> capsules =
        new HashMap<KijiColumnName, FreshnessCapsule>();
    final Collection<Column> columns = dataRequest.getColumns();

    mRecordReadLock.lock();
    try {
      for (Column column : columns) {
        final KijiColumnName columnName = column.getColumnName();
        final KijiColumnName family = new KijiColumnName(column.getFamily());

        final boolean containsQualifiedRecord = mPolicyRecords.containsKey(columnName);
        final boolean containsFamilyRecord = mPolicyRecords.containsKey(family);

        if (!columnName.isFullyQualified() && !containsFamilyRecord) {
          for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry
              : mPolicyRecords.entrySet()) {
            if (entry.getKey().getFamily().equals(columnName.getFamily())) {
              synchronized (mCapsuleCache) {
                if (mCapsuleCache.containsKey(entry.getKey())) {
                  capsules.put(entry.getKey(), getCapsule(entry.getKey()));
                } else {
                  final FreshnessCapsule capsule = makeCapsule(entry.getKey());
                  capsules.put(entry.getKey(), capsule);
                  putCapsule(entry.getKey(), capsule);
                }
              }
            }
          }
        }

        if (containsQualifiedRecord && containsFamilyRecord) {
          throw new InternalKijiError(String.format("Found freshness policy record for qualified "
              + "column: %s and family: %s only one may exist at a time.", columnName, family));
        } else if (containsQualifiedRecord) {
          synchronized (mCapsuleCache) {
            if (mCapsuleCache.containsKey(columnName)) {
              capsules.put(columnName, getCapsule(columnName));
            } else {
              final FreshnessCapsule capsule = makeCapsule(columnName);
              capsules.put(columnName, capsule);
              putCapsule(columnName, capsule);
            }
          }
        } else if (containsFamilyRecord) {
          synchronized (mCapsuleCache) {
            if (mCapsuleCache.containsKey(family)) {
              capsules.put(family, getCapsule(family));
            } else {
              final FreshnessCapsule capsule = makeCapsule(family);
              capsules.put(family, capsule);
              putCapsule(family, capsule);
            }
          }
        }
      }
    } finally {
      mRecordReadLock.unlock();
    }
    return capsules;
  }

  /**
   * Asynchronously Gets a KijiRowData representing the data the user requested at the time they
   * requested it. May be used by freshness policies to determine freshness, and may be returned by
   * a call to {@link #get(EntityId, KijiDataRequest)}.  Should only be called once per call to
   * get().
   *
   * @param eid The EntityId specified by the client's call to get().
   * @param dataRequest The client's data request.
   * @return A Future&lt;KijiRowData&gt; representing the data requested by the user.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  Future<KijiRowData> getClientData(final EntityId eid, final KijiDataRequest dataRequest) {
    return mExecutor.submit(new Callable<KijiRowData>() {
      public KijiRowData call() throws IOException {
        return mReader.get(eid, dataRequest);
      }
    });
  }

  /**
   * Creates a future for each {@link org.kiji.scoring.KijiFreshnessPolicy} applicable to a given
   * {@link org.kiji.schema.KijiDataRequest}.
   *
   * @param capsules a map from column names to freshness capsules as they were registered at the
   * time of this call.  Capsules are assumed retained earlier and will be released by this method.
   * @param clientData A Future&lt;KijiRowData&gt; representing the data requested by the client.
   *   Freshness policies which use the client data request will block on the return of this future.
   * @param eid The EntityId specified by the client's call to get().
   * @param clientRequest the client's original request.
   * @param getId the internal Id of the get request which invokes this method.  This Id is used as
   * a key to collect write caches for all producers associated with a single invokation of get().
   * @return A list of Future&lt;Boolean&gt; representing the need to reread data from the table
   *   to include producer output after freshening.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  List<Future<Boolean>> getFutures(
      final Map<KijiColumnName, FreshnessCapsule> capsules,
      final Future<KijiRowData> clientData,
      final EntityId eid,
      final KijiDataRequest clientRequest,
      final String getId) {
    final List<Future<Boolean>> futures = Lists.newArrayList();
    final Map<KijiColumnName, FreshnessCapsule> usesClientDataRequest = Maps.newHashMap();
    final Map<KijiColumnName, FreshnessCapsule> usesOwnDataRequest = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, FreshnessCapsule> entry : capsules.entrySet()) {
      if (entry.getValue().getPolicy().shouldUseClientDataRequest()) {
        usesClientDataRequest.put(entry.getKey(), entry.getValue());
      } else {
        usesOwnDataRequest.put(entry.getKey(), entry.getValue());
      }
    }
    for (final KijiColumnName key: usesClientDataRequest.keySet()) {
      final Future<Boolean> requiresReread = mExecutor.submit(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          final PolicyContext policyContext =
              new InternalPolicyContext(clientRequest, key, mTable.getKiji().getConf());
          KijiRowData rowData = null;
          try {
            rowData = clientData.get();
          } catch (InterruptedException ie) {
            throw new RuntimeException("Freshening thread interrupted", ie);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
              LOG.warn("Client data could not be retrieved.  "
                  + "Freshness policies which operate against "
                  + "the client data request will not run. " + ee.getCause().getMessage());
            } else {
              throw new RuntimeException(ee);
            }
          }
          if (rowData != null) {
            final boolean isFresh =
                usesClientDataRequest.get(key).getPolicy().isFresh(rowData, policyContext);
            if (isFresh) {
              return Boolean.FALSE;
            } else {
              final FreshnessCapsule capsule = usesClientDataRequest.get(key);
              final KijiFreshProducerContext context;
              if (mAllowPartialFresh) {
                context = KijiFreshProducerContext.create(
                    key,
                    eid,
                    mCapsuleCache.get(key).getFactory(),
                    mTable.getWriterFactory().openBufferedWriter());
              } else {
                context = KijiFreshProducerContext.create(
                    key,
                    eid,
                    mCapsuleCache.get(key).getFactory(),
                    mBuffers.get(getId));
              }
              synchronized (mContextMap) {
                if (mContextMap.containsKey(getId)) {
                  mContextMap.get(getId).add(context);
                } else {
                  mContextMap.put(getId, Lists.newArrayList(context));
                }
              }
              final KijiProducer producer = mCapsuleCache.get(key).getProducer();
              producer.produce(mReader.get(eid, producer.getDataRequest()), context);
              capsule.release();
              context.finish();
              return shouldReread(context, getId, capsules.size());
            }
          } else {
            return Boolean.FALSE;
          }
        }
      });
      futures.add(requiresReread);
    }
    for (final KijiColumnName key: usesOwnDataRequest.keySet()) {
      final Future<Boolean> requiresReread = mExecutor.submit(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          final KijiRowData rowData =
              mReader.get(eid, usesOwnDataRequest.get(key).getPolicy().getDataRequest());
          final PolicyContext policyContext =
              new InternalPolicyContext(clientRequest, key, mTable.getKiji().getConf());
          final boolean isFresh =
              usesOwnDataRequest.get(key).getPolicy().isFresh(rowData, policyContext);
          if (isFresh) {
            return Boolean.FALSE;
          } else {
            final FreshnessCapsule capsule = usesOwnDataRequest.get(key);
            final KijiFreshProducerContext context;
            if (mAllowPartialFresh) {
              context = KijiFreshProducerContext.create(
                  key,
                  eid,
                  mCapsuleCache.get(key).getFactory(),
                  mTable.getWriterFactory().openBufferedWriter());
            } else {
              context = KijiFreshProducerContext.create(
                  key,
                  eid,
                  mCapsuleCache.get(key).getFactory(),
                  mBuffers.get(getId));
            }
            synchronized (mContextMap) {
              if (mContextMap.containsKey(getId)) {
                mContextMap.get(getId).add(context);
              } else {
                mContextMap.put(getId, Lists.newArrayList(context));
              }
            }
            final KijiProducer producer = mCapsuleCache.get(key).getProducer();
            producer.produce(mReader.get(eid, producer.getDataRequest()), context);
            capsule.release();
            context.finish();
            return shouldReread(context, getId, capsules.size());
          }
        }
      });
      futures.add(requiresReread);
    }
    return futures;
  }

  /**
   * Called whenever a producer finishes.  Manages flushing buffers according to mAllowPartialFresh
   * and returns whether any new data has been written.
   *
   * @param context the producer context of the producer which just finished.
   * @param getId the ID of the request which triggered the producer.
   * @param capsulesSize the number of freshness capsules associated with the given getId.
   * @return whether data was written, requiring a reread.
   * @throws IOException in case of an IO error.
   */
  private Boolean shouldReread(
      KijiFreshProducerContext context,
      String getId,
      int capsulesSize)
      throws IOException {
    boolean shouldReread = false;
    if (mAllowPartialFresh) {
      if (context.hasReceivedWrites()) {
        context.flush();
        shouldReread = true;
      }
    } else {
      // Ensure that all producers have added their Contexts to the Context map before
      // checking if they are finished.
      if (mContextMap.get(getId).size() == capsulesSize) {
        boolean allFinished = true;
        for (KijiFreshProducerContext cont : mContextMap.get(getId)) {
          shouldReread = shouldReread || cont.hasReceivedWrites();
          allFinished = allFinished && cont.isFinished();
        }
        if (allFinished) {
          mBuffers.get(getId).flush();
          mContextMap.remove(getId);
        }
      }
    }
    return shouldReread;
  }

  /**
   * Return the state of the world before any producers writes are committed. If a producer or all
   * producers finish after the decision to return stale data and clientData is not done, this may
   * return partially or entirely fresh data.
   *
   * @param eid The EntityId of the row from which stale data was collected.
   * @param dataRequest The KijiDataRequest defining the KijiRowData returned.
   * @param clientData A Future containing stale data retrieved before producers ran.
   * @return the state of the world before and producer writes are committed.
   * @throws IOException in case of an error reading from the table.
   */
  private KijiRowData returnStale(
      EntityId eid,
      KijiDataRequest dataRequest,
      Future<KijiRowData> clientData) throws IOException {
    if (clientData.isDone()) {
      try {
        // If clientData is ready to be retrieved, do so and return it.  This should never throw
        // exceptions.
        return clientData.get();
      } catch (InterruptedException ie) {
        throw new RuntimeException("Freshening thread interrupted.", ie);
      } catch (ExecutionException ee) {
        if (ee.getCause() instanceof IOException) {
          // If there was an IOException reading from the table in the clientData thread, there will
          // be no data present so we should read from the table.
          return mReader.get(eid, dataRequest);
        } else {
          // Any other exception should terminate execution.
          throw new RuntimeException(ee);
        }
      }
    } else {
      // if clientData is not ready to be retrieved, read data from the table.  This data may
      // include the output of producers which finished after being checked earlier.
      return mReader.get(eid, dataRequest);
    }
  }

  /**
   * Callable used by {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)}.
   */
  private static final class GetFuture implements Callable<Boolean> {
    private final List<Future<Boolean>> mFutures;

    /**
     * Default Constructor.
     *
     * @param futures a List&lt;Future&lt;Boolean&gt;&gt; to convert to an aggregated Boolean.
     */
    private GetFuture(final List<Future<Boolean>> futures) {
      mFutures = futures;
    }

    /**
     * Aggregated Boolean return value of each Future in futures.
     *
     * @return Aggregated return value of each Future in futures.
     */
    public Boolean call() {
      boolean retVal = false;
      for (Future<Boolean> future: mFutures) {
        // block on completion of each future and update the return value to be true if any
        // future returns true.
        try {
          retVal = future.get() || retVal;
        } catch (ExecutionException ee) {
          if (ee.getCause() instanceof IOException) {
            LOG.warn("Custom freshness policy data request failed.  Failed freshness policy will"
                + "not run. " + ee.getCause().getMessage());
          } else {
            throw new RuntimeException(ee);
          }
        } catch (InterruptedException ie) {
          throw new RuntimeException("Freshening thread interrupted.", ie);
        }
      }
      return retVal;
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(final EntityId eid, final KijiDataRequest dataRequest) throws IOException {
    return get(eid, dataRequest, mTimeout);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(final EntityId eid, final KijiDataRequest dataRequest, final long timeout)
      throws IOException {
    final String getId = String.valueOf(mGetId.getAndIncrement());

    final Map<KijiColumnName, FreshnessCapsule> capsules = getCapsules(dataRequest);
    // If there are no freshness policies attached to the requested columns, return the requested
    // data.
    if (capsules.isEmpty()) {
      return mReader.get(eid, dataRequest);
    }

    final Future<KijiRowData> clientData = getClientData(eid, dataRequest);
    if (!mAllowPartialFresh) {
      final KijiBufferedWriter writer = mTable.getWriterFactory().openBufferedWriter();
      mBuffers.put(getId, writer);
    }
    final List<Future<Boolean>> futures =
        getFutures(capsules, clientData, eid, dataRequest, getId);

    final Future<Boolean> superFuture = mExecutor.submit(new GetFuture(futures));

    try {
      if (superFuture.get(timeout, TimeUnit.MILLISECONDS)) {
        // If superFuture returns true to indicate the need for a reread, do so.
        return mReader.get(eid, dataRequest);
      } else {
        try {
          return clientData.get(0L, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          // If clientData is not immediately available, read from the table.
          return mReader.get(eid, dataRequest);
        }
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("Freshening thread interrupted.", ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    } catch (TimeoutException te) {
      // If superFuture times out, read partially freshened data from the table or return the cached
      // data based on whether partial freshness is allowed.
      if (mAllowPartialFresh) {
        for (KijiFreshProducerContext context : mContextMap.get(getId)) {
          // If any context is finished we should reread from the table.
          if (context.isFinished()) {
            return mReader.get(eid, dataRequest);
          }
        }
        // If no contexts are finished we do not need to read from the table and should return stale
        // data.
        return returnStale(eid, dataRequest, clientData);
      } else {
        return returnStale(eid, dataRequest, clientData);
      }
    }
  }

  /**
   * Callable used by {@link #bulkGet(java.util.List, org.kiji.schema.KijiDataRequest)}.
   */
  private static final class BulkGetFuture implements Callable<List<KijiRowData>> {
    private final List<Future<KijiRowData>> mFutures;

    /**
     * Default constructor.
     *
     * @param futures a list of Future&lt;KijiRowData&gt; to convert into a List&lt;KijiRowData&gt;.
     */
    public BulkGetFuture(List<Future<KijiRowData>> futures) {
      mFutures = futures;
    }

    /**
     * Returns the results of internal futures.
     *
     * @return the resulting List&lt;KijiRowData&gt;.
     */
    public List<KijiRowData> call() {
      List<KijiRowData> results = Lists.newArrayList();
      for (Future<KijiRowData> future : mFutures) {
        try {
          results.add(future.get());
        } catch (InterruptedException ie) {
          throw new RuntimeException("Freshening thread interrupted.", ie);
        } catch (ExecutionException ee) {
          if (ee.getCause() instanceof IOException) {
            LOG.warn("Custom freshness policy data request failed.  Failed freshness policy will"
                + "not run. " + ee.getCause().getMessage());
          } else {
            throw new RuntimeException(ee);
          }
        }
      }
      return results;
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      List<EntityId> eids, final KijiDataRequest dataRequest) throws IOException {
    return bulkGet(eids, dataRequest, mTimeout);
  }


  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> eids, final KijiDataRequest dataRequest,
      final long timeout) throws IOException {
    final List<Future<KijiRowData>> futures = Lists.newArrayList();
    for (final EntityId eid : eids) {
      final Future<KijiRowData> future = mExecutor.submit(new Callable<KijiRowData>() {
        public KijiRowData call() throws IOException {
          return get(eid, dataRequest);
        }
      });
      futures.add(future);
    }
    final Future<List<KijiRowData>> superDuperFuture =
        mExecutor.submit(new BulkGetFuture(futures));

    final List<KijiRowData> futureResult;
    try {
      futureResult = superDuperFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      throw new RuntimeException("Freshening thread interrupted.", ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    } catch (TimeoutException te) {
      return mReader.bulkGet(eids, dataRequest);
    }
    if (futureResult != null) {
      return futureResult;
    } else {
      return mReader.bulkGet(eids, dataRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      KijiDataRequest dataRequest, KijiScannerOptions kijiScannerOptions) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public void preload(KijiDataRequest dataRequest) throws IOException {
    getCapsules(dataRequest);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // Release all cached freshness capsules, they will close when producers are finished with them.
    for (Map.Entry<KijiColumnName, FreshnessCapsule> entry : mCapsuleCache.entrySet()) {
      entry.getValue().release();
    }
    if (mRereadTask != null) {
      mRereadTask.cancel();
    }
    // Closing the reader releases the underlying table reference, so we do not have to release it
    // manually.
    mReader.close();
  }
}
