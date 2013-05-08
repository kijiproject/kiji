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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
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

  /** Default time between automatic reloads.  0 indicates no automatic reloads. */
  private static final int DEFAULT_RELOAD_TIME = 0;

  /** The kiji table instance. */
  private final KijiTable mTable;

  /** Default reader to which to delegate reads. */
  private final KijiTableReader mReader;

  /** Freshener thread pool executor service. */
  private final ExecutorService mExecutor;

  /** Timeout duration for get requests. */
  private final int mTimeout;

  /** Time between automatically reloading freshness policies from the metatable in milliseconds. */
  private final int mReloadTime;

  /**
   * Map from column names to freshness policy records. Created on initialization of the
   * FreshKijiTableReader with all freshness policies for the entire table.  Only recreated when
   * the reader is closed and reopened.
   */
  private final Map<KijiColumnName, KijiFreshnessPolicyRecord> mPolicyRecords;

  /**
   * Cache of FreshnessCapsules containing a KijiFreshnessPolicy, a KijiProducer, and a
   * KeyValueStoreReaderFactory.  Lazily populated as needed.
   */
  private final Map<KijiColumnName, FreshnessCapsule> mCapsuleCache;

  /** TimerTask for automatically reloading freshness policies on a schedule. */
  private final ReloadTask mReloadTask;

  /**
   * Container class for KijiFreshnessPolicy and associated KijiProducer and
   * KeyValueStoreReaderFactory.
   */
  private static final class FreshnessCapsule {
    private final KijiFreshnessPolicy mPolicy;
    private final KijiProducer mProducer;
    private final KeyValueStoreReaderFactory mFactory;

    /**
     * Default Constructor.
     * @param policy the KijiFreshnessPolicy to serialize.
     * @param producer the KijiProducer to serialize.
     * @param factory the KeyValueStoreReaderFactory to serialize.
     */
    public FreshnessCapsule(
        final KijiFreshnessPolicy policy,
        final KijiProducer producer,
        final KeyValueStoreReaderFactory factory) {
      mPolicy = policy;
      mProducer = producer;
      mFactory = factory;
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
  }

  /** TimerTask for reloading freshness policies automatically on a schedule. */
  private final class ReloadTask extends TimerTask {
    /** Method to run when the task executes. */
    public void run() {
      try {
        reloadPolicies();
      } catch (IOException ioe) {
        LOG.warn("Failed to reload freshness policies.  Will attempt again in {} milliseconds",
            mReloadTime);
      }
    }
  }

  /**
   * Creates a new <code>InternalFreshKijiTableReader</code> instance that sends read requests
   * to a Kiji table and performs freshening on the returned data.  Never automatically reloads
   * policies from the meta table.  If there is no instance of FreshenerThreadPool in process, a new
   * one will be created with the default thread pool size.
   *
   * @param table the table that will be read/scored.
   * @param timeout the maximum number of milliseconds to spend trying to score data. If the
   *   process times out, stale data will be returned by
   *   {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} calls.
   * @throws IOException if an error occurs communicating with the table or meta table.
   */
  public InternalFreshKijiTableReader(KijiTable table, int timeout) throws IOException {
    this(table, timeout, DEFAULT_RELOAD_TIME, FreshenerThreadPool.DEFAULT_THREAD_POOL_SIZE);
  }

  /**
   * Creates a new <code>InternalFreshKijiTableReader</code> instance that sends read requests
   * to a Kiji table and performs freshening on returned data.  Automatically reloads freshness
   * policies from the meta table on a specified interval.  If there is no instance  of
   * FreshenerThreadPool in process, a new one will be created with the default thread pool size.
   *
   * @param table the KijiTable from which to read.
   * @param timeout the maximum number of milliseconds to spend trying to score data. If the
   *   process times out, stale data will be returned by
   *   {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} calls.
   * @param reloadTime The time to wait in milliseconds between automatically reloading freshness
   * policies.
   * @throws IOException if an error occurs communicating with the table or meta table.
   */
  public InternalFreshKijiTableReader(KijiTable table, int timeout, int reloadTime)
      throws IOException {
    this(table, timeout, reloadTime, FreshenerThreadPool.DEFAULT_THREAD_POOL_SIZE);
  }

  /**
   * Creates a new <code>InternalFreshKijiTableReader</code> instance that sends read requests
   * to a Kiji table and performs freshening on the returned data.  Automatically reloads freshness
   * policies from the meta table on a schedule.
   *
   * @param table the Kiji table that will be read/scored.
   * @param timeout the maximum number of milliseconds to spend trying to score data.  If the
   *   process times out, stale data will be returned by
   *   {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} calls.
   * @param reloadTime The time to wait in milliseconds between automatically reloading freshness
   * policies.
   * @param poolSize the size of a FreshenerThreadPool to create if one does not already exist.
   * @throws IOException if an error occurs communicating with the table or meta table.
   */
  public InternalFreshKijiTableReader(KijiTable table, int timeout, int reloadTime, int poolSize)
      throws IOException {
    mTable = table;
    // opening a reader retains the table, so we do not need to call retain manually.
    mReader = mTable.openTableReader();
    mExecutor = FreshenerThreadPool.getInstance(poolSize).getExecutorService();
    mTimeout = timeout;
    final KijiFreshnessManager manager = KijiFreshnessManager.create(table.getKiji());
    mPolicyRecords = manager.retrievePolicies(mTable.getName());
    mCapsuleCache = new HashMap<KijiColumnName, FreshnessCapsule>();
    if (reloadTime > 0) {
      final Timer reloadTimer = new Timer();
      mReloadTask = new ReloadTask();
      reloadTimer.scheduleAtFixedRate(mReloadTask, reloadTime, reloadTime);
      mReloadTime = reloadTime;
    } else if (reloadTime == 0) {
      mReloadTask = null;
      mReloadTime = 0;
    } else {
      throw new IllegalArgumentException(
          String.format("Reload time must be >= 0, found: %d", reloadTime));
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void reloadPolicies() throws IOException {
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> newRecords =
        KijiFreshnessManager.create(mTable.getKiji()).retrievePolicies(mTable.getName());
    for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : mPolicyRecords.entrySet()) {
      // Remove duplicate records from the new record map.
      if (newRecords.containsKey(entry.getKey())
          && newRecords.get(entry.getKey()) == entry.getValue()) {
        newRecords.remove(entry.getKey());
      }
      // Unload freshness policies for columns which no longer have policies attached.
      if (!newRecords.containsKey(entry.getKey())) {
        // Remove the policy record.
        mPolicyRecords.remove(entry.getKey());
        // Remove any cached objects.
        if (mCapsuleCache.containsKey(entry.getKey())) {
          final FreshnessCapsule capsule = mCapsuleCache.get(entry.getKey());
          capsule.getFactory().close();
          capsule.getProducer().cleanup(null);
          mCapsuleCache.remove(entry.getKey());
        }
      // Unload freshness policies from columns whose policies have changed.
      } else if (newRecords.get(entry.getKey()) != entry.getValue()) {
        // Remove the policy record.
        mPolicyRecords.remove(entry.getKey());
        // Remove any cached objects.
        if (mCapsuleCache.containsKey(entry.getKey())) {
          final FreshnessCapsule capsule = mCapsuleCache.get(entry.getKey());
          capsule.getFactory().close();
          capsule.getProducer().cleanup(null);
          mCapsuleCache.remove(entry.getKey());
        }

      }
    }
    mPolicyRecords.putAll(newRecords);
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
   * Gets all freshness policies from the local cache necessary to validate a given data request.
   * Returns an empty Map if there are no policies applicable to the data request.
   *
   * @param dataRequest the data request for which to find freshness policies.
   * @return A map from column name to KijiFreshnessPolicy.
   * @throws IOException if an error occurs while setting up a producer.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  Map<KijiColumnName, KijiFreshnessPolicy> getPolicies(KijiDataRequest dataRequest)
      throws IOException {
    final Map<KijiColumnName, KijiFreshnessPolicy> policies =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    final Collection<Column> columns = dataRequest.getColumns();
    for (Column column : columns) {
      final KijiColumnName family = new KijiColumnName(column.getFamily());
      if (mCapsuleCache.containsKey(column.getColumnName())) {
        policies.put(column.getColumnName(), mCapsuleCache.get(column.getColumnName()).getPolicy());
      } else if (mCapsuleCache.containsKey(family)) {
        policies.put(family, mCapsuleCache.get(family).getPolicy());
      } else {
        final KijiFreshnessPolicyRecord qualifiedRecord =
            mPolicyRecords.get(column.getColumnName());
        final KijiFreshnessPolicyRecord familyRecord = mPolicyRecords.get(family);
        // Ensure that there is only one freshness policy applicable to this column.
        Preconditions.checkState(!(qualifiedRecord != null && familyRecord != null),
            String.format("A record exists for both the family: %s and qualified column: %s%n"
                + "Only one may be specified.",
                family.toString(), column.getColumnName().toString()));
        KijiFreshnessPolicyRecord record = null;
        KijiColumnName columnName = null;
        if (qualifiedRecord != null) {
          record = qualifiedRecord;
          columnName = column.getColumnName();
        } else {
          record = familyRecord;
          columnName = family;
        }

        if (record != null) {
          // Instantiate and initialize the policies.
          final KijiFreshnessPolicy policy = policyForName(record.getFreshnessPolicyClass());
          policy.deserialize(record.getFreshnessPolicyState());
          // Add the policy to the list of policies applicable to this data request.
          policies.put(columnName, policy);

          // Instantiate the producer.
          final KijiProducer producer = producerForName(record.getProducerClass());

          // Create a kvstore reader factory for this policy and populate it with
          // required stores.
          final Map<String, KeyValueStore<?, ?>> kvMap = new HashMap<String, KeyValueStore<?, ?>>();
          kvMap.putAll(producer.getRequiredStores());
          kvMap.putAll(policy.getRequiredStores());
          KeyValueStoreReaderFactory factory = KeyValueStoreReaderFactory.create(kvMap);

          // Initialize the producer.
          producer.setup(KijiFreshProducerContext.create(mTable, columnName, null, factory));

          // Encapsulate the policy, producer, and factory and serialize them in a cache.
          final FreshnessCapsule capsule = new FreshnessCapsule(policy, producer, factory);
          mCapsuleCache.put(columnName, capsule);
        }
      }
    }
    return policies;
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
   * Creates a future for each {@link org.kiji.scoring.KijiFreshnessPolicy} applicable to a given
   * {@link org.kiji.schema.KijiDataRequest}.
   *
   * @param usesClientDataRequest A map from column name to KijiFreshnessPolicies that use the
   *   client data request to fulfill isFresh() calls.
   * @param usesOwnDataRequest A map from column name to KijiFreshnessPolicies that use custom
   *   data requests to fulfill isFresh() calls.
   * @param clientData A Future&lt;KijiRowData&gt; representing the data requested by the client.
   *   Freshness policies which use the client data request will block on the return of this future.
   * @param eid The EntityId specified by the client's call to get().
   * @param clientRequest the client's original request.
   * @return A list of Future&lt;Boolean&gt; representing the need to reread data from the table
   *   to include producer output after freshening.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  List<Future<Boolean>> getFutures(
      final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest,
      final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest,
      final Future<KijiRowData> clientData,
      final EntityId eid,
      final KijiDataRequest clientRequest) {
    final List<Future<Boolean>> futures = Lists.newArrayList();
    for (final KijiColumnName key: usesClientDataRequest.keySet()) {
      if (clientData == null) {
        throw new InternalKijiError(
            "Freshness policy usesClientDataRequest, but client data is null.");
      }
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
            final boolean isFresh = usesClientDataRequest.get(key).isFresh(rowData, policyContext);
            if (isFresh) {
              // If isFresh, return false to indicate a reread is not necessary.
              return Boolean.FALSE;
            } else {
              final KijiFreshProducerContext context =
                  KijiFreshProducerContext.create(
                  mTable, key, eid, mCapsuleCache.get(key).getFactory());
              final KijiProducer producer = mCapsuleCache.get(key).getProducer();
              producer.produce(mReader.get(eid, producer.getDataRequest()), context);

              // If a producer runs, return true to indicate a reread is necessary.  This assumes
              // the producer will write to the requested cells, eventually it may be appropriate
              // to actually check if this is true.
              return Boolean.TRUE;
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
              mReader.get(eid, usesOwnDataRequest.get(key).getDataRequest());
          final PolicyContext policyContext =
              new InternalPolicyContext(clientRequest, key, mTable.getKiji().getConf());
          final boolean isFresh = usesOwnDataRequest.get(key).isFresh(rowData, policyContext);
          if (isFresh) {
            // If isFresh, return false to indicate that a reread is not necessary.
            return Boolean.FALSE;
          } else {
            final KijiFreshProducerContext context =
                KijiFreshProducerContext.create(
                mTable, key, eid, mCapsuleCache.get(key).getFactory());
            final KijiProducer producer = mCapsuleCache.get(key).getProducer();
            producer.produce(mReader.get(eid, producer.getDataRequest()), context);
            // If a producer runs, return true to indicate that a reread is necessary.  This assumes
            // the producer will write to the requested cells, eventually it may be appropriate
            // to actually check if this is true.
            return Boolean.TRUE;
          }
        }
      });
      futures.add(requiresReread);
    }
    return futures;
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

    final Map<KijiColumnName, KijiFreshnessPolicy> policies = getPolicies(dataRequest);
    // If there are no freshness policies attached to the requested columns, return the requested
    // data.
    if (policies.size() == 0) {
      return mReader.get(eid, dataRequest);
    }

    final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    for (Map.Entry<KijiColumnName, KijiFreshnessPolicy> entry : policies.entrySet()) {
      if (entry.getValue().shouldUseClientDataRequest()) {
        usesClientDataRequest.put(entry.getKey(), entry.getValue());
      } else {
        usesOwnDataRequest.put(entry.getKey(), entry.getValue());
      }
    }

    final Future<KijiRowData> clientData = getClientData(eid, dataRequest);
    final List<Future<Boolean>> futures =
        getFutures(usesClientDataRequest, usesOwnDataRequest, clientData, eid, dataRequest);

    final Future<Boolean> superFuture = mExecutor.submit(new GetFuture(futures));

    try {
      if (superFuture.get(mTimeout, TimeUnit.MILLISECONDS)) {
        // If superFuture returns true to indicate the need for a reread, do so.
        return mReader.get(eid, dataRequest);
      } else {
        return clientData.get();
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("Freshening thread interrupted.", ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    } catch (TimeoutException te) {
      // If superFuture times out, return cached stale data.
      try {
        return clientData.get();
      } catch (InterruptedException ie) {
        throw new RuntimeException("Freshening thread interrupted.", ie);
      } catch (ExecutionException ee) {
        if (ee.getCause() instanceof IOException) {
          return mReader.get(eid, dataRequest);
        } else {
          throw new RuntimeException(ee);
        }
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
      futureResult = superDuperFuture.get(mTimeout, TimeUnit.MILLISECONDS);
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
  public void close() throws IOException {
    // Cleanup all cached producers.
    for (KijiColumnName key : mCapsuleCache.keySet()) {
      KeyValueStoreReaderFactory factory = mCapsuleCache.get(key).getFactory();
      mCapsuleCache.get(key)
          .getProducer().cleanup(KijiFreshProducerContext.create(mTable, key, null, factory));
      factory.close();
    }
    if (mReloadTask != null) {
      mReloadTask.cancel();
    }
    // Closing the reader releases the underlying table reference, so we do not have to release it
    // manually.
    mReader.close();
  }
}
