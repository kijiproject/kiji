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

package org.kiji.rest;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.rest.util.KijiInstanceCache;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.zookeeper.ZooKeeperUtils;
import org.kiji.scoring.FreshKijiTableReader;

/**
 * Managed resource for tracking Kiji connections.
 */
public class ManagedKijiClient implements KijiClient, Managed {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKijiClient.class);

  public static final long DEFAULT_TIMEOUT = 10;

  /** Holds instances currently being served. */
  private final LoadingCache<String, KijiInstanceCache> mInstanceCaches;

  /** CuratorFramework object which is backing <code>mZKInstances</code>. */
  private final CuratorFramework mZKFramework;

  /**
   * A 'cache' object which keeps track of the currently registered Kiji instances in ZooKeeper.
   */
  private final PathChildrenCache mZKInstances;

  /**
   * Holds the currently known Kiji instances as registered in ZooKeeper. It is sufficient for this
   * to be volatile (as opposed to atomic or using locks), because it always holds an immutable set,
   * and the referenced set is only changed by {@link #refreshInstances()}, which does not require
   * any check and set semantics.
   */
  private volatile Set<String> mKijiInstances;
  private final Set<String> mVisibleKijiInstances;

  /** Tracks the lifecycle state of this ManagedKijiClient. */
  private final AtomicReference<State> mState;

  /** The possible states of this ManagedKijiClient. */
  private static enum State {
    /** ManagedKijiClient is constructed, but not yet started. */
    INITIALIZED,
    /** ManagedKijiClient is started and alive. */
    STARTED,
    /** ManagedKijiClient is stopped. */
    STOPPED
  }

  /**
   * Constructs a ManagedKijiClient.
   *
   * @param configuration of HBase cluster to serve.
   * @throws IOException if error while creating connections to the cluster.
   */
  public ManagedKijiClient(final KijiRESTConfiguration configuration) throws IOException {
    this(KijiURI.newBuilder(configuration.getClusterURI()).build(),
         configuration.getCacheTimeout(),
         configuration.getVisibleInstances());
  }

  /**
   * Constructs a ManagedKijiClient.
   *
   * @param clusterURI of HBase cluster to serve.
   * @throws IOException if error while creating connections to the cluster.
   */
  public ManagedKijiClient(final KijiURI clusterURI) throws IOException {
    this(clusterURI, DEFAULT_TIMEOUT, new HashSet<String>());
  }


  /**
   * Constructs a ManagedKijiClient.
   *
   * @param clusterURI of HBase cluster to serve.
   * @param cacheTimeout time to hold open connections to instances and tables before clearing them
   *        from the cache.
   * @param visibleInstances is the set of instances that are specified as visible in the
   *        configuration.yml file. If this set is empty, all instances are considered to be
   *        visible.
   * @throws IOException if error while creating connections to the cluster.
   */
  public ManagedKijiClient(final KijiURI clusterURI,
                           final long cacheTimeout,
                           final Set<String> visibleInstances)
      throws IOException {
    mVisibleKijiInstances = visibleInstances;
    mZKFramework = ZooKeeperUtils.getZooKeeperClient(clusterURI);
    mZKInstances =
        new PathChildrenCache(
            mZKFramework,
            ZooKeeperUtils.INSTANCES_ZOOKEEPER_PATH.getPath(),
            true);
    mInstanceCaches = CacheBuilder
        .newBuilder()
        .expireAfterAccess(cacheTimeout, TimeUnit.MINUTES)
        .removalListener(new RemovalListener<String, KijiInstanceCache>() {
          @Override
          public void onRemoval(RemovalNotification<String, KijiInstanceCache> notification) {
            try {
              notification.getValue().stop(); // strong cache; should not be null
            } catch (IOException e) {
              LOG.warn("Unable to stop KijiInstanceCache {} for instance {}.",
                  notification.getValue(), notification.getKey());
            }
          }
        })
        .build(new CacheLoader<String, KijiInstanceCache>() {
          @Override
          public KijiInstanceCache load(String instanceName) throws Exception {
            final KijiURI instanceURI =
                KijiURI.newBuilder(clusterURI).withInstanceName(instanceName).build();

            // Check if our instances list contains the instance before attempting to construct it.
            if (!mKijiInstances.contains(instanceName)) {
              throw new KijiNotInstalledException(
                  "Kiji instance not found in known instances set.", instanceURI);
            }
            return new KijiInstanceCache(instanceURI);
          }
        });

    mState = new AtomicReference<State>(State.INITIALIZED);
  }

  /** {@inheritDoc} */
  @Override
  public void start() throws Exception {
    Preconditions.checkState(mState.compareAndSet(State.INITIALIZED, State.STARTED),
        "Can not start ManagedKijiClient in state %s.", mState.get());
    /** The listener updates the set of served instances based on ZK changes. */
    mZKInstances.getListenable().addListener(new InstanceListener());
    mZKInstances.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    refreshInstances();
    LOG.info("Successfully started ManagedKijiClient!");
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void stop() throws Exception {
    Preconditions.checkState(mState.compareAndSet(State.STARTED, State.STOPPED),
        "Can not stop in state %s.", mState.get());
    LOG.info("Stopping ManagedKijiClient.");

    ResourceUtils.closeOrLog(mZKInstances);
    ResourceUtils.closeOrLog(mZKFramework);

    mInstanceCaches.invalidateAll();
    mInstanceCaches.cleanUp();
  }

  /**
   * Retrieve the cache for a given instance.
   *
   * @param instance to retrieve cache for.
   * @return the instance cache.
   */
  private KijiInstanceCache getInstanceCache(String instance) {
    try {
      return mInstanceCaches.get(instance);
    } catch (Exception e) {
      final Throwable cause = e.getCause();
      throw new WebApplicationException(cause, getExceptionStatus(cause));
    }
  }

  /**
   * Match an exception against a response status.
   *
   * @param cause exception to match.
   * @return the appropriate response code.
   */
  private Response.Status getExceptionStatus(Throwable cause) {
    if (cause instanceof KijiNotInstalledException) {
      return Response.Status.FORBIDDEN;
    }
    if (cause instanceof KijiTableNotFoundException) {
      return Response.Status.NOT_FOUND;
    } else {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji(String instance) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not get a Kiji while in state %s.", state);
    return getInstanceCache(instance).getKiji();
  }

  /** {@inheritDoc} */
  @Override
  public KijiSchemaTable getKijiSchemaTable(String instance) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not get a schema table while in state %s.", state);
    try {
      return getKiji(instance).getSchemaTable();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Collection<String> getInstances() {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not get instances while in state %s.", state);
    return mKijiInstances;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable getKijiTable(String instance, String table) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not get Kiji table while in state %s.", state);
    try {
      return getInstanceCache(instance).getKijiTable(table);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      throw new WebApplicationException(cause, getExceptionStatus(cause));
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FreshKijiTableReader getFreshKijiTableReader(String instance, String table) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not get fresh Kiji table reader while in state %s.", state);
    try {
      return getInstanceCache(instance).getFreshKijiTableReader(table);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      throw new WebApplicationException(cause, getExceptionStatus(cause));
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateTable(String instance, String table) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not invalidate table while in state %s.", state);
    getInstanceCache(instance).invalidateTable(table);
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateInstance(String instance) {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not invalidate instance while in state %s.", state);
    mInstanceCaches.invalidate(instance);
  }

  /**
   * Update the instances served by this ManagedKijiClient.
   *
   * @throws IOException if an instance can not be added to the cache.
   */
  public void refreshInstances() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not invalidate instance while in state %s.", state);
    LOG.info("Refreshing instances.");

    Set<String> instances = Sets.newHashSet();
    for (ChildData node : mZKInstances.getCurrentData()) {
      instances.add(Iterables.getLast(Splitter.on('/').split(node.getPath())));
    }
    // Keep the intersection of the visible and actual sets.
    if (!mVisibleKijiInstances.isEmpty()) {
      instances.retainAll(mVisibleKijiInstances);
    }

    final ImmutableSet.Builder<String> instancesBuilder = ImmutableSet.builder();
    instancesBuilder.addAll(instances);
    mKijiInstances = instancesBuilder.build();
  }

  /**
   * Check whether this KijiClient is healthy.
   *
   * @return health status of the KijiClient.
   */
  public HealthCheck.Result checkHealth() {
    final State state = mState.get();
    Preconditions.checkState(state == State.STARTED,
        "Can not check health while in state %s.", state);
    List<String> issues = Lists.newArrayList();

    if (mZKFramework.getState() != CuratorFrameworkState.STARTED) {
      issues.add(String.format("ZooKeeper connection in unhealthy state %s.",
              mZKFramework.getState()));
    }

    for (KijiInstanceCache instanceCache : mInstanceCaches.asMap().values()) {
      issues.addAll(instanceCache.checkHealth());
    }
    if (issues.isEmpty()) {
      return HealthCheck.Result.healthy();
    } else {
      return HealthCheck.Result.unhealthy(Joiner.on('\n').join(issues));
    }
  }

  /**
   * A {@link PathChildrenCacheListener} to listen to the Kiji instances ZNode directory and
   * update the set of available Kiji instances.
   */
  private class InstanceListener implements PathChildrenCacheListener {
    /**
     * Creates a new InstanceListener to update the parent ManagedKijiClient.
     */
    public InstanceListener() {}

    @Override
    public void childEvent(
        CuratorFramework client,
        PathChildrenCacheEvent event
    ) throws IOException {
      LOG.debug("InstanceListener for {} triggered on event {}.", ManagedKijiClient.this, event);
      refreshInstances();
    }
  }
}
