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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.rest.util.KijiInstanceCache;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.scoring.FreshKijiTableReader;

/**
 * Managed resource for tracking Kiji connections.
 */
public class ManagedKijiClient implements KijiClient, Managed {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKijiClient.class);

  /** Keeps track of visible instances. When instances are added/deleted, this map is mutated. */
  private final ConcurrentMap<String, KijiInstanceCache> mInstanceCaches;

  /** Holds the cluster URI being served by this REST server. */
  private final KijiURI mClusterURI;

  /** CuratorFramework object which is backing <code>mZKInstances</code>. */
  private final CuratorFramework mZKFramework;

  /**
   * A 'cache' object which can look up the current instances in Zookeeper, and updates this
   * ManagedKijiClient when instances are created or destroyed.
   */
  private final PathChildrenCache mZKInstances;

  /**
   * Constructs a ManagedKijiClient.
   *
   * @param clusterURI of HBase cluster to serve.
   * @throws IOException if error while creating connections to the cluster.
   */
  public ManagedKijiClient(KijiURI clusterURI) throws IOException {
    mInstanceCaches = Maps.newConcurrentMap();
    mClusterURI = clusterURI;

    mZKFramework = CuratorFrameworkFactory.newClient(
        clusterURI.getZooKeeperEnsemble(), new ExponentialBackoffRetry(1000, 3));
    mZKInstances =
        new PathChildrenCache(
            mZKFramework,
            ZooKeeperMonitor.INSTANCES_ZOOKEEPER_PATH.getPath(),
            true);
  }

  /** {@inheritDoc} */
  @Override
  public void start() throws Exception {
    mZKInstances.getListenable().addListener(new InstanceListener());
    mZKFramework.start();
    mZKInstances.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    refreshInstances();
    LOG.info("Successfully started ManagedKijiClient!");
  }

  /** {@inheritDoc} */
  @Override
  public void stop() throws Exception {
    LOG.info("Stopping ManagedKijiClient...");

    mZKInstances.close();
    mZKFramework.close();

    // Make the caches unavailable, and then stop them all
    List<KijiInstanceCache> cacheCopies = ImmutableList.copyOf(mInstanceCaches.values());
    mInstanceCaches.clear();
    for (KijiInstanceCache instanceCache: cacheCopies) {
      instanceCache.stop();
    }
  }

  /**
   * Retrieve the cache for a given instance.
   *
   * @param instance to retrieve cache for.
   * @return the instance cache.
   */
  private KijiInstanceCache getInstanceCache(String instance) {
    KijiInstanceCache instanceCache = mInstanceCaches.get(instance);
    if (instanceCache == null) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Response.Status.FORBIDDEN);
    }
    return instanceCache;
  }

  /**
   * Unwrap the provided ExecutionException and return the correct response type for its cause.
   *
   * @param e ExecutionException to unwrap.
   * @return the appropriate response code.
   */
  private Response.Status unwrapExecutionException(ExecutionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof KijiTableNotFoundException) {
      return Response.Status.NOT_FOUND;
    } else {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji(String instance) {
    return getInstanceCache(instance).getKiji();
  }

  /** {@inheritDoc} */
  @Override
  public KijiSchemaTable getKijiSchemaTable(String instance) {
    try {
      return getKiji(instance).getSchemaTable();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }


  /** {@inheritDoc} */
  @Override
  public Collection<String> getInstances() {
    return ImmutableSet.copyOf(mInstanceCaches.keySet());
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable getKijiTable(String instance, String table) {
    try {
      return getInstanceCache(instance).getKijiTable(table);
    } catch (ExecutionException e) {
      Response.Status status = unwrapExecutionException(e);
      throw new WebApplicationException(e.getCause(), status);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FreshKijiTableReader getFreshKijiTableReader(String instance, String table) {
    try {
      return getInstanceCache(instance).getFreshKijiTableReader(table);
    } catch (ExecutionException e) {
      Response.Status status = unwrapExecutionException(e);
      throw new WebApplicationException(e.getCause(), status);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateTable(String instance, String table) {
    getInstanceCache(instance).invalidateTable(table);
  }

  /**
   * Add a new instance to the set of served instances.
   *
   * @param uri of instance to add to cache.
   * @throws IOException if error while adding the instance.
   */
  private synchronized void addInstance(final KijiURI uri) throws IOException {
    if (!mInstanceCaches.containsKey(uri.getInstance())) {
      LOG.info("Adding instance {}.", uri);
      try {
        mInstanceCaches.put(uri.getInstance(), new KijiInstanceCache(uri));
      } catch (KijiNotInstalledException kine) {
        // The fix for SCHEMA-651 was to ensure that deleted instances remove their corresponding
        // information from zookeeper; however, if people are running REST against a cluster
        // that didn't have this fix, then there is a chance that zookeeper has some crud that
        // needs removing. Don't prevent KijiREST from starting completely, but simply log the
        // message and move on.
        LOG.error("Kiji " + uri + " not installed. Please check zookeeper for any old instances.");
      }
    }
  }

  /**
   * Remove cached reference to instance and all its readers.
   *
   * @param uri of instance to remove from cache.
   * @throws IOException if error while removing the instance.
   */
  private synchronized void removeInstance(final KijiURI uri) throws IOException {
    KijiInstanceCache instanceCache = mInstanceCaches.remove(uri.getInstance());
    if (instanceCache != null) {
      LOG.info("Removing instance {}.", uri);
      instanceCache.stop();
    }
  }

  /**
   * Retrieves the URIs for all served instances.
   *
   * @return the set of instance URIs being served.
   */
  private Set<KijiURI> getInstanceURIs() {
    final ImmutableSet.Builder<KijiURI> setBuilder = ImmutableSet.builder();
    for (KijiInstanceCache instanceCache : mInstanceCaches.values()) {
      setBuilder.add(instanceCache.getKiji().getURI());
    }
    return setBuilder.build();
  }

  /**
   * Update the instances served by this ManagedKijiClient.
   *
   * @throws IOException if an instance can not be added to the cache.
   */
  public void refreshInstances() throws IOException {
    LOG.info("Refreshing instances.");
    Set<KijiURI> instances = zNodesToInstanceURIs(mClusterURI, mZKInstances.getCurrentData());

    // Remove instances not in updatedInstances.
    for (KijiURI instance : getInstanceURIs()) {
      if (!instances.contains(instance)) {
        removeInstance(instance);
      }
    }

    // Add all instances.
    for (KijiURI instance : instances) {
      addInstance(instance);
    }
  }

  /**
   * Check whether this KijiClient is healthy.
   *
   * @return health status of the KijiClient.
   */
  public HealthCheck.Result checkHealth() {
    List<String> issues = Lists.newArrayList();
    for (KijiInstanceCache instanceCache : mInstanceCaches.values()) {
      issues.addAll(instanceCache.checkHealth());
    }
    if (issues.isEmpty()) {
      return HealthCheck.Result.healthy();
    } else {
      return HealthCheck.Result.unhealthy(Joiner.on('\n').join(issues));
    }
  }

  /**
   * Parses a Kiji instance name from its ZNode path.
   *
   * @param path The path to the instance's ZNode.
   * @return instance name
   */
  private static String parseInstanceName(final String path) {
    return Iterables.getLast(Splitter.on('/').split(path));
  }

  /**
   * Converts a list of instance ZNodes into a set of instance URIs with the same base URI as the
   * provided cluster URI.
   *
   * @param clusterURI of instances.
   * @param nodes containing instance names.
   * @return set of instance URIs.
   */
  private static Set<KijiURI> zNodesToInstanceURIs(KijiURI clusterURI, List<ChildData> nodes) {
    final ImmutableSet.Builder<KijiURI> instances = ImmutableSet.builder();
    for (ChildData node : nodes) {
      instances.add(
          KijiURI
              .newBuilder(clusterURI)
              .withInstanceName(parseInstanceName(node.getPath()))
              .build()
      );
    }
    return instances.build();
  }

  /**
   * A <code>PathChildrenCacheListener</code> to listen to the Kiji instances ZNode directory and
   * update a KijiClient's served instances when the available instances change.
   */
  private class InstanceListener implements PathChildrenCacheListener {
    /**
     * Creates a new InstanceListener to update the parent ManagedKijiClient.
     */
    public InstanceListener() {}

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
        throws IOException {
      LOG.debug("InstanceListener for {} triggered on event {}.", ManagedKijiClient.this, event);
      ManagedKijiClient.this.refreshInstances();
    }
  }
}
