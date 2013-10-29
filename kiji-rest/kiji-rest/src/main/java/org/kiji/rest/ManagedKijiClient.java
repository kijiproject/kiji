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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.yammer.dropwizard.lifecycle.Managed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReader;

/**
 * Managed resource for tracking Kiji connections.
 */
public class ManagedKijiClient implements KijiClient, Managed {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKijiClient.class);
  private static final long TEN_MINUTES = 10 * 60 * 1000;

  private final Set<KijiURI> mInstances;
  private final Map<String, Kiji> mKijiMap;
  private final Map<String, LoadingCache<String, KijiTable>> mKijiTableMap;
  private final Map<String, LoadingCache<String, FreshKijiTableReader>> mFreshKijiTableReaderMap;

  /**
   * Constructs a ManagedKijiClient with the specified cluster and instances.
   *
   * @param instances set of available instances available to this client.
   */
  public ManagedKijiClient(Set<KijiURI> instances) {
    mInstances = ImmutableSet.copyOf(instances);
    mKijiMap = Maps.newConcurrentMap();
    mKijiTableMap = Maps.newConcurrentMap();
    mFreshKijiTableReaderMap = Maps.newConcurrentMap();
  }

  @Override
  public void start() throws Exception {
    for (KijiURI instance : mInstances) {
      final String instanceName = instance.getInstance();
      final Kiji kiji = Kiji.Factory.open(instance);
      mKijiMap.put(instanceName, kiji);

      final LoadingCache<String, KijiTable> kijiTableCache = CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, KijiTable>() {
                @Override
                public KijiTable load(String table) throws IOException {
                  return kiji.openTable(table);
                }
              }
          );
      kijiTableCache.getAll(kiji.getTableNames()); // Pre-load cache
      mKijiTableMap.put(instanceName, kijiTableCache);

      final LoadingCache<String, FreshKijiTableReader> freshReaderCache = CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, FreshKijiTableReader>() {
                @Override
                public FreshKijiTableReader load(String table) throws IOException {
                  return createFreshKijiTableReader(instanceName, table);
                }
              }
          );
      freshReaderCache.getAll(kiji.getTableNames());  // Pre-load cache
      mFreshKijiTableReaderMap.put(instanceName, freshReaderCache);
    }
    LOG.info("Successfully started ManagedKijiClient!");
  }

  @Override
  public KijiSchemaTable getKijiSchemaTable(String instance) {
    if (!mKijiMap.containsKey(instance)) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Response.Status.FORBIDDEN);
    }
    try {
      return mKijiMap.get(instance).getSchemaTable();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping ManagedKijiClient...");
    // Release resources in reverse instantiation order
    for (Cache<String, FreshKijiTableReader> cache : mFreshKijiTableReaderMap.values()) {
      for (FreshKijiTableReader reader : cache.asMap().values()) {
        ResourceUtils.closeOrLog(reader);
      }
      cache.invalidateAll();
    }
    for (Cache<String, KijiTable> cache : mKijiTableMap.values()) {
      for (KijiTable table : cache.asMap().values()) {
        ResourceUtils.releaseOrLog(table);
      }
      cache.invalidateAll();
    }
    for (Kiji kiji : mKijiMap.values()) {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Gets a Kiji object for the specified instance.  Client is responsible for releasing the Kiji
   * instance when done.
   *
   * @param instance of the Kiji to request.
   * @return Kiji object for reading instance data.
   * @throws javax.ws.rs.WebApplicationException if there is an error getting the instance OR
   *    if the instance requested is unavailable for handling via REST.
   */
  @Override
  public Kiji getKiji(String instance)  {
    if (!mKijiMap.containsKey(instance)) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Response.Status.FORBIDDEN);
    }

    return mKijiMap.get(instance).retain();
  }

  /** @return a collection of instances served by this client. */
  @Override
  public Collection<KijiURI> getInstances() {
    return mInstances;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable getKijiTable(String instance, String table) {
    if (!mKijiTableMap.containsKey(instance)) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Response.Status.FORBIDDEN);
    }
    try {
      // Verify that this table still exists in this instance.  If not, invalidate the entry in
      // the cache (REST-50).
      if (!mKijiMap.get(instance).getTableNames().contains(table)) {
        mKijiTableMap.get(instance).invalidate(table);
        mFreshKijiTableReaderMap.get(instance).invalidate(table);
      }
      return mKijiTableMap.get(instance).get(table).retain();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Response.Status status;
      if (cause instanceof KijiTableNotFoundException) {
        status = Response.Status.NOT_FOUND;
      } else {
        status = Response.Status.INTERNAL_SERVER_ERROR;
      }
      throw new WebApplicationException(cause, status);
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FreshKijiTableReader getFreshKijiTableReader(String instance, String table) {
    if (!mFreshKijiTableReaderMap.containsKey(instance)) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Response.Status.FORBIDDEN);
    }
    try {
      return mFreshKijiTableReaderMap.get(instance).get(table);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Response.Status status;
      if (cause instanceof KijiTableNotFoundException) {
        status = Response.Status.NOT_FOUND;
      } else {
        status = Response.Status.INTERNAL_SERVER_ERROR;
      }
      throw new WebApplicationException(cause, status);
    } catch (Exception e) {
      throw new WebApplicationException(e.getCause(), Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Creates a FreshKijiTableReader instance.  Should only be used by the loading cache.
   * @param instance in which the table resides.
   * @param table to be read.
   * @return FreshKijiTableReader instance.
   * @throws IOException if there is an error in constructing the instance.
   * @throws org.kiji.schema.KijiTableNotFoundException if the table does not exist.
   */
  private FreshKijiTableReader createFreshKijiTableReader(String instance, String table)
      throws IOException {
    try {
      return FreshKijiTableReader.Builder.create()
          .withTable(mKijiTableMap.get(instance).get(table))
          .withAutomaticReread(TEN_MINUTES)
          .withPartialFreshening(false)
          .build();
    } catch (ExecutionException e) {
      // Unwrap (if possible) and rethrow.  Will be caught by getFreshKijiTableReader().
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }
}
