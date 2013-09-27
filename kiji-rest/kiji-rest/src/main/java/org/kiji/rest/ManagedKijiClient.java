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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.Maps;
import com.yammer.dropwizard.lifecycle.Managed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTablePool;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * Managed resource for tracking Kiji connections.
 */
public class ManagedKijiClient implements KijiClient, Managed {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedKijiClient.class);

  private final Set<KijiURI> mInstances;
  private Map<String, Kiji> mKijiMap;
  private Map<String, KijiTablePool> mKijiTablePoolMap;
  private Map<String, KijiSchemaTable> mKijiSchemaTableMap;

  /**
   * Constructs a ManagedKijiClient with the specified cluster and instances.
   *
   * @param instances set of available instances available to this client.
   */
  public ManagedKijiClient(Set<KijiURI> instances) {
    mInstances = instances;
  }

  @Override
  public void start() throws Exception {
    mKijiMap = Maps.newHashMap();
    mKijiTablePoolMap = Maps.newHashMap();
    mKijiSchemaTableMap = Maps.newHashMap();

    for (KijiURI instance : mInstances) {
      final String instanceName = instance.getInstance();
      final Kiji kiji = Kiji.Factory.open(instance);
      mKijiMap.put(instanceName, kiji);

      final KijiTablePool tablePool = KijiTablePool.newBuilder(kiji).build();
      mKijiTablePoolMap.put(instanceName, tablePool);
      mKijiSchemaTableMap.put(instanceName, kiji.getSchemaTable());
    }
    LOG.info("Successfully started ManagedKijiClient!");
  }

  @Override
  public KijiSchemaTable getKijiSchemaTable(String instance) {
    return mKijiSchemaTableMap.get(instance);
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping ManagedKijiClient...");
    for (KijiTablePool tablePool : mKijiTablePoolMap.values()) {
      ResourceUtils.closeOrLog(tablePool);
    }
    for (Kiji kiji : mKijiMap.values()) {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Gets a Kiji object for the specified instance.  These instances are not thread safe, but
   * should be fine for read only access.  If modifications to a Kiji instance are desired, the
   * client is responsible for maintaining their own connection.  Client is responsible for
   * releasing the Kiji instance when done.
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

    // TODO: Wrap this kiji instance to make it strictly read only.
    final Kiji kiji = mKijiMap.get(instance);
    kiji.retain();
    return kiji;
  }

  /** @return a collection of instances served by this client. */
  @Override
  public Collection<KijiURI> getInstances() {
    return mInstances;
  }

  /**
   * Gets a Kiji table.  Caller is responsible for releasing the table when done.
   *
   * @param instance in which this table resides
   * @param table name of the requested table
   * @return KijiTable object
   * @throws WebApplicationException if there is an error.
   */
  @Override
  public KijiTable getKijiTable(String instance, String table) {
    final KijiTablePool tablePool = mKijiTablePoolMap.get(instance);
    KijiTable kijiTable;
    try {
      kijiTable = tablePool.get(table);
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
    return kijiTable;
  }
}
