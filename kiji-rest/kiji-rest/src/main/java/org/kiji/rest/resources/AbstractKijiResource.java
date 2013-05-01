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
package org.kiji.rest.resources;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Base class with helper methods for accessing Kiji resources.
 */
public abstract class AbstractKijiResource {
  private final KijiURI mCluster;
  private final Set<KijiURI> mInstances;

  /**
   * Construct the AbstractKijiResource with a partially constructed URI for
   * the cluster and the list of accessible instances.
   *
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public AbstractKijiResource(KijiURI cluster, Set<KijiURI> instances) {
    super();
    mCluster = cluster;
    mInstances = instances;
  }

  /** @return the cluster associated with this resource. */
  protected KijiURI getCluster() {
    return mCluster;
  }

  /** @return list of instances associated with this resource. */
  protected Set<KijiURI> getInstances() {
    return Collections.unmodifiableSet(mInstances);
  }

  /**
   * Gets a Kiji object for the specified instance.
   * @param instance of the Kiji to request.
   * @return Kiji object
   * @throws WebApplicationException if there is an error getting the instance OR
   *    if the instance requested is unavailable for handling via REST.
   */
  protected Kiji getKiji(String instance) throws IOException {
    final KijiURI kijiURI = KijiURI.newBuilder(getCluster()).withInstanceName(instance).build();
    if (!getInstances().contains(kijiURI)) {
      throw new WebApplicationException(new IOException("Instance " + instance + " unavailable!"),
          Status.FORBIDDEN);
    }
    final Kiji kiji = Kiji.Factory.open(kijiURI);
    // TODO Consider using a pool here.
    return kiji;
  }

  /**
   * Gets a Kiji table.
   *
   * @param instance in which this table resides
   * @param table name of the requested table
   * @return KijiTable object
   * @throws IOException if there is an error.
   */
  protected KijiTable getKijiTable(String instance, String table) throws IOException {
    final Kiji kiji = getKiji(instance);
    final KijiTable kijiTable = kiji.openTable(table);
    // TODO Consider using a KijiTablePool here.
    return kijiTable;
  }
}
