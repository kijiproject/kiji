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
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.util.StringUtils;
import org.kiji.rest.core.ContentReturnable;
import org.kiji.rest.core.ElementReturnable;
import org.kiji.rest.core.Returnable;
import static org.kiji.rest.resources.ResourceConstants.API_ENTRY_POINT;
import static org.kiji.rest.resources.ResourceConstants.INSTANCES;
import org.kiji.schema.KijiURI;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

/**
 * This REST resource interacts with Kiji instances.
 * This resource is served whenever requests are made using the following
 * resource identifiers: /v1/instances/, /v1/instances/&lt;singleton&gt;,
 * /v1/instances/&lt;instance&gt;.
 */
@Path(API_ENTRY_POINT + INSTANCES)
@Produces(MediaType.APPLICATION_JSON)
public class InstanceResource {
  private final KijiURI mCluster;
  private final List<KijiURI> mInstances;

  /**
   * Construct the InstanceResource with a partially constructed URI for
   * the cluster and the list of accessible instances.
   *
   * @param clusterURI The builder for the cluster's URI.
   * @param instances The list of accessible instances.
   */
  public InstanceResource(KijiURI cluster, List<KijiURI> instances) {
    super();
    mCluster = cluster;
    mInstances = instances;
  }

  /**
   * Called when the terminal resource element is the singleton 'version'.
   * @return a Returnable message indicating the version.
   */
  @Path("version")
  @GET
  @Timed
  public Returnable version() {
    ContentReturnable version = new ContentReturnable("Version");
    version.add(new ElementReturnable("0.0.1"));
    return version;
  }

  /**
   * Called when the terminal resource element is the instance name.
   * @return a Returnable message indicating the landing point.
   */
  @Path("{instance}")
  @GET
  @Timed
  public Returnable instance(final @PathParam("instance") String instance) {
    ContentReturnable message = new ContentReturnable("instance: " + instance);
    message.add(new ElementReturnable("0.0.1"));
    return message;
  }

  /**
   * Lists instances on the cluster which are accessible via this REST service.
   * Called when the terminal resource element is not identified.
   *
   * @return a list of instances on the cluster.
   */
  @GET
  @Timed
  public Map<String, Object> list() throws IOException {
    final Map<String, Object> outputMap = Maps.newHashMap();
    final List<String> listOfInstances = Lists.newArrayList();
    for (KijiURI instance : mInstances) {
      listOfInstances.add(instance.toString());
    }
    outputMap.put("kiji_uri", mCluster.toOrderedString());
    outputMap.put("instances", listOfInstances);
    return outputMap;
  }

  /**
   * Parses a table name for a kiji instance name.
   *
   * @param kijiTableName The table name to parse
   * @return instance name (or null if none found)
   */
  protected static String parseInstanceName(String kijiTableName) {
    final String[] parts = StringUtils.split(kijiTableName, '\u0000', '.');
    if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }
}


