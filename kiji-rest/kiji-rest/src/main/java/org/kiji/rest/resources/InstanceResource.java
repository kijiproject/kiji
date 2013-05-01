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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

import org.kiji.rest.RoutesConstants;
import org.kiji.rest.core.ContentReturnable;
import org.kiji.rest.core.ElementReturnable;
import org.kiji.rest.core.Returnable;
import org.kiji.schema.KijiURI;

/**
 * This REST resource interacts with Kiji instances.
 *
 * This resource is served for requests using the resource identifiers:
 * <li>/v1/instances/
 * <li>/v1/instances/&lt;instance&gt;
 */
@Path(RoutesConstants.INSTANCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class InstanceResource extends AbstractKijiResource {
  /**
   * Default constructor.
   *
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public InstanceResource(KijiURI cluster, Set<KijiURI> instances) {
    super(cluster, instances);
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
   * @param instance is the instance name to retrieve information about.
   * @return a Returnable message indicating the landing point.
   */
  @GET
  @Timed
  public Returnable instance(@PathParam(RoutesConstants.INSTANCE_PATH) final String instance) {
    ContentReturnable message = new ContentReturnable("instance: " + instance);
    message.add(new ElementReturnable("0.0.1"));
    return message;
  }

  /**
   * GETs a list of instances that are available.
   *
   * @return a list of instances on the cluster.
   */
  @GET
  @Timed
  public Map<String, Object> getInstanceList() {
    final List<String> listOfInstances = Lists.newArrayList();
    for (KijiURI instance : getInstances()) {
      listOfInstances.add(instance.getInstance());
    }
    final Map<String, Object> outputMap = Maps.newHashMap();
    outputMap.put("parent_kiji_uri", getCluster().toOrderedString());
    outputMap.put("instances", listOfInstances);
    return outputMap;
  }
}


