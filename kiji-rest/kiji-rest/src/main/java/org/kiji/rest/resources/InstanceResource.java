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

import static org.kiji.rest.resources.ResourceConstants.API_ENTRY_POINT;
import static org.kiji.rest.resources.ResourceConstants.INSTANCES;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

import org.apache.hadoop.util.StringUtils;

import org.kiji.rest.util.VersionInfo;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

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
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public InstanceResource(KijiURI cluster, List<KijiURI> instances) {
    super();
    mCluster = cluster;
    mInstances = instances;
  }

  /**
   * Called when the terminal resource element is the instance name.
   * @param instance to inspect
   * @return a message containing a list of available sub-resources.
   * @throws IOException if the instance is unavailable.
   */
  @Path("{instance}")
  @GET
  @Timed
  public Map<String, Object> instance(@PathParam("instance") String instance) throws IOException {
    final KijiURI kijiURI = KijiURI.newBuilder(mCluster).withInstanceName(instance).build();
    if (!mInstances.contains(kijiURI)) {
      throw new IOException("Instance unavailable");
    }

    Map<String, Object> namespace = Maps.newHashMap();
    namespace.put("instance_name", kijiURI.getInstance());
    namespace.put("kiji_uri", kijiURI.toOrderedString());
    List<String> resources = Lists.newArrayList();
    resources.add("tables");
    resources.add("version");
    namespace.put("resources", resources);
    return namespace;
  }

  /**
   * Lists instances on the cluster which are accessible via this REST service.
   * Called when the terminal resource element is not identified.
   *
   * @return a list of instances on the cluster.
   */
  @GET
  @Timed
  public Map<String, Object> list() {
    final List<String> listOfInstances = Lists.newArrayList();
    for (KijiURI instance : mInstances) {
      listOfInstances.add(instance.getInstance());
    }
    final Map<String, Object> outputMap = Maps.newHashMap();
    outputMap.put("parent_kiji_uri", mCluster.toOrderedString());
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

  /**
   * Singleton resource identifying the version information.
   *
   * @param instance the instance whose version is sought.
   * @return A message containing version information.
   * @throws IOException when Kiji software version can not be determined.
   */
  @Path("{instance}/version")
  @GET
  @Timed
  public Map<String, Object> version(@PathParam("instance") String instance) throws IOException {
    final KijiURI kijiURI = KijiURI.newBuilder(mCluster).withInstanceName(instance).build();
    if (!mInstances.contains(kijiURI)) {
      throw new IOException("Instance unavailable");
    }
    Map<String, Object> version = Maps.newHashMap();
    version.put("kiji-client-data-version",
        org.kiji.schema.util.VersionInfo.getClientDataVersion());
    version.put("kiji-software-version",
        org.kiji.schema.util.VersionInfo.getSoftwareVersion());

    final Kiji kiji = Kiji.Factory.open(kijiURI);
    version.put("kiji-instance-version",
        org.kiji.schema.util.VersionInfo.getClusterDataVersion(kiji));
    version.put("is_kiji_version_compatible",
        org.kiji.schema.util.VersionInfo.isKijiVersionCompatible(kiji));
    kiji.release();

    version.put("rest-version", VersionInfo.getRESTVersion());
    return version;
  }
}


