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
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.kiji.rest.RoutesConstants;
import org.kiji.schema.util.VersionInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

/**
 * This REST resource interacts with the Kiji cluster.
 *
 * This resource is served for requests using the resource identifier:
 * /v1/
 */
@Path(RoutesConstants.API_ENTRY_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class KijiRESTResource {
  /**
   * GETs a message containing a list of the available sub-resources.
   * @return a message containing a list of available sub-resources.
   */
  @GET
  @Timed
  public Map<String, Object> getRoot() {
    Map<String, Object> namespace = Maps.newHashMap();
    namespace.put("service", "KijiREST");
    List<String> resources = Lists.newArrayList();
    resources.add("version");
    resources.add("instances");
    namespace.put("resources", resources);
    return namespace;
  }

  /**
   * GETs version information.
   *
   * @return A message containing version information.
   * @throws IOException when Kiji software version can not be determined.
   */
  @Path(RoutesConstants.VERSION_ENDPOINT)
  @GET
  @Timed
  public Map<String, Object> getVersion() throws IOException {
    Map<String, Object> version = Maps.newHashMap();
    version.put("kiji-client-data-version", VersionInfo.getClientDataVersion());
    version.put("kiji-software-version", VersionInfo.getSoftwareVersion());
    version.put("rest-version", "0.1.0");
    return version;
  }
}
