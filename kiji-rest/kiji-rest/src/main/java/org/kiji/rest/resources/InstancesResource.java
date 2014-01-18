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

import static org.kiji.rest.RoutesConstants.INSTANCES_PATH;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.INSTANCE_PATH;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.yammer.metrics.annotation.Timed;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.representations.GenericResourceRepresentation;

/**
 * This REST resource interacts with Kiji instances collection resource.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/
 */
@Path(INSTANCES_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class InstancesResource {
  private final KijiClient mKijiClient;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public InstancesResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * GETs a list of instances that are available.
   *
   * @return a map of instances to their resource URIs for easier future access
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public List<GenericResourceRepresentation> getInstanceList() {
    List<GenericResourceRepresentation> instanceList = Lists.newArrayList();

    for (String instance : mKijiClient.getInstances()) {
      instanceList.add(new GenericResourceRepresentation(instance,
          INSTANCE_PATH.replace("{" + INSTANCE_PARAMETER + "}", instance)));
    }
    return instanceList;
  }
}
