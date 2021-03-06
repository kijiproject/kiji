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

import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.INSTANCE_PATH;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.schema.Kiji;
import org.kiji.schema.avro.MetadataBackup;

/**
 * This REST resource interacts with Kiji instances.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt;
 */
@Path(INSTANCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class InstanceResource {
  private final KijiClient mKijiClient;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public InstanceResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * GETs a list of instances that are available.
   * @param instance is the instance whose metadata is being requested.
   * @return the metadata about the instance.
   */
  @GET
  @Timed
  @ApiStability.Experimental
  public MetadataBackup getInstanceMetadata(@PathParam(INSTANCE_PARAMETER) String instance) {
    Kiji kiji = mKijiClient.getKiji(instance);
    try {
      MetadataBackup backup = MetadataBackup.newBuilder()
          .setLayoutVersion(kiji.getSystemTable().getDataVersion().toString())
          .setSystemTable(kiji.getSystemTable().toBackup())
          .setSchemaTable(kiji.getSchemaTable().toBackup())
          .setMetaTable(kiji.getMetaTable().toBackup()).build();
      return backup;
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
  }
}
