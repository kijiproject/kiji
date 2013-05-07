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

import java.io.IOException;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import com.sun.jersey.core.spi.factory.ResponseBuilderImpl;
import com.yammer.metrics.annotation.Timed;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MetadataBackup;

/**
 * This REST resource interacts with Kiji instances.
 *
 * This resource is served for requests using the resource identifiers: <li>/v1/instances/
 */
@Path(INSTANCE_PATH)
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
   * GETs a list of instances that are available.
   * @param instance is the instance whose metadata is being requested.
   * @return the metadata about the instance.
   */
  @GET
  @Timed
  public MetadataBackup getInstanceMetadata(@PathParam(INSTANCE_PARAMETER) String instance) {
    Kiji kiji = super.getKiji(instance);
    MetadataBackup backup = null;
    try {
      backup = MetadataBackup.newBuilder()
          .setLayoutVersion(kiji.getSystemTable().getDataVersion().toString())
          .setSchemaTable(kiji.getSchemaTable().toBackup())
          .setMetaTable(kiji.getMetaTable().toBackup()).build();
    } catch (IOException e) {
      ResponseBuilder builder = new ResponseBuilderImpl();
      builder.entity(e.getMessage());
      builder.status(Status.INTERNAL_SERVER_ERROR);
      throw new WebApplicationException(builder.build());
    }
    return backup;
  }
}
