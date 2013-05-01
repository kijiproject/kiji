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
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.kiji.rest.RoutesConstants;
import org.kiji.schema.KijiURI;

import com.yammer.metrics.annotation.Timed;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt/tables/&lt;table&gt;/rows
 */
@Path(RoutesConstants.ROWS_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class RowsResource extends AbstractKijiResource {
  /**
   * Default constructor.
   *
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public RowsResource(KijiURI cluster, Set<KijiURI> instances) {
    super(cluster, instances);
  }

  /**
   * GETs a Kiji row specified by the hex entity id.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param hexEntityId for the row in question
   * @param uriInfo containing query parameters.
   * @return a message containing the row in question
   * @throws java.io.IOException if the instance or table is unavailable.
   */
  @Path(RoutesConstants.HEX_ENTITY_ID_SUBPATH)
  @GET
  @Timed
  public String getRowByHexEntityId(
      @PathParam(RoutesConstants.INSTANCE_PARAMETER) String instance,
      @PathParam(RoutesConstants.TABLE_PARAMETER) String table,
      @PathParam(RoutesConstants.HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @Context UriInfo uriInfo
  ) throws IOException {
    return hexEntityId;
  }

  /**
   * PUTs a Kiji row specified by the hex entity id.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param hexEntityId for the row in question
   * @param uriInfo containing query parameters.
   * @return a message containing the row in question
   * @throws java.io.IOException if the instance or table is unavailable.
   */
  @Path(RoutesConstants.HEX_ENTITY_ID_SUBPATH)
  @PUT
  @Timed
  public String putRowByHexEntityId(
      @PathParam(RoutesConstants.INSTANCE_PARAMETER) String instance,
      @PathParam(RoutesConstants.TABLE_PARAMETER) String table,
      @PathParam(RoutesConstants.HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @Context UriInfo uriInfo
  ) throws IOException {
    return hexEntityId;
  }
}


