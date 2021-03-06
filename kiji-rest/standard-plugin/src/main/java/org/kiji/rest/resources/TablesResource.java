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
import static org.kiji.rest.RoutesConstants.TABLES_PATH;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PATH;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.representations.GenericResourceRepresentation;
import org.kiji.schema.Kiji;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt;/tables
 */
@Path(TABLES_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class TablesResource {
  private final KijiClient mKijiClient;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public TablesResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * GETs a list of tables in the specified instance.
   *
   * @param instance to list the contained tables.
   * @return a map of tables to their respective resource URIs for easier future access.
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public List<GenericResourceRepresentation> getTables(
      @PathParam(INSTANCE_PARAMETER) String instance) {

    List<GenericResourceRepresentation> tables = Lists.newArrayList();
    Kiji kiji = mKijiClient.getKiji(instance);
    try {
      for (String table : kiji.getTableNames()) {
        String tableUri = TABLE_PATH.replace("{" + TABLE_PARAMETER + "}", table)
            .replace("{" + INSTANCE_PARAMETER + "}", instance);
        tables.add(new GenericResourceRepresentation(table, tableUri));
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
    return tables;
  }
}
