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

import static org.kiji.rest.RoutesConstants.ENTITY_ID_PATH;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.yammer.metrics.annotation.Timed;
import org.apache.commons.codec.binary.Hex;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.representations.EntityIdWrapper;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.ResourceUtils;

/**
 * This REST resource represents an entity_id in Kiji.
 *
 * This resource is served for requests using the resource identifiers:
 * <li>GET /v1/instances/&lt;instance&gt/tables/&lt;table&gt/entityId;
 */
@Path(ENTITY_ID_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class EntityIdResource {
  private final KijiClient mKijiClient;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public EntityIdResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * GETs a hexadecimal EntityId using the components specified in the query.
   *
   * @param instance in which the table resides.
   * @param table to translate the entityId for.
   * @param eid is the JSON array of components of the entityId.
   * @return the resulting entity_id in a JSON object.
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public EntityIdWrapper getEntityId(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @QueryParam("eid") String eid) {

    if (eid == null || eid.trim().isEmpty()) {
      throw new WebApplicationException(new IllegalArgumentException("Entity ID JSON required!"),
          Status.BAD_REQUEST);
    }

    KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);
    try {
      KijiTableLayout layout = kijiTable.getLayout();

      EntityId entityId = null;
      try {
        entityId = ToolUtils.createEntityIdFromUserInputs(eid, layout);
      } catch (IOException e) {
        throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
      }
      EntityIdWrapper wrapper = new EntityIdWrapper();
      wrapper.setRowKey(Hex.encodeHexString(entityId.getHBaseRowKey()));

      return wrapper;
    } finally {
      ResourceUtils.releaseOrLog(kijiTable);
    }
  }
}
