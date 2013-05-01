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
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.kiji.rest.RoutesConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifiers:
 * <li>/v1/instances/&lt;instance&gt/tables,
 * <li>/v1/instances/&lt;instance&gt/tables/&lt;table&gt;
 * <li>/v1/instances/&lt;instance&gt/tables/&lt;table&gt;/entityId
 */
@Path(RoutesConstants.TABLES_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource extends AbstractKijiResource {
  /**
   * Default constructor.
   *
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public TableResource(KijiURI cluster, Set<KijiURI> instances) {
    super(cluster, instances);
  }

  /**
   * GETs a list of tables in the specified instance.
   *
   * @param instance to list the contained tables.
   * @return a list of tables on the instance.
   * @throws IOException if the instance is unavailable.
   */
  @GET
  @Timed
  public Map<String, Object> getTableList(
      @PathParam(RoutesConstants.INSTANCE_PARAMETER) String instance
  ) throws IOException {
    final Map<String, Object> outputMap = Maps.newTreeMap();
    final Kiji kiji = getKiji(instance);
    outputMap.put("parent_kiji_uri", kiji.getURI().toOrderedString());
    outputMap.put("tables", kiji.getTableNames());
    kiji.release();
    return outputMap;
  }

  /**
   * GETs the layout of the specified table.
   *
   * @param instance in which the table resides.
   * @param table to get the layout for.
   * @return a message containing the layout of the specified table
   * @throws IOException if the instance or table is unavailable.
   */
  @Path(RoutesConstants.TABLE_PATH)
  @GET
  @Timed
  public String getTable(
      @PathParam(RoutesConstants.INSTANCE_PARAMETER) String instance,
      @PathParam(RoutesConstants.TABLE_PARAMETER) String table
  ) throws IOException {
    final Kiji kiji = getKiji(instance);
    final String layout = kiji
        .getMetaTable()
        .getTableLayout(table)
        .getDesc()
        .toString();
    kiji.release();
    return layout;
  }

  /**
   * GETs a hexadecimal EntityId using the components specified in the query.
   *
   * @param instance in which the table resides.
   * @param table to translate the entityId for.
   * @param uriInfo containing the query parameters.
   * @return a message containing a list of available sub-resources.
   * @throws IOException if the instance or table is unavailable.
   */
  @Path(RoutesConstants.ENTITY_ID_PATH)
  @GET
  @Timed
  public String getEntityId(
      @PathParam(RoutesConstants.INSTANCE_PARAMETER) String instance,
      @PathParam(RoutesConstants.TABLE_PARAMETER) String table,
      @Context UriInfo uriInfo
  ) throws IOException {
    return "getEntityId() called with query parameters:" + uriInfo.getQueryParameters().toString();
  }
}


