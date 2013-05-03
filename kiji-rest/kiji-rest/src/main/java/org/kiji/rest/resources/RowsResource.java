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

import static org.kiji.rest.RoutesConstants.HEX_ENTITY_ID_PARAMETER;
import static org.kiji.rest.RoutesConstants.INSTANCE_PARAMETER;
import static org.kiji.rest.RoutesConstants.ROWS_PATH;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.yammer.metrics.annotation.Timed;

import org.kiji.rest.core.KijiRestRow;
import org.kiji.schema.KijiURI;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt/tables/&lt;table&gt;/rows
 */
@Path(ROWS_PATH)
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
   * @return a message containing the row in question
   */
  @GET
  @Timed
  public List<KijiRestRow> getRowByHexEntityId(
      @PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId
  ) {
    return Lists.newArrayList();
  }
}


