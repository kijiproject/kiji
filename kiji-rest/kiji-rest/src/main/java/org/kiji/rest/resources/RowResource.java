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
import static org.kiji.rest.RoutesConstants.ROW_PATH;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.util.Set;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.yammer.metrics.annotation.Timed;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import org.kiji.rest.core.KijiRestRow;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * This REST resource interacts with a single Kiji row identified by its hbase rowkey (in hex).
 *
 * This resource is served for requests using the resource identifiers:
 * <ul>
 * <li>GET /v1/instances/&lt;instance&gt/tables/&lt;table&gt/rows/&lt;hex_row_key&gt;
 * <li>PUT /v1/instances/&lt;instance&gt/tables/&lt;table&gt/rows/&lt;hex_row_key&gt;
 * </ul>
 */
@Path(ROW_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class RowResource extends AbstractRowResource {

  /**
   * Default constructor.
   *
   * @param cluster KijiURI in which these instances are contained.
   *
   * @param instances The list of accessible instances.
   *
   */
  public RowResource(KijiURI cluster, Set<KijiURI> instances) {
    super(cluster, instances);
  }

  /**
   * PUTs a Kiji row specified by the hex entity id.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param hexEntityId for the row in question
   * @param uriInfo containing query parameters.
   * @return a message containing the row in question
   */
  @PUT
  @Timed
  public String putRowByHexEntityId(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId, @Context UriInfo uriInfo)
      {
    return hexEntityId;
  }

  /**
   * GETs a KijiRow given the hex representation of the hbase rowkey.
   *
   * @param instanceId is the instance name
   * @param tableId is the table name
   * @param hexEntityId is the hex representation of the hbase rowkey of the row to return
   * @param columns is a comma separated list of columns (either family or family:qualifier) to
   *        fetch
   * @param maxVersions is the max versions per column to return
   * @param timeRange is the time range of cells to return (specified by min..max where min/max is
   *        the ms since UNIX epoch. min and max are both optional; however, if something is
   *        specified, at least one of min/max must be present.)
   * @return a single KijiRestRow
   */
  @GET
  @Timed
  public KijiRestRow getRow(@PathParam(INSTANCE_PARAMETER) String instanceId,
      @PathParam(TABLE_PARAMETER) String tableId,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @QueryParam("cols") @DefaultValue("*") String columns,
      @QueryParam("versions") @DefaultValue("1") int maxVersions,
      @QueryParam("timerange") String timeRange) {

    byte[] hbaseRowKey = null;
    try {
      hbaseRowKey = Hex.decodeHex(hexEntityId.toCharArray());
    } catch (DecoderException e1) {
      throw new WebApplicationException(e1, Status.BAD_REQUEST);
    }
    long[] lTimeRange = null;
    if (timeRange != null) {
      lTimeRange = getTimestamps(timeRange);
    }
    final KijiTable table = super.getKijiTable(instanceId, tableId);
    //TODO: This currently leaks the table and we need to close resources
    //properly.
    return getKijiRow(table, hbaseRowKey, lTimeRange, columns, maxVersions);
  }
}
