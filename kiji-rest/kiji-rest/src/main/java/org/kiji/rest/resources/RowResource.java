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
import static org.kiji.rest.util.RowResourceUtil.addColumnDefs;
import static org.kiji.rest.util.RowResourceUtil.getKijiRestRow;
import static org.kiji.rest.util.RowResourceUtil.getKijiRowData;
import static org.kiji.rest.util.RowResourceUtil.getTimestamps;
import static org.kiji.rest.util.RowResourceUtil.putCell;
import static org.kiji.rest.util.RowResourceUtil.putCounterCell;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.util.ByteArrayFormatter;
import org.kiji.schema.util.ResourceUtils;
/**
 * This REST resource interacts with a single Kiji row identified by its hbase rowkey (in hex).
 *
 * This resource is served for requests using the resource identifiers:
 * <ul>
 * <li>GET /v1/instances/&lt;instance&gt;/tables/&lt;table&gt/rows/&lt;hex_row_key&gt;
 * <li>PUT /v1/instances/&lt;instance&gt;/tables/&lt;table&gt/rows/&lt;hex_row_key&gt;
 * </ul>
 */
@Path(ROW_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class RowResource {
  private final KijiClient mKijiClient;

  /** Prefix for cell-specific schema parameter. */
  private static final String SCHEMA_PREFIX = "schema.";

  /** Query parameter specifying default timestamp for put. */
  private static final String TIMESTAMP_KEY = "timestamp";

  /** Prefix for cell-specific timestamp parameter. */
  private static final String TIMESTAMP_PREFIX = "timestamp.";

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public RowResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * Puts a Kiji row specified by the hex rowkey: performs create and update.
   * This operation is idempotent only when timestamp is specified.
   * Note that every table put is a 4-tuple: &lt;family:column, value, timestamp, schema&gt;.
   * Query parameters are constructed as follows:
   * <li>family:column=value - value is a JSON string.
   * <li>timestamp=t - t is the long global timestamp for all puts. Overrided by
   * 'timestamp.'-prefixed query parameter. This field is mandatory.
   * <li>timestamp.family:column=t - t is the long timestamp at which to put the corresponding
   * family:column=value. Warning: this overrides to the above global timestamp.
   * <li>schema.family:column=schema - schema is the JSON containing the schema of cell.
   * Optional; defaults to what is specified in the table layout.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param hexEntityId for the row of interest
   * @param uriInfo containing query parameters
   * @return a message containing the rowkey of interest
   * @throws IOException when row put fails
   */
  @PUT
  @Timed
  @ApiStability.Experimental
  public Map<String, String> putRow(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @Context UriInfo uriInfo)
      throws IOException {
    // Checks existence of mandatory global timestamp.
    if (!uriInfo.getQueryParameters().containsKey(TIMESTAMP_KEY)) {
      throw new WebApplicationException(new IllegalArgumentException("Timestamp is unspecified."),
          Response.Status.BAD_REQUEST);
    }
    // Default global timestamp. Will be set by a query parameter.
    long globalTimestamp = 0;

    final KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);

    final EntityIdFactory factory = EntityIdFactory.getFactory(kijiTable.getLayout());
    final EntityId entityId = factory.getEntityIdFromHBaseRowKey(
        ByteArrayFormatter.parseHex(hexEntityId));

    Map<KijiColumnName, String> schemasMap = Maps.newHashMap();
    Map<KijiColumnName, Long> timestampsMap = Maps.newHashMap();
    Map<KijiColumnName, String> valuesMap = Maps.newHashMap();

    // Parse the query map to extract schemas, timestamps, and values cell-wise.
    MultivaluedMap<String, String> queryMap = uriInfo.getQueryParameters();
    for (Map.Entry<String, List<String>> query : queryMap.entrySet()) {
      // TODO This parsing loop requires extensive validation.
      final String queryKey = query.getKey();

      // TODO If more than one queryValue is found, throw an exception.
      final String queryValue = query.getValue().get(0);
      if (queryKey.startsWith(SCHEMA_PREFIX)) {
        KijiColumnName column = new KijiColumnName(queryKey.substring(SCHEMA_PREFIX.length()));
        schemasMap.put(column, queryValue);
      } else if (queryKey.startsWith(TIMESTAMP_PREFIX)) {
        KijiColumnName column = new KijiColumnName(queryKey.substring(TIMESTAMP_PREFIX.length()));
        timestampsMap.put(column, Long.parseLong(queryValue));
      } else if (queryKey.equals(TIMESTAMP_KEY)) {
        globalTimestamp = Long.parseLong(queryValue);
      } else { // The query entry is column->value pair.
        KijiColumnName column = new KijiColumnName(queryKey);
        if (kijiTable.getLayout().exists(column)) {
          valuesMap.put(column, queryValue);
        } else {
          // TODO: Collect all of the columns that don't exist and throw one exception with them.
          throw new WebApplicationException(
              new IllegalArgumentException("Specified column does not exist: " + column),
              Response.Status.BAD_REQUEST);
        }
      }
    }

    // Open writer and write.
    final KijiTableWriter writer = kijiTable.openTableWriter();
    try {
      for (Map.Entry<KijiColumnName, String> entry : valuesMap.entrySet()) {
        final KijiColumnName column = entry.getKey();
        final String jsonValue = entry.getValue();

        final long timestamp;
        if (null != timestampsMap.get(column)) {
          timestamp = timestampsMap.get(column);
        } else {
          timestamp = globalTimestamp;
        }

        // Put to either a counter or a regular cell.
        if (SchemaType.COUNTER == kijiTable.getLayout().getCellSchema(column).getType()) {
          // Write the counter cell.
          putCounterCell(writer, entityId, jsonValue, column, timestamp);
        } else {
          // Get writer schema, otherwise, set schema in preparation to write an Avro record.
          final Schema schema;
          if (schemasMap.containsKey(column)) {
            try {
              schema = new Schema.Parser().parse(schemasMap.get(column));
            } catch (AvroRuntimeException are) {
              // TODO Make this a more informative exception.
              // Could not parse writer schema.
              throw new WebApplicationException(are, Response.Status.BAD_REQUEST);
            }
          } else {
            try {
              schema = kijiTable.getLayout().getSchema(column);
            } catch (Exception e) {
              // TODO Make this a more informative exception.
              throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
            }
          }
          // Write the cell.
          putCell(writer, entityId, jsonValue, column, timestamp, schema);
        }
      }
    } finally {
      ResourceUtils.closeOrLog(writer);
      ResourceUtils.releaseOrLog(kijiTable);
    }
    // Better output?
    Map<String, String> returnedTarget = Maps.newHashMap();
    returnedTarget.put("target", "/" + uriInfo.getPath());
    return returnedTarget;
 }

  /**
   * GETs a KijiRow given the hex representation of the hbase rowkey.
   *
   * @param instanceId is the instance name
   * @param tableId is the table name
   * @param hexEntityId is the hex representation of the hbase rowkey of the row to return
   * @param columns is a comma separated list of columns (either family or family:qualifier) to
   *        fetch
   * @param maxVersionsString is the max versions per column to return.
   *        Can be "all" for all versions.
   * @param timeRange is the time range of cells to return (specified by min..max where min/max is
   *        the ms since UNIX epoch. min and max are both optional; however, if something is
   *        specified, at least one of min/max must be present.)
   * @return a single KijiRestRow
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public KijiRestRow getRow(@PathParam(INSTANCE_PARAMETER) String instanceId,
      @PathParam(TABLE_PARAMETER) String tableId,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @QueryParam("cols") @DefaultValue("*") String columns,
      @QueryParam("versions") @DefaultValue("1") String maxVersionsString,
      @QueryParam("timerange") String timeRange) {

    byte[] hbaseRowKey = null;
    try {
      hbaseRowKey = Hex.decodeHex(hexEntityId.toCharArray());
    } catch (DecoderException e1) {
      throw new WebApplicationException(e1, Status.BAD_REQUEST);
    }

    int maxVersions;
    try {
      if ("all".equals(maxVersionsString)) {
        maxVersions = HConstants.ALL_VERSIONS;
      } else {
        maxVersions = Integer.parseInt(maxVersionsString);
      }
    } catch (NumberFormatException nfe) {
      throw new WebApplicationException(nfe, Status.BAD_REQUEST);
    }

    long[] timeRanges = null;
    if (timeRange != null) {
      timeRanges = getTimestamps(timeRange);
    }

    final KijiTable table = mKijiClient.getKijiTable(instanceId, tableId);
    try {
      EntityIdFactory eidFactory = EntityIdFactory.getFactory(table.getLayout());
      EntityId entityId = eidFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);

      KijiRestRow returnRow = null;

      KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
      if (timeRange != null) {
        dataBuilder.withTimeRange(timeRanges[0], timeRanges[1]);
      }

      ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
      List<KijiColumnName> requestedColumns = addColumnDefs(table.getLayout(), colsRequested,
          columns);

      KijiRowData row = getKijiRowData(table, entityId, dataBuilder.build());
      returnRow = getKijiRestRow(row, table.getLayout(), requestedColumns,
          mKijiClient.getKijiSchemaTable(instanceId));

      return returnRow;
    } catch (IOException e) {
      throw new WebApplicationException(e);
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }
}
