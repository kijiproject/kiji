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
import static org.kiji.rest.RoutesConstants.ROWS_PATH;
import static org.kiji.rest.RoutesConstants.TABLE_PARAMETER;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.representations.KijiRestCell;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.ResourceUtils;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows
 */
@Path(ROWS_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class RowsResource extends AbstractRowResource {
  private static final String UNLIMITED_VERSIONS = "all";

  private final KijiClient mKijiClient;

  /**
   * Special constant to denote stream unlimited amount of rows
   * to the client.
   */
  private static final int UNLIMITED_ROWS = -1;

  /**
   * Since we are streaming the rows to the user, we need access to the object mapper
   * used by DropWizard to convert objects to JSON.
   */
  private final ObjectMapper mJsonObjectMapper;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   * @param jsonObjectMapper is the ObjectMapper used by DropWizard to convert from Java
   *        objects to JSON.
   */
  public RowsResource(KijiClient kijiClient, ObjectMapper jsonObjectMapper) {
    mKijiClient = kijiClient;
    mJsonObjectMapper = jsonObjectMapper;
  }

  /**
   * Class to support streaming KijiRows to the client.
   *
   */
  private class RowStreamer implements StreamingOutput {

    private Iterable<KijiRowData> mScanner = null;
    private final KijiTable mTable;
    private int mNumRows = 0;
    private final List<KijiColumnName> mColsRequested;

    /**
     * Construct a new RowStreamer.
     *
     * @param scanner is the iterator over KijiRowData.
     * @param table the table from which the rows originate.
     * @param numRows is the maximum number of rows to stream.
     * @param columns are the columns requested by the client.
     */
    public RowStreamer(Iterable<KijiRowData> scanner, KijiTable table, int numRows,
        List<KijiColumnName> columns) {
      mScanner = scanner;
      mTable = table;
      mNumRows = numRows;
      mColsRequested = columns;
    }

    /**
     * Performs the actual streaming of the rows.
     *
     * @param os is the OutputStream where the results are written.
     */
    @Override
    public void write(OutputStream os) {
      int numRows = 0;
      Writer writer = new BufferedWriter(new OutputStreamWriter(os, Charset.forName("UTF-8")));
      Iterator<KijiRowData> it = mScanner.iterator();
      boolean clientClosed = false;

      try {
        while (it.hasNext() && (numRows < mNumRows || mNumRows == UNLIMITED_ROWS)
            && !clientClosed) {
          KijiRowData row = it.next();
          KijiRestRow restRow = getKijiRestRow(row, mTable.getLayout(), mColsRequested);
          String jsonResult = mJsonObjectMapper.writeValueAsString(restRow);
          // Let's strip out any carriage return + line feeds and replace them with just
          // line feeds. Therefore we can safely delimit individual json messages on the
          // carriage return + line feed for clients to parse properly.
          jsonResult = jsonResult.replaceAll("\r\n", "\n");
          writer.write(jsonResult + "\r\n");
          writer.flush();
          numRows++;
        }
      } catch (IOException e) {
        clientClosed = true;
      } finally {
        if (mScanner instanceof KijiRowScanner) {
          try {
            ((KijiRowScanner) mScanner).close();
          } catch (IOException e1) {
            throw new WebApplicationException(e1, Status.INTERNAL_SERVER_ERROR);
          }
        }
      }

      if (!clientClosed) {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e) {
          throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
        }
      }
    }
  }

  /**
   * GETs a list of Kiji rows.
   *
   * @param instance is the instance where the table resides.
   * @param table is the table where the rows from which the rows will be streamed
   * @param jsonEntityId the entity_id of the row to return.
   * @param startHBaseRowKey the hex representation of the starting hbase row key.
   * @param endHBaseRowKey the hex representation of the ending hbase row key.
   * @param limit the maximum number of rows to return. Set to -1 to stream all rows.
   * @param columns is a comma separated list of columns (either family or family:qualifier) to
   *        fetch
   * @param maxVersionsString is the max versions per column to return.
   *        Can be "all" for all versions.
   * @param timeRange is the time range of cells to return (specified by min..max where min/max is
   *        the ms since UNIX epoch. min and max are both optional; however, if something is
   *        specified, at least one of min/max must be present.)
   * @return the Response object containing the rows requested in JSON
   */
  @GET
  @Timed
  @ApiStability.Experimental
  // CSOFF: ParameterNumberCheck - There are a bunch of query param options
  public Response getRows(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @QueryParam("eid") String jsonEntityId,
      @QueryParam("start_rk") String startHBaseRowKey,
      @QueryParam("end_rk") String endHBaseRowKey,
      @QueryParam("limit") @DefaultValue("100") int limit,
      @QueryParam("cols") @DefaultValue("*") String columns,
      @QueryParam("versions") @DefaultValue("1") String maxVersionsString,
      @QueryParam("timerange") String timeRange) {
    // CSON: ParameterNumberCheck - There are a bunch of query param options

    long[] timeRanges = null;
    KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);
    Iterable<KijiRowData> scanner = null;
    int maxVersions;
    KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();

    if (timeRange != null) {
      timeRanges = getTimestamps(timeRange);
    }

    try  {
      if (UNLIMITED_VERSIONS.equalsIgnoreCase(maxVersionsString)) {
        maxVersions = HConstants.ALL_VERSIONS;
      } else {
        maxVersions = Integer.parseInt(maxVersionsString);
      }
    } catch (NumberFormatException nfe) {
      throw new WebApplicationException(nfe, Status.BAD_REQUEST);
    }

    if (timeRange != null) {
      dataBuilder.withTimeRange(timeRanges[0], timeRanges[1]);
    }

    ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
    List<KijiColumnName> requestedColumns = addColumnDefs(kijiTable.getLayout(), colsRequested,
        columns);

    if (jsonEntityId != null && (startHBaseRowKey != null || endHBaseRowKey != null)) {
      throw new WebApplicationException(new IllegalArgumentException("Ambiguous request. "
          + "Specified both jsonEntityId and start/end HBase row keys."), Status.BAD_REQUEST);
    }

    // We will honor eid over start/end rk.
    try {
      if (jsonEntityId != null) {
        EntityId eid = ToolUtils.createEntityIdFromUserInputs(jsonEntityId, kijiTable.getLayout());
        KijiRowData returnRow = super.getKijiRowData(kijiTable, eid, dataBuilder.build());
        List<KijiRowData> tempRowList = Lists.newLinkedList();
        tempRowList.add(returnRow);
        scanner = tempRowList;
      } else {
        EntityIdFactory eidFactory = EntityIdFactory.getFactory(kijiTable.getLayout());
        final KijiScannerOptions scanOptions = new KijiScannerOptions();
        if (startHBaseRowKey != null) {
          EntityId eid = eidFactory.getEntityIdFromHBaseRowKey(Hex.decodeHex(startHBaseRowKey
              .toCharArray()));
          scanOptions.setStartRow(eid);
        }

        if (endHBaseRowKey != null) {
          EntityId eid = eidFactory.getEntityIdFromHBaseRowKey(Hex.decodeHex(endHBaseRowKey
              .toCharArray()));
          scanOptions.setStopRow(eid);
        }

        final KijiTableReader reader = kijiTable.openTableReader();

        scanner = reader.getScanner(dataBuilder.build(), scanOptions);
      }
    } catch (RuntimeException e) {
      throw new WebApplicationException(e, Status.BAD_REQUEST);
    } catch (Exception e) {
      throw new WebApplicationException(e, Status.BAD_REQUEST);
    } finally {
      ResourceUtils.releaseOrLog(kijiTable);
    }
    return Response.ok(new RowStreamer(scanner, kijiTable, limit, requestedColumns)).build();
  }

  /**
   * POSTs JSON file to row: performs create and update.
   * Note that the user-formatted entityId is required.
   * Also note that writer schema is not considered as of the latest version.
   * Following is an example of a postable JSON:
   * {
   * "entityId" : "hbase=hex:8c2d2fcc2c150efb49ce0817e1823d46",
   * "hbaseRowKey" : "8c2d2fcc2c150efb49ce0817e1823d46",
   * "cells" : [ {
   * "value" : "\"somevalue\"",
   * "timestamp" : 123,
   * "columnName" : "info",
   * "columnQualifier" : "firstname"
   * } ]
   * }
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param kijiRestRow POST-ed json data
   * @return a message containing the rowkey of interest
   * @throws IOException when post fails
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiStability.Experimental
  public Map<String, String> postCell(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      KijiRestRow kijiRestRow)
      throws IOException {
    final KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);

    // Default global timestamp.
    long globalTimestamp = System.currentTimeMillis();

    final EntityId entityId;
    if (null != kijiRestRow.getEntityId()) {
      entityId = ToolUtils.createEntityIdFromUserInputs(kijiRestRow.getEntityId(),
          kijiTable.getLayout());
    } else {
      throw new WebApplicationException(new IllegalArgumentException("EntityId was not specified."),
          Status.BAD_REQUEST);
    }

    // Open writer and write.
    final KijiTableWriter writer = kijiTable.openTableWriter();
    try {
      for (KijiRestCell kijiRestCell : kijiRestRow.getCells()) {
        final KijiColumnName column = new KijiColumnName(kijiRestCell.getColumnFamily(),
            kijiRestCell.getColumnQualifier());
        if (!kijiTable.getLayout().exists(column)) {
          throw new WebApplicationException(
              new IllegalArgumentException("Specified column does not exist: " + column),
              Response.Status.BAD_REQUEST);
        }
        final long timestamp;
        if (null != kijiRestCell.getTimestamp()) {
          timestamp = kijiRestCell.getTimestamp();
        } else {
          timestamp = globalTimestamp;
        }
        if (timestamp >= 0) {
          // Put to either a counter or a regular cell.
          if (SchemaType.COUNTER == kijiTable.getLayout().getCellSchema(column).getType()) {
            // Write the counter cell.
            putCounterCell(writer, entityId, kijiRestCell.getValue().toString(), column, timestamp);
          } else {
            // Set writer schema in preparation to write an Avro record.
            final Schema schema;
            try {
              schema = kijiTable.getLayout().getSchema(column);
            } catch (Exception e) {
              throw new WebApplicationException(e, Response.Status.BAD_REQUEST);
            }
            // Write the cell.
            putCell(writer, entityId, kijiRestCell.getValue().toString(),
                column, timestamp, schema);
          }
        }
      }
    } finally {
      ResourceUtils.closeOrLog(writer);
      ResourceUtils.releaseOrLog(kijiTable);
    }

    // Better output?
    Map<String, String> returnedTarget = Maps.newHashMap();

    URI targetResource = UriBuilder.fromResource(RowResource.class).build(instance, table,
        new String(Hex.encodeHex(entityId.getHBaseRowKey())));

    returnedTarget.put("target", targetResource.toString());

    return returnedTarget;

  }
}
