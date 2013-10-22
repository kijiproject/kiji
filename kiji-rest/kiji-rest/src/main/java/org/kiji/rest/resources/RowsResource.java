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
import static org.kiji.rest.util.RowResourceUtil.addColumnDefs;
import static org.kiji.rest.util.RowResourceUtil.getKijiRestRow;
import static org.kiji.rest.util.RowResourceUtil.getTimestamps;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.annotation.Timed;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.rest.config.FresheningConfiguration;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.util.RowResourceUtil;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.filter.FormattedEntityIdRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReader;

/**
 * This REST resource interacts with Kiji tables.
 *
 * This resource is served for requests using the resource identifier: <li>
 * /v1/instances/&lt;instance&gt;/tables/&lt;table&gt;/rows
 */
@Path(ROWS_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class RowsResource {
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
   * Configuration values to use while freshening.
   */
  private final FresheningConfiguration mFreshenConfig;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   * @param jsonObjectMapper is the ObjectMapper used by DropWizard to convert from Java
   *        objects to JSON.
   * @param freshenConfig to use with freshening reader.
   */
  public RowsResource(KijiClient kijiClient, ObjectMapper jsonObjectMapper,
      FresheningConfiguration freshenConfig) {
    mKijiClient = kijiClient;
    mJsonObjectMapper = jsonObjectMapper;
    mFreshenConfig = freshenConfig;
  }

  /**
   * Class to support streaming KijiRows to the client.
   *
   */
  private class RowStreamer implements StreamingOutput {

    private Iterable<KijiRowData> mScanner = null;
    private final KijiTable mTable;
    private final KijiSchemaTable mSchemaTable;

    private int mNumRows = 0;
    private final List<KijiColumnName> mColsRequested;

    /**
     * Construct a new RowStreamer.
     *
     * @param scanner is the iterator over KijiRowData.
     * @param table the table from which the rows originate.
     * @param numRows is the maximum number of rows to stream.
     * @param columns are the columns requested by the client.
     * @param schemaTable is the handle to the KijiSchemaTable used to encode the cell's writer
     *        schema as a UID.
     */
    public RowStreamer(Iterable<KijiRowData> scanner, KijiTable table, int numRows,
        List<KijiColumnName> columns, KijiSchemaTable schemaTable) {
      mScanner = scanner;
      mTable = table;
      mNumRows = numRows;
      mColsRequested = columns;
      mSchemaTable = schemaTable;
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
          KijiRestRow restRow = getKijiRestRow(row, mTable.getLayout(), mColsRequested,
              mSchemaTable);
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
   * @param freshen determines whether freshening should be done as part of the request.
   * @param timeout amount of time in ms to wait for freshening to finish before returning the
   *                old/stale/previous value of the column(s).
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
      @QueryParam("timerange") String timeRange,
      @QueryParam("freshen") Boolean freshen,
      @QueryParam("timeout") Long timeout) {
    // CSON: ParameterNumberCheck - There are a bunch of query param options

    long[] timeRanges = null;
    KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);
    Iterable<KijiRowData> scanner = null;
    int maxVersions;
    KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();

    if (timeRange != null) {
      timeRanges = getTimestamps(timeRange);
    }

    try {
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
        // Attempt to parse eid for potential wildcards.
        // Construction of EntityIdComponents fails if jsonEntityId is not a valid.
        try {
          final EntityIdComponents components = new EntityIdComponents(jsonEntityId);
          if (components.isWildcarded()) {
            // Wildcards were found, continue with FormattedEntityIdRowFilter.
            final KijiRowFilter entityIdRowFilter =
                new FormattedEntityIdRowFilter(
                    (RowKeyFormat2) kijiTable.getLayout().getDesc().getKeysFormat(),
                    components.getComponents());
            final KijiTableReader reader = kijiTable.openTableReader();
            final KijiScannerOptions scanOptions = new KijiScannerOptions();
            scanOptions.setKijiRowFilter(entityIdRowFilter);
            scanner = reader.getScanner(dataBuilder.build(), scanOptions);
          } else {
            // No wildcards found, but valid json entity id. Continue scanning point row.
            EntityId eid = kijiTable.getEntityId(components.getComponents());
            KijiDataRequest request = dataBuilder.build();
            scanner = ImmutableList.of(getKijiRowData(
                kijiTable,
                eid,
                request,
                freshen != null ? freshen : mFreshenConfig.isFreshen(),
                timeout != null ? timeout : mFreshenConfig.getTimeout()));
          }
        } catch (IOException io) {
          // Eid could not be parsed as a json. Try parsing as a byte array through ToolUtils.
          EntityId eid = ToolUtils.createEntityIdFromUserInputs(
              jsonEntityId,
              kijiTable.getLayout());
          KijiDataRequest request = dataBuilder.build();
          scanner = ImmutableList.of(getKijiRowData(
              kijiTable,
              eid,
              request,
              freshen != null ? freshen : mFreshenConfig.isFreshen(),
              timeout != null ? timeout : mFreshenConfig.getTimeout()));
        }
      } else {
        // Json array eid not found. Continue with a rowkey scan.
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
      // TODO(REST-51): Close the above reader.
      ResourceUtils.releaseOrLog(kijiTable);
    }
    KijiSchemaTable schemaTable = mKijiClient.getKijiSchemaTable(instance);
    return Response.ok(new RowStreamer(scanner, kijiTable, limit, requestedColumns,
        schemaTable)).build();
  }

  /**
   * Maintains list of components which can be formed into an entityId (isWildcarded is false);
   * or fed into FormattedEntityIdRowFilter (isWildcarded is true).
   */
  private static class EntityIdComponents {
    private final Object[] mComponents;
    private boolean mIsWildcarded = false;

    /**
     * Given a json entity id, constructs an object encapsulating an array of components and
     * specifies whether there are wildcards (with nulls array elements).
     *
     * @param json string of components of entity id.
     * @throws IOException if entity id can not be resolved as a json array.
     */
    public EntityIdComponents(final String json) throws IOException {
      final List<Object> components = Lists.newArrayList();
      final ObjectMapper mapper = new ObjectMapper();
      final JsonParser parser = new JsonFactory().createJsonParser(json)
          .enable(Feature.ALLOW_COMMENTS)
          .enable(Feature.ALLOW_SINGLE_QUOTES)
          .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      final JsonNode node = mapper.readTree(parser);

      if (node.isArray()) {
        for (int i = 0; i < node.size(); i++) {
          final Object component = getNodeValue(node.get(i));
          if (component.equals(WildcardSingleton.INSTANCE)) {
            // Replace wildcards with nulls in order to feed into FormattedEntityIdRowFilter
            mIsWildcarded = true;
            components.add(null);
          } else {
            components.add(component);
          }
        }
      } else if (node.isObject()) {
        // TODO: Implement map row key specifications:
        throw new RuntimeException("Map row key specifications are not implemented yet.");
      } else {
        components.add(getNodeValue(node));
      }
      mComponents = components.toArray();
    }

    /**
     * Gets the array of components.
     *
     * @return array of components.
     */
    public Object[] getComponents() {
      return mComponents;
    }

    /**
     * Are any of the components wildcarded...
     *
     * @return true iff at least one component is a wildcard (indicated by a null).
     */
    public boolean isWildcarded() {
      return mIsWildcarded;
    }

    /**
     * Singleton object to use to represent a wildcard.
     */
    private static enum WildcardSingleton {
      INSTANCE;
    }

    /**
     * Converts a JSON string, integer, or wildcard (empty array)
     * node into a Java object (String, Integer, Long, WILDCARD, or null).
     *
     * @param node JSON string, integer numeric, or wildcard (empty array) node.
     * @return the JSON value, as a String, an Integer, a Long, a WILDCARD, or null.
     * @throws IOException if the JSON node is not a String, Integer, Long, WILDCARD, or null.
     */
    private static Object getNodeValue(JsonNode node) throws IOException {
      if (node.isInt()) {
        return node.asInt();
      } else if (node.isLong()) {
        return node.asLong();
      } else if (node.isTextual()) {
        return node.asText();
      } else if (node.isArray() && node.size() == 0) {
        // An empty array token indicates a wildcard.
        return WildcardSingleton.INSTANCE;
      } else if (node.isNull()) {
        return null;
      } else {
        throw new IOException(String.format(
            "Invalid JSON value: '%s', expecting string, int, long, null, or wildcard [].", node));
      }
    }
  }

  /**
   * Get potentially fresh row.
   *
   * @param table to query from.
   * @param eid of the row to query.
   * @param request for data.
   * @param freshen is true iff we prefer to freshen.
   * @param timeout at which the freshener returns preexisting data.
   * @return row data.
   * @throws IOException in case the data can not be fetched.
   */
  private KijiRowData getKijiRowData(
      final KijiTable table,
      final EntityId eid,
      final KijiDataRequest request,
      final boolean freshen,
      final long timeout) throws IOException {
    KijiRowData rowData;
    // TODO: add FreshRequestOptions to disable freshening and simplify below - WDSCORE-75
    // Give priority to request freshness parameter; if not set use default
    if (freshen) {
      // Do freshening
      FreshKijiTableReader reader = mKijiClient.getFreshKijiTableReader(
          table.getURI().getInstance(),
          table.getURI().getTable());
      FreshKijiTableReader.FreshRequestOptions freshOpts =
          FreshKijiTableReader.FreshRequestOptions.Builder.create()
          .withTimeout(timeout)
          .build();
      rowData = reader.get(eid, request, freshOpts);
    } else {
      // Don't freshen
      rowData = RowResourceUtil.getKijiRowData(table, eid, request);
    }
    return rowData;
  }

  /**
   * Commits a KijiRestRow representation to the kiji table: performs create and update.
   * Note that the user-formatted entityId is required.
   * Also note that writer schema is not considered as of the latest version.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param kijiRestRow POST-ed json data
   * @return a message containing the rowkey of interest
   * @throws IOException when post fails
   */
  private Map<String, String> postRow(final String instance,
      final String table,
      final KijiRestRow kijiRestRow)
      throws IOException {
    final KijiTable kijiTable = mKijiClient.getKijiTable(instance, table);

    final EntityId entityId;
    if (null != kijiRestRow.getEntityId()) {
      entityId = ToolUtils.createEntityIdFromUserInputs(kijiRestRow.getEntityId(),
          kijiTable.getLayout());
    } else {
      throw new WebApplicationException(
          new IllegalArgumentException("EntityId was not specified."), Status.BAD_REQUEST);
    }

    // Open writer and write.
    RowResourceUtil.writeRow(kijiTable, entityId, kijiRestRow,
        mKijiClient.getKijiSchemaTable(instance));

    // Better output?
    Map<String, String> returnedTarget = Maps.newHashMap();

    URI targetResource = UriBuilder.fromResource(RowResource.class).build(instance, table,
        new String(Hex.encodeHex(entityId.getHBaseRowKey())));

    returnedTarget.put("target", targetResource.toString());

    return returnedTarget;

  }

  /**
   * POSTs JSON body to row(s): performs create and update.
   * The input JSON blob can either represent a single KijiRestRow or a list of KijiRestRows.
   *
   * For example, a single KijiRestRow:
   * {
   *   "entityId":"hbase=hex:8c2d2fcc2c150efb49ce0817e1823d46",
   *   "cells":{
   *       "info":{
   *          "firstname":[
   *             {
   *                "timestamp":123,
   *                "value":"John"
   *             }
   *          ]
   *       },
   *       "info":{
   *          "lastname":[
   *             {
   *                "timestamp":123,
   *                "value":"Smith"
   *             }
   *          ]
   *       }
   *    }
   * }
   *
   * A list of KijiRestRows:
   * [
   *    {
   *       "entityId":"hbase=hex:8c2d2fcc2c150efb49ce0817e1823d46",
   *       "cells":{
   *          "info":{
   *             "firstname":[
   *                {
   *                   "timestamp":123,
   *                   "value":"John"
   *                }
   *             ]
   *          }
   *       }
   *    },
   *    {
   *       "entityId":"hbase=hex:acfbe1234567890987654321abcfdega",
   *       "cells":{
   *          "info":{
   *             "firstname":[
   *                {
   *                   "timestamp":12312345,
   *                   "value":"Jane"
   *                }
   *             ]
   *          }
   *       }
   *    }
   * ]
   *
   * Note that the user-formatted entityId is required.
   * Also note that writer schema is not considered as of the latest version.
   *
   * @param instance in which the table resides
   * @param table in which the row resides
   * @param kijiRestRows POST-ed json data
   * @return a message containing the rowkey of interest
   * @throws IOException when post fails
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiStability.Experimental
  public Map<String, List<String>> postRows(@PathParam(INSTANCE_PARAMETER) final String instance,
      @PathParam(TABLE_PARAMETER) final String table,
      final JsonNode kijiRestRows)
      throws IOException {
    // We intend to return a JSON blob listing the row keys we are putting to.
    // i.e. {targets : [..., ..., ...]}
    final List<String> results = Lists.newLinkedList();

    final Iterator<JsonNode> rowIterator;
    if (kijiRestRows.isArray()) {
      rowIterator = kijiRestRows.elements();
    } else {
      rowIterator = Iterators.singletonIterator(kijiRestRows);
    }

    // Put each row.
    while (rowIterator.hasNext()) {
      final KijiRestRow kijiRestRow = mJsonObjectMapper
          .treeToValue(rowIterator.next(), KijiRestRow.class);
      final Map<String, String> result = postRow(instance, table, kijiRestRow);
      results.add(result.get("target"));
    }

    final Map<String, List<String>> returnedResults = Maps.newHashMap();
    returnedResults.put("targets", results);
    return returnedResults;
  }
}
