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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.google.common.collect.Lists;
import com.yammer.metrics.annotation.Timed;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import org.kiji.rest.core.KijiRestCell;
import org.kiji.rest.core.KijiRestRow;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.SchemaClassNotFoundException;

/**
 * This REST resource interacts with a single Kiji row identified by its hbase rowkey (in hex).
 *
 * This resource is served for requests using the resource identifiers: <li>
 * GET /v1/instances/&lt;instance&gt/tables/&lt;table&gt/rows/&lt;hex_row_key&gt; PUT
 * /v1/instances/&lt;instance&gt/tables/&lt;table&gt/rows/&lt;hex_row_key&gt;
 */
@Path(ROW_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class RowResource extends AbstractKijiResource {

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
   * Retrieves the Min..Max timestamp given the user specified time range. Min and Max represent
   * long-type time in milliseconds since the UNIX Epoch. e.g. '123..1234', '0..', or '..1234'.
   * (Default=0..)
   *
   * @param timeRange is the user supplied timerange.
   *
   * @return A long 2-tuple containing the min and max timestamps (in ms since UNIX Epoch)
   */
  private long[] getTimestamps(String timeRange) {
    long[] lReturn = new long[] { 0, Long.MAX_VALUE };
    final Pattern timestampPattern = Pattern.compile("([0-9]*)\\.\\.([0-9]*)");
    final Matcher timestampMatcher = timestampPattern.matcher(timeRange);

    if (timestampMatcher.matches()) {
      final String leftEndpoint = timestampMatcher.group(1);
      lReturn[0] = ("".equals(leftEndpoint)) ? 0 : Long.parseLong(leftEndpoint);

      final String rightEndpoint = timestampMatcher.group(2);
      lReturn[1] = ("".equals(rightEndpoint)) ? Long.MAX_VALUE : Long.parseLong(rightEndpoint);
    }
    return lReturn;
  }

  /**
   * Returns a list of fully qualified KijiColumnNames to return to the client.
   *
   * @param tableLayout is the layout of the table from which the row is being fetched.
   *
   * @param columnsDef is the columns definition object being modified to be passed down to the
   *        KijiTableReader.
   * @param requestedColumns the list of user requested columns to display.
   * @return the list of KijiColumns that will ultimately be displayed. Since this method validates
   *         the list of incoming columns, it's not necessarily the case that what was specified in
   *         the requestedColumns string correspond exactly to the list of outgoing columns. In some
   *         cases it could be less (in case of an invalid column/qualifier) or more in case of
   *         specifying only the family but no qualifiers.
   */
  private List<KijiColumnName> addColumnDefs(KijiTableLayout tableLayout, ColumnsDef columnsDef,
      String requestedColumns) {
    List<KijiColumnName> lReturnCols = Lists.newArrayList();
    Collection<KijiColumnName> lColumnsRequested = null;
    // Check for whether or not *all* columns were requested
    if (requestedColumns == null || requestedColumns.trim().equals("*")) {
      lColumnsRequested = tableLayout.getColumnNames();
    } else {
      lColumnsRequested = Lists.newArrayList();
      String[] pColumns = requestedColumns.split(",");
      for (String s : pColumns) {
        lColumnsRequested.add(new KijiColumnName(s));
      }
    }

    Map<String, FamilyLayout> colMap = tableLayout.getFamilyMap();
    // Loop over the columns requested and validate that they exist and/or
    // expand qualifiers
    // in case only family names were specified (in the case of group type
    // families).
    for (KijiColumnName kijiColumn : lColumnsRequested) {
      FamilyLayout layout = colMap.get(kijiColumn.getFamily());
      if (null != layout) {
        if (layout.isMapType()) {
          columnsDef.add(kijiColumn);
          lReturnCols.add(kijiColumn);
        } else {
          Map<String, ColumnLayout> groupColMap = layout.getColumnMap();
          if (kijiColumn.isFullyQualified()) {
            ColumnLayout groupColLayout = groupColMap.get(kijiColumn.getQualifier());
            if (null != groupColLayout) {
              columnsDef.add(kijiColumn);
              lReturnCols.add(kijiColumn);
            }
          } else {
            for (ColumnLayout c : groupColMap.values()) {
              KijiColumnName fullyQualifiedGroupCol = new KijiColumnName(kijiColumn.getFamily(),
                  c.getName());
              columnsDef.add(fullyQualifiedGroupCol);
              lReturnCols.add(fullyQualifiedGroupCol);
            }
          }
        }
      }
    }
    return lReturnCols;
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
  @PUT
  @Timed
  public String putRowByHexEntityId(@PathParam(INSTANCE_PARAMETER) String instance,
      @PathParam(TABLE_PARAMETER) String table,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId, @Context UriInfo uriInfo)
      throws IOException {
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
   * @return a single KijiRestRow (which is the POJO that is serialized into JSON to the client).
   */
  @GET
  @Timed
  public KijiRestRow getRow(@PathParam(INSTANCE_PARAMETER) String instanceId,
      @PathParam(TABLE_PARAMETER) String tableId,
      @PathParam(HEX_ENTITY_ID_PARAMETER) String hexEntityId,
      @QueryParam("cols") @DefaultValue("*") String columns,
      @QueryParam("versions") @DefaultValue("1") int maxVersions,
      @QueryParam("timerange") String timeRange) {

    KijiRestRow returnRow = null;
    byte[] hbaseRowKey = null;

    try {
      hbaseRowKey = Hex.decodeHex(hexEntityId.toCharArray());
    } catch (DecoderException e1) {
      throw new WebApplicationException(e1, Status.BAD_REQUEST);
    }

    try {
      final KijiTable table = super.getKijiTable(instanceId, tableId);
      try {
        final KijiTableReader reader = table.openTableReader();
        try {
          KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
          if (timeRange != null) {
            long[] lTimeRange = getTimestamps(timeRange);
            dataBuilder.withTimeRange(lTimeRange[0], lTimeRange[1]);
          }

          ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
          List<KijiColumnName> lRequestedColumns = addColumnDefs(table.getLayout(), colsRequested,
              columns);
          if (lRequestedColumns.isEmpty()) {
            throw new WebApplicationException(Status.BAD_REQUEST);
          }

          final KijiDataRequest dataRequest = dataBuilder.build();
          EntityIdFactory eidFactory = EntityIdFactory.getFactory(table.getLayout());
          final EntityId entityId = eidFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);
          final KijiRowData rowData = reader.get(entityId, dataRequest);
          returnRow = getKijiRow(rowData, table.getLayout(), lRequestedColumns);
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }

    return returnRow;
  }

  /**
   * Reads the KijiRowData retrieved and returns the POJO representing the result sent to the
   * client.
   *
   * @param rowData is the actual row data fetched from Kiji
   * @param tableLayout the layout of the underlying Kiji table itself.
   * @param columnsRequested is the list of columns requested by the client
   * @return The Kiji row data POJO to be sent to the client
   * @throws IOException when trying to request the specs of a column family that doesn't exist.
   *         Although this shouldn't happen as columns are assumed to have been validated before
   *         this method is invoked.
   */
  private KijiRestRow getKijiRow(KijiRowData rowData, KijiTableLayout tableLayout,
      List<KijiColumnName> columnsRequested) throws IOException {
    KijiRestRow returnRow = new KijiRestRow(rowData.getEntityId());
    Map<String, FamilyLayout> familyLayoutMap = tableLayout.getFamilyMap();

    for (KijiColumnName col : columnsRequested) {
      FamilyLayout familyInfo = familyLayoutMap.get(col.getFamily());
      CellSpec spec = null;
      try {
        spec = tableLayout.getCellSpec(col);
      } catch (SchemaClassNotFoundException e) {
        // If the user is requesting a column whose class is not on the
        // classpath, then we
        // will get an exception here. Until we migrate to KijiSchema 1.1.0 and
        // use the generic
        // Avro API, we will have to require clients to load the rest server
        // with compiled
        // Avro schemas on the classpath.
        continue;
      }
      if (spec.isCounter()) {
        if (col.isFullyQualified()) {
          KijiCell<Long> counter = rowData.getMostRecentCell(col.getFamily(), col.getQualifier());
          if (null != counter) {
            returnRow.addCell(new KijiRestCell(counter));
          }
        } else if (familyInfo.isMapType()) {
          // Only can print all qualifiers on map types
          for (String key : rowData.getQualifiers(col.getFamily())) {
            KijiCell<Long> counter = rowData.getMostRecentCell(col.getFamily(), key);
            if (null != counter) {
              returnRow.addCell(new KijiRestCell(counter));
            }
          }
        }
      } else {
        if (col.isFullyQualified()) {
          Map<Long, KijiCell<Object>> rowVals = rowData.getCells(col.getFamily(),
              col.getQualifier());
          for (Entry<Long, KijiCell<Object>> timestampedCell : rowVals.entrySet()) {
            returnRow.addCell(new KijiRestCell(timestampedCell.getValue()));
          }
        } else if (familyInfo.isMapType()) {
          Map<String, NavigableMap<Long, KijiCell<Object>>> rowVals = rowData.getCells(col
              .getFamily());

          for (String key : rowVals.keySet()) {
            for (Entry<Long, KijiCell<Object>> timestampedCell : rowVals.get(key).entrySet()) {
              returnRow.addCell(new KijiRestCell(timestampedCell.getValue()));
            }
          }
        }
      }
    }
    return returnRow;
  }
}
