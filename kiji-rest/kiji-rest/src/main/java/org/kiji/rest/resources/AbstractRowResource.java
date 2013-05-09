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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;

import org.kiji.rest.representations.KijiRestCell;
import org.kiji.rest.representations.KijiRestRow;
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
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.SchemaClassNotFoundException;

/**
 * Base class with helper methods for accessing KijiRow resources.
 */
public class AbstractRowResource extends AbstractKijiResource {

  /**
   * Default constructor.
   *
   * @param cluster KijiURI in which these instances are contained.
   * @param instances The list of accessible instances.
   */
  public AbstractRowResource(KijiURI cluster, Set<KijiURI> instances) {
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
  protected final long[] getTimestamps(String timeRange) {

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
  protected final List<KijiColumnName> addColumnDefs(KijiTableLayout tableLayout,
      ColumnsDef columnsDef, String requestedColumns) {

    List<KijiColumnName> returnCols = Lists.newArrayList();
    Collection<KijiColumnName> requestedColumnList = null;
    // Check for whether or not *all* columns were requested
    if (requestedColumns == null || requestedColumns.trim().equals("*")) {
      requestedColumnList = tableLayout.getColumnNames();
    } else {
      requestedColumnList = Lists.newArrayList();
      String[] pColumns = requestedColumns.split(",");
      for (String s : pColumns) {
        requestedColumnList.add(new KijiColumnName(s));
      }
    }

    Map<String, FamilyLayout> colMap = tableLayout.getFamilyMap();
    // Loop over the columns requested and validate that they exist and/or
    // expand qualifiers
    // in case only family names were specified (in the case of group type
    // families).
    for (KijiColumnName kijiColumn : requestedColumnList) {
      FamilyLayout layout = colMap.get(kijiColumn.getFamily());
      if (null != layout) {
        if (layout.isMapType()) {
          columnsDef.add(kijiColumn);
          returnCols.add(kijiColumn);
        } else {
          Map<String, ColumnLayout> groupColMap = layout.getColumnMap();
          if (kijiColumn.isFullyQualified()) {
            ColumnLayout groupColLayout = groupColMap.get(kijiColumn.getQualifier());
            if (null != groupColLayout) {
              columnsDef.add(kijiColumn);
              returnCols.add(kijiColumn);
            }
          } else {
            for (ColumnLayout c : groupColMap.values()) {
              KijiColumnName fullyQualifiedGroupCol = new KijiColumnName(kijiColumn.getFamily(),
                  c.getName());
              columnsDef.add(fullyQualifiedGroupCol);
              returnCols.add(fullyQualifiedGroupCol);
            }
          }
        }
      }
    }

    if (returnCols.isEmpty()) {
      throw new WebApplicationException(new IllegalArgumentException("No columns selected!"),
          Status.BAD_REQUEST);
    }

    return returnCols;
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
  protected final KijiRestRow getKijiRow(KijiRowData rowData, KijiTableLayout tableLayout,
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

          for (Entry<String, NavigableMap<Long, KijiCell<Object>>> e : rowVals.entrySet()) {
            for (KijiCell<Object> timestampedCell : e.getValue().values()) {
              returnRow.addCell(new KijiRestCell(timestampedCell));
            }
          }
        }
      }
    }
    return returnRow;
  }

  /**
   * Returns a KijiRestRow from a KijiTable.
   *
   * @param table is the table containing the row.
   * @param hbaseRowKey is the hbase rowkey of the row to return.
   * @param timeRange the timerange within which the cells should be
   * @param columns the requested column (+ qualifier) to return
   * @param maxVersions the maximum number of cells per column:qualifier to return
   * @return the KijiRestRow representing the row requested.
   */
  protected final KijiRestRow getKijiRow(KijiTable table, byte[] hbaseRowKey, long[] timeRange,
      String columns, int maxVersions) {

    KijiRestRow returnRow = null;
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
        if (timeRange != null) {
          dataBuilder.withTimeRange(timeRange[0], timeRange[1]);
        }

        ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(maxVersions);
        List<KijiColumnName> lRequestedColumns = addColumnDefs(table.getLayout(), colsRequested,
            columns);

        KijiDataRequest dataRequest = dataBuilder.build();
        EntityIdFactory eidFactory = EntityIdFactory.getFactory(table.getLayout());
        EntityId entityId = eidFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);
        KijiRowData row = reader.get(entityId, dataRequest);
        returnRow = getKijiRow(row, table.getLayout(), lRequestedColumns);

      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
    }
    return returnRow;
  }

  /**
   * A helper method to perform counter puts.
   *
   * @param writer The table writer which will do the putting.
   * @param entityId The entityId of the row to put to.
   * @param valueString The value to put; should be convertible to long.
   * @param column The column to put the cell to.
   * @param timestamp The timestamp to put the cell at (default is cluster-side UNIX time).
   * @throws IOException When the put fails.
   */
  public static void putCounterCell(
      final KijiTableWriter writer,
      final EntityId entityId,
      final String valueString,
      final KijiColumnName column,
      final long timestamp)
      throws IOException {
    try {
      long value = Long.parseLong(valueString);
      writer.put(entityId, column.getFamily(), column.getQualifier(), timestamp, value);
    } catch (NumberFormatException nfe) {
      // TODO Make this a more informative exception.
      // Could not parse parameter to a long.
      throw new WebApplicationException(nfe, Response.Status.BAD_REQUEST);
    }
  }

  /**
   * A helper method to perform individual cell puts.
   *
   * @param writer The table writer which will do the putting.
   * @param entityId The entityId of the row to put to.
   * @param jsonValue The json value to put.
   * @param column The column to put the cell to.
   * @param timestamp The timestamp to put the cell at (default is cluster-side UNIX time).
   * @param schema The schema of the cell (default is specified in layout.).
   * @throws IOException When the put fails.
   */
  public static void putCell(
      final KijiTableWriter writer,
      final EntityId entityId,
      final String jsonValue,
      final KijiColumnName column,
      final long timestamp,
      final Schema schema)
      throws IOException {
    Preconditions.checkNotNull(schema);
    // Create the Avro record to write.
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Object datum = reader.read(null, new DecoderFactory().jsonDecoder(schema, jsonValue));

    // Write the put.
    writer.put(entityId, column.getFamily(), column.getQualifier(), timestamp, datum);
  }
}
