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

package org.kiji.rest.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.rest.representations.KijiRestCell;
import org.kiji.rest.representations.KijiRestEntityId;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.representations.SchemaOption;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.SchemaClassNotFoundException;
import org.kiji.schema.util.ResourceUtils;

/**
 * Utility methods used for reading and writing REST row model objects to/from Kiji.
 */
@ApiAudience.Framework
public final class RowResourceUtil {

  private static final ObjectMapper BASIC_MAPPER = new ObjectMapper();

  /**
   * Blank constructor.
   */
  private RowResourceUtil() {
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
  public static long[] getTimestamps(String timeRange) {

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
  public static List<KijiColumnName> addColumnDefs(KijiTableLayout tableLayout,
      ColumnsDef columnsDef, String requestedColumns) {

    List<KijiColumnName> returnCols = Lists.newArrayList();
    Collection<KijiColumnName> requestedColumnList = null;
    // Check for whether or not *all* columns were requested
    if (requestedColumns == null || requestedColumns.trim().equals("*")) {
      requestedColumnList = tableLayout.getColumnNames();
    } else {
      requestedColumnList = Lists.newArrayList();
      String[] requestedColumnArray = requestedColumns.split(",");
      for (String s : requestedColumnArray) {
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
   * @param schemaTable is the handle to the schema table used to resolve the writer's schema into
   *        the Kiji specific UID.
   * @return The Kiji row data POJO to be sent to the client
   * @throws IOException when trying to request the specs of a column family that doesn't exist.
   *         Although this shouldn't happen as columns are assumed to have been validated before
   *         this method is invoked.
   */
  public static KijiRestRow getKijiRestRow(KijiRowData rowData, KijiTableLayout tableLayout,
      List<KijiColumnName> columnsRequested, KijiSchemaTable schemaTable) throws IOException {
    // The entityId is materialized based on the row key format.
    KijiRestRow returnRow = new KijiRestRow(
        KijiRestEntityId.create(rowData.getEntityId(), tableLayout));
    Map<String, FamilyLayout> familyLayoutMap = tableLayout.getFamilyMap();

    // Let's sort this to keep the response consistent with what hbase would return.
    Collections.sort(columnsRequested);

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
        DecodedCell<String> decodedCell = new DecodedCell<String>(null, "Error loading cell: "
            + e.getMessage());
        String qualifier = "";
        if (col.getQualifier() != null) {
          qualifier = col.getQualifier();
        }
        // Error condition.
        SchemaOption schemaOption = new SchemaOption(-1);
        returnRow.addCell(new KijiCell<String>(col.getFamily(), qualifier, -1L, decodedCell),
            schemaOption);
        continue;
      }
      if (spec.isCounter()) {
        if (col.isFullyQualified()) {
          KijiCell<Long> counter = rowData.getMostRecentCell(col.getFamily(), col.getQualifier());
          if (null != counter) {
            SchemaOption schemaOption = new SchemaOption(schemaTable.
                getOrCreateSchemaId(Schema.create(Schema.Type.LONG)));
            returnRow.addCell(counter, schemaOption);
          }
        } else if (familyInfo.isMapType()) {
          // Only can print all qualifiers on map types
          for (String key : rowData.getQualifiers(col.getFamily())) {
            KijiCell<Long> counter = rowData.getMostRecentCell(col.getFamily(), key);
            if (null != counter) {
              SchemaOption schemaOption = new SchemaOption(schemaTable.
                  getOrCreateSchemaId(counter.getWriterSchema()));
              returnRow.addCell(counter, schemaOption);
            }
          }
        }
      } else {

        if (col.isFullyQualified()) {
          Map<Long, KijiCell<Object>> rowVals = rowData.getCells(col.getFamily(),
              col.getQualifier());
          for (Entry<Long, KijiCell<Object>> timestampedCell : rowVals.entrySet()) {
            KijiCell<Object> kijiCell = timestampedCell.getValue();
            SchemaOption schemaOption = new SchemaOption(schemaTable.
                getOrCreateSchemaId(kijiCell.getWriterSchema()));
            returnRow.addCell(kijiCell, schemaOption);
          }
        } else if (familyInfo.isMapType()) {
          Map<String, NavigableMap<Long, KijiCell<Object>>> rowVals = rowData.getCells(col
              .getFamily());

          for (Entry<String, NavigableMap<Long, KijiCell<Object>>> e : rowVals.entrySet()) {
            for (KijiCell<Object> timestampedCell : e.getValue().values()) {
              SchemaOption schemaOption = new SchemaOption(schemaTable.
                  getOrCreateSchemaId(timestampedCell.getWriterSchema()));
              returnRow.addCell(timestampedCell, schemaOption);
            }
          }
        }
      }
    }
    return returnRow;
  }

  /**
   * Returns a Kiji row object given the table, entity_id and data request.
   *
   * @param table is the table containing the row.
   * @param eid is the entity id of the row to return.
   * @param request contains information about what to return.
   * @return a Kiji row object conforming to the parameters of the request.
   *
   * @throws IOException if the retrieve fails.
   */
  public static KijiRowData getKijiRowData(KijiTable table, EntityId eid,
      KijiDataRequest request) throws IOException {

    KijiRowData returnRow = null;
    final KijiTableReader reader = table.openTableReader();
    try {
      returnRow = reader.get(eid, request);

    } finally {
      reader.close();
    }

    return returnRow;
  }

  /**
   * A helper method to perform counter puts.
   *
   * @param writer is the table writer which will do the putting.
   * @param entityId is the entityId of the row to put to.
   * @param valueString is the value to put; should be convertible to long.
   * @param column is the column to put the cell to.
   * @param timestamp is the timestamp to put the cell at (default is cluster-side UNIX time).
   * @throws IOException if the put fails.
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
    // String types are a bit annoying in that those need double escaping of the quotations
    // which are impractical for clients and don't match the semantics of what GET returns. This
    // is because the JSON decoder requires escaped strings to be properly parsed into JSON.
    Object datum = jsonValue;
    if (schema.getType() != Type.STRING) {
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      datum = reader.read(null, new DecoderFactory().jsonDecoder(schema, jsonValue));
    }
    // Write the put.
    writer.put(entityId, column.getFamily(), column.getQualifier(), timestamp, datum);
  }

  /**
   * Util method to write a rest row into Kiji.
   *
   * @param kijiTable is the table to write into.
   * @param entityId is the entity id of the row to write.
   * @param kijiRestRow is the row model to write to Kiji.
   * @param schemaTable is the handle to the schema table used to resolve the KijiRestCell's
   *        writer schema if it was specified as a UID.
   * @throws IOException if there a failure writing the row.
   */
  public static void writeRow(KijiTable kijiTable, EntityId entityId,
      KijiRestRow kijiRestRow, KijiSchemaTable schemaTable) throws IOException {
    final KijiTableWriter writer = kijiTable.openTableWriter();
    // Default global timestamp.
    long globalTimestamp = System.currentTimeMillis();

    try {
      for (Entry<String, NavigableMap<String, List<KijiRestCell>>> familyEntry : kijiRestRow
          .getCells().entrySet()) {
        String columnFamily = familyEntry.getKey();
        NavigableMap<String, List<KijiRestCell>> qualifiedCells = familyEntry.getValue();
        for (Entry<String, List<KijiRestCell>> qualifiedCell : qualifiedCells.entrySet()) {
          final KijiColumnName column = new KijiColumnName(columnFamily, qualifiedCell.getKey());
          if (!kijiTable.getLayout().exists(column)) {
            throw new WebApplicationException(new IllegalArgumentException(
                "Specified column does not exist: " + column), Response.Status.BAD_REQUEST);
          }

          for (KijiRestCell restCell : qualifiedCell.getValue()) {
            final long timestamp;
            if (null != restCell.getTimestamp()) {
              timestamp = restCell.getTimestamp();
            } else {
              timestamp = globalTimestamp;
            }
            if (timestamp >= 0) {
              // Put to either a counter or a regular cell.
              if (SchemaType.COUNTER == kijiTable.getLayout().getCellSchema(column).getType()) {
                // Write the counter cell.
                putCounterCell(writer, entityId, restCell.getValue().toString(), column, timestamp);
              } else {
                // Write the cell.
                String jsonValue = restCell.getValue().toString();
                // TODO: This is ugly. Converting from Map to JSON to String.
                if (restCell.getValue() instanceof Map<?, ?>) {
                  JsonNode node = BASIC_MAPPER.valueToTree(restCell.getValue());
                  jsonValue = node.toString();
                }
                Schema actualWriter = restCell.getWriterSchema(schemaTable);
                if (actualWriter == null) {
                  throw new IOException("Unrecognized schema " + restCell.getValue());
                }
                putCell(writer, entityId, jsonValue, column, timestamp, actualWriter);
              }
            }
          }
        }
      }
    } finally {
      ResourceUtils.closeOrLog(writer);
      ResourceUtils.releaseOrLog(kijiTable);
    }

  }
}
