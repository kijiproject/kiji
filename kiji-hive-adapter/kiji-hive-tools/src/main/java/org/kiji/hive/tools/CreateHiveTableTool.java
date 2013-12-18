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

package org.kiji.hive.tools;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.util.ResourceUtils;

/**
 * CLI tool that reads the layout of a Kiji table and outputs a CREATE EXTERNAL TABLE statement
 * that is usable as a starting point for Hive.
 */
public class CreateHiveTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(CreateHiveTableTool.class);

  /** Limit on how much a recursive Avro record can be before returning tbe base string. */
  private static final Integer DEFAULT_MAX_LAYOUT_DEPTH = 2;

  @Flag(name = "kiji", usage = "URI of the Kiji table to generate a Hive DDL statement for.")
  private String mKijiURIFlag = null;
  private KijiURI mTableURI = null;

  @Flag(name="maxLayoutDepth",
  usage = "Limit on the number of instances of a particular Avro record when unrolling to Hive.")
  private static Integer mMaxLayoutDepth = DEFAULT_MAX_LAYOUT_DEPTH;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    mTableURI = KijiURI.newBuilder(mKijiURIFlag).build();
  }

  @Override
  public String getName() {
    return "generate-hive-table";
  }

  @Override
  public String getDescription() {
    return "Generates CREATE EXTERNAL TABLE statement for Hive";
  }

  @Override
  public String getCategory() {
    return "Hive";
  }

  /**
   * Given a KijiColumnName, return a sensible Hive column name.  For a fully qualified column,
   * return the qualifier.  Otherwise if it's a map type family, return the family name.
   *
   * @param kijiColumnName to generate the Hive column name for.
   * @return default Hive column name corresponding to the Kiji column.
   */
  protected static String getDefaultHiveColumnName(KijiColumnName kijiColumnName) {
    if (kijiColumnName.isFullyQualified()) {
      return kijiColumnName.getQualifier();
    }
    return kijiColumnName.getFamily();
  }

  /**
   * Builds a String representing a equivalent Hive type for an Avro schema.
   *
   * @param schema to be converted.
   * @return String representing the equivalent Hive type.
   */
  protected static String convertSchemaToHiveType(Schema schema) {
    Map<String, Integer> recordCount = Maps.newHashMap();
    StringBuilder hiveStringBuilder = wrapSB("STRUCT<ts: TIMESTAMP, value: ",
        convertSchemaToHiveTypeSB(schema, recordCount),
        ">"
    );
    return hiveStringBuilder.toString();
  }

  /**
   * Builds the Hive type from the schema for a particular cell within Hive.
   *
   * @param kijiColumnName  that specifices which column to build the Hive type from.
   * @param kijiTableLayout where the column resides.
   * @param schemaTable of the Kiji instance for type mappings
   * @return String representing the corresponding Hive type for a cell within the table's layout.
   * @throws IOException if there is an issue retrieving the schema of a particular column.
   */
  protected static String getHiveType(KijiColumnName kijiColumnName,
                                      KijiTableLayout kijiTableLayout,
                                      KijiSchemaTable schemaTable)
      throws IOException {
    CellSchema cellSchema = kijiTableLayout.getCellSchema(kijiColumnName);
    Schema schema = getSchemaFromCellSchema(cellSchema, schemaTable);

    String hiveType = "";
    if (null != schema) {
      hiveType = convertSchemaToHiveType(schema);
      if (!kijiColumnName.isFullyQualified()) {
        hiveType = "MAP<STRING, " + hiveType + ">";
      }
    } else {
      // Null schemas are probably indicative of a counter, but they are currently unsupported
      // within the Kiji Hive Adapter.  Log a warning, and let the user fix it.
      LOG.warn(kijiColumnName.toString() + " has a null schema and is unsupported within Hive.");
    }
    return hiveType;
  }

  /**
   * Retrieves the relevant Schema from a CellSchema by either parsing it from the relevant String,
   * or retrieving it from the passed in SchemaTable.
   *
   * @param cellSchema that defines the desired Schema.
   * @param schemaTable of the Kiji instance to look up this schema for.
   * @return Schema referenced by the CellSchema.
   * @throws IOException if there is an issue with retrieving the schema of a particular column.
   */
  private static Schema getSchemaFromCellSchema(CellSchema cellSchema, KijiSchemaTable schemaTable)
      throws IOException {
    Schema.Parser parser = new Schema.Parser();
    switch (cellSchema.getType()) {
      case INLINE:
        return parser.parse(cellSchema.getValue());
      case AVRO:
        AvroSchema avroSchema = cellSchema.getDefaultReader();
        if (avroSchema.getUid() != null) {
          return schemaTable.getSchema(avroSchema.getUid());
        } else if (avroSchema.getJson() != null) {
          return parser.parse(avroSchema.getJson());
        }
        throw new IOException("Unable to find Schema for AVRO CellSchema.");
      case CLASS:
        String className = cellSchema.getValue();
        try {
          SpecificRecord clazz = (SpecificRecord) Class.forName(className).newInstance();
          return clazz.getSchema();
        } catch (Exception e) {
          throw new IOException("Unable to find/instantiate class: " + className, e);
        }
      case COUNTER:
      case RAW_BYTES:
      default:
        throw new UnsupportedOperationException(
            "CellSchema " + cellSchema.getType() + " unsupported.");
    }
  }

  /**
   * Retrieves a list of KijiColumnNames from a KijiTableLayout including both fully qualified
   * columns as well as map-type families.
   *
   * @param kijiTableLayout to retrieve all of the KijiColumnNames from.
   * @return collection of KijiColumnNames in the specified Layout.
   */
  private static Collection<KijiColumnName> getKijiColumns(KijiTableLayout kijiTableLayout) {
    // We need to do this because getColumnNames doesn't seem to get all of the columns names;
    List<KijiColumnName> kijiColumnNames = Lists.newArrayList();
    for (FamilyLayout family : kijiTableLayout.getFamilies()) {
      String familyName = family.getName();
      if (family.isMapType()) {
        // Map type column family
        KijiColumnName kijiColumnName = new KijiColumnName(familyName);
        kijiColumnNames.add(kijiColumnName);
      } else {
        for (FamilyLayout.ColumnLayout column : family.getColumns()) {
          String columnName = column.getName();
          KijiColumnName kijiColumnName = new KijiColumnName(familyName, columnName);
          kijiColumnNames.add(kijiColumnName);
        }
      }
    }
    return kijiColumnNames;
  }

  /**
   * Helper method to wrap the recursive outputs of other StringBuilders with the more logical
   * semantics.
   *
   * @param prependStr The String to prepend onto the StringBuilder.
   * @param base The base StringBuilder to build the results from.
   * @param appendStr The String to append to the StringBuilder.
   * @return the base StringBuilder with the strings prepended and appeanded.
   */
  private static StringBuilder wrapSB(String prependStr, StringBuilder base, String appendStr) {
    return base.insert(0, prependStr).append(appendStr);
  }

  /**
   * Recursive helper method that does the conversion of an Avro Schema to a Hive type.  Keeps
   * track of record types that are seen and enforces a limit on the number of levels of recursion
   * for self-referential types.
   *
   * @param schema to be converted.
   * @param recordCount internal map representing the names of the records that have been seen in
   *                    this level of recursion.
   * @return StringBuilder representing the equivalent Hive type.
   */
  private static StringBuilder convertSchemaToHiveTypeSB(Schema schema, Map
      <String, Integer> recordCount) {
    switch (schema.getType()) {
      case RECORD:
        // Since recordCount is basically a shared map across levels of nested data structures,
        // increment and decrement the values inside of this to restore the previous state.
        int oldCount;
        if (recordCount.containsKey(schema.getFullName())) {
          oldCount = recordCount.get(schema.getFullName());
        } else {
          oldCount = 0;
        }

        // Only allow for mMaxLayoutDepth levels of recursions in any Avro type for Hive.
        // If we go over, return the rest of this as a String,
        if (oldCount >= mMaxLayoutDepth) {
          return new StringBuilder("STRING");
        }

        recordCount.put(schema.getFullName(), oldCount + 1);
        StringBuilder sb = new StringBuilder("STRUCT<");
        for (Schema.Field field: schema.getFields()) {
          if (field.pos() != 0) {
            sb.append(", ");
          }
          sb.append("`"); // We wrap all field names with backticks to avoid Hive reserved words
          sb.append(field.name());
          sb.append("`: ");
          sb.append(convertSchemaToHiveTypeSB(field.schema(), recordCount));
        }
        sb.append(">");

        // Restore the old state of recordCount
        if (oldCount == 0) {
          recordCount.remove(schema.getFullName());
        } else {
          recordCount.put(schema.getFullName(), oldCount);
        }
        return sb;
      case ENUM:
        // ENUMs are treated as Strings in Hive.
        return new StringBuilder(Schema.Type.STRING.toString());
      case ARRAY:
        return wrapSB("ARRAY<",
            convertSchemaToHiveTypeSB(schema.getElementType(), recordCount),
            ">");
      case MAP:
        return wrapSB("MAP<STRING, ",
            convertSchemaToHiveTypeSB(schema.getValueType(), recordCount),
            ">");
      case UNION:
        // If the UNION is of a Schema and null return the result for that Schema
        if (schema.getTypes().size() == 2) {
          Schema firstSchema = schema.getTypes().get(0);
          Schema secondSchema = schema.getTypes().get(1);
          if (firstSchema.getType() == Schema.Type.NULL) {
            return convertSchemaToHiveTypeSB(secondSchema, recordCount);
          } else if (secondSchema.getType() == Schema.Type.NULL) {
            return convertSchemaToHiveTypeSB(firstSchema, recordCount);
          }
        }

        // If this is a union type of not just null items.
        StringBuilder unionSB = new StringBuilder("UNIONTYPE<");
        boolean first = true;
        for (Schema unionType : schema.getTypes()) {
          if (first) {
            first = false;
          } else {
            unionSB.append(", ");
          }
          unionSB.append(convertSchemaToHiveTypeSB(unionType, recordCount));
        }
        unionSB.append(">");
        break;
      case FIXED:
      case BYTES:
        // Avro FIXED and BYTES types both map to the BINARY Hive type.
        return new StringBuilder("BINARY");
      case LONG:
        return new StringBuilder("BIGINT");
      case STRING:
      case INT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
      default:
        return new StringBuilder(schema.getType().toString());
    }
    throw new UnsupportedOperationException("Unsupported type");
  }

  /**
   * Generates a DDL statement for creating a view of a Kiji table within Hive.
   *
   * @param kijiURI of the table which define the connection parameters used within the DDL.
   * @param kijiTableLayout layout of the table whose schema should be used for the DDL.
   * @param schemaTable of the Kiji instance for type mappings
   * @return String representing the DDL statement that Hive use to materialize
   * @throws IOException if there is an issue retrieving columns from the layout.
   */
  private static String generateHiveDDLStatement(KijiURI kijiURI,
                                                 KijiTableLayout kijiTableLayout,
                                                 KijiSchemaTable schemaTable)
      throws IOException {
    Collection<KijiColumnName> kijiColumnNames = getKijiColumns(kijiTableLayout);
    Set<KijiColumnName> kijiCounterColumns = Sets.newHashSet();

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE EXTERNAL TABLE ")
      .append(kijiURI.getTable())
      .append(" (\n");

    // Hive column name and type information.
    sb.append("  `entity_id` STRING");

    for (KijiColumnName kijiColumnName : kijiColumnNames) {
      String hiveType = getHiveType(kijiColumnName, kijiTableLayout, schemaTable);

      // Counters aren't explicitly supported in Hive yet, so skip over these columns.
      if (!hiveType.isEmpty()) {
        sb.append(",\n");

        sb.append("  `") // We wrap all Column names with backticks to avoid Hive reserved words
          .append(getDefaultHiveColumnName(kijiColumnName))
          .append("` ")
          .append(getHiveType(kijiColumnName, kijiTableLayout, schemaTable));
      } else {
        kijiCounterColumns.add(kijiColumnName);
      }
    }
    sb.append("\n");
    sb.append(")\n");

    // Mappings of Hive columns to Kiji columns
    sb.append("STORED BY 'org.kiji.hive.KijiTableStorageHandler'\n");
    sb.append("WITH SERDEPROPERTIES (\n");
    sb.append("  'kiji.columns' = ':entity_id");
    for (KijiColumnName kijiColumnName : kijiColumnNames) {
      if (!kijiCounterColumns.contains(kijiColumnName)) {
        sb.append(",")
          .append(kijiColumnName.getName())
          .append("[0]");
      }
    }
    sb.append("'\n");
    sb.append(")\n");

    // Connection information for the Kiji instance where this table resides.
    sb.append("TBLPROPERTIES (\n");
    sb.append("  'kiji.table.uri' = '")
      .append(kijiURI.toString())
      .append("'\n");
    sb.append(");");
    return sb.toString();
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Kiji kiji = null;
    KijiTable kijiTable = null;
    KijiSchemaTable schemaTable = null;
    try {
      kiji = Kiji.Factory.open(mTableURI);
      kijiTable = kiji.openTable(mTableURI.getTable());
      KijiTableLayout kijiTableLayout = kijiTable.getLayout();
      schemaTable = kiji.getSchemaTable();
      getPrintStream().println(generateHiveDDLStatement(mTableURI, kijiTableLayout, schemaTable));
    } catch (IOException ioe) {
      LOG.warn(ioe.getMessage());
      return FAILURE;
    } finally {
      if (null != kiji) {
        ResourceUtils.releaseOrLog(kiji);
      }
      if (null != kijiTable) {
        ResourceUtils.releaseOrLog(kijiTable);
      }
    }
    return SUCCESS;
  }
}
