/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.impl.cassandra;

import java.util.LinkedHashMap;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Provides utility methods and constants for constructing CQL statements.
 *
 * <h2>Notes on Kiji & Cassandra data model Entity ID to Primary Key translation</h2>
 *
 * <p>
 *   Cassandra (CQL) has the notion of a primary key, which consists of 1 or more CQL columns.  A
 *   primary key composed of >1 column is a compound primary key.  For example, the following table
 *   definition has a compound primary key consisting of two columns (c1, c2):
 * </p>
 *
 * <pre>
 *    CREATE TABLE t1 (
 *      c1 varchar,
 *      c2 int,
 *      c3 blob,
 *      PRIMARY KEY (c1, c2)
 *    )
 * </pre>
 *
 * <p>
 *   The first element of a compound primary key (or the sole element of a non-compound primary key)
 *   is the partition key. For example, in table t1, c1 is the partition key. The partition key is
 *   tokenized in order to determine what partition a row will fall into. IUD operations on rows
 *   with the same partition key are performed atomically and in isolation (theoretically).
 * </p>
 *
 * <p>
 *   The remaining elements of a primary key (if they exist) are referred to as the clustering
 *   columns.  For example, in table t1, c2 is the sole clustering column.
 * </p>
 *
 * <p>
 *   Partition keys can be made up of multiple columns using a composite partition key, for example:
 * </p>
 *
 * <pre>
 *    CREATE TABLE t2 (
 *      c1 uuid,
 *      c2 varchar,
 *      c3 int,
 *      c4 int,
 *      c5 blob,
 *      PRIMARY KEY((c1, c2), c3, c4)
 *    );
 * </pre>
 *
 * <p>
 *   Table t2 has a composite partition key consisting of c1 and c2. Table t2 has clustering columns
 *   c3 and c4.
 * </p>
 *
 * <p>
 *   Kiji RowKeyFormat2 defines 2 valid entity ID formats: formatted and raw.
 * </p>
 *
 * <ul>
 *   <li><em>Formatted</em>: formatted entity IDs consist of 1 or more components of type STRING,
 *     INT, or LONG. additionally, 1 or more of the components (in sequence) must be hashed.  The
 *     hashed components correspond exactly to the partition key of the CQL primary key.  The
 *     unhashed components correspond to the first clustering columns of the CQL primary key. The
 *     name of the columns will match the component names of the entity ID.
 *   </li>
 *
 *   <li><em>Raw</em>: raw entity IDs consist of a single byte array blob component. This single
 *     component corresponds to the partition key of the CQL primary key. There are no clustering
 *     columns in the CQL primary key. The name of the single primary key column is
 *     {@value #RAW_KEY_COL}.
 *   </li>
 * </ul>
 *
 *
 * <h2>Notes on Kiji Cassandra Tables</h2>
 *
 * <p>
 *   A single Kiji table is stored in Cassandra as multiple tables. There will be a Cassandra table
 *   per Kiji locality group.
 * </p>
 */
public final class CQLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CQLUtils.class);

  // Useful static members for referring to different fields in the C* tables.
  public static final String RAW_KEY_COL = "key";         // Only used for tables with raw eids
  public static final String FAMILY_COL = "family";
  public static final String QUALIFIER_COL = "qualifier";
  public static final String VERSION_COL = "version";     // Only used for locality group tables
  public static final String VALUE_COL = "value";

  static final String BYTES_TYPE = "blob";
  private static final String STRING_TYPE = "varchar";
  private static final String INT_TYPE = "int";
  private static final String LONG_TYPE = "bigint";

  public static final Joiner COMMA_JOINER = Joiner.on(", ");


  /**
   * Private constructor for utility class.
   */
  private CQLUtils() {
  }

  /**
   * Get the names and types of Entity ID columns in the Cassandra table.
   *
   * @param layout The table layout.
   * @return The names and types of Entity ID columns.
   */
  private static LinkedHashMap<String, String> getEntityIdColumnTypes(
      final KijiTableLayout layout
  ) {
    LinkedHashMap<String, String> columns = Maps.newLinkedHashMap();
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        columns.put(RAW_KEY_COL, BYTES_TYPE);
        break;
      }
      case FORMATTED: {
        for (RowKeyComponent component : keyFormat.getComponents()) {
          columns.put(
              translateEntityIDComponentNameToColumnName(component.getName()),
              getCQLType(component.getType()));
        }
        break;
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
    return columns;
  }

  /**
   * Return the columns and their associated types of the primary key for the associated table
   * layout. The returned LinkedHashMap can be iterated through in primary key column order.
   *
   * @param layout to get primary key column and types for.
   * @return a map of column name to CQL column type with proper iteration order.
   */
  private static LinkedHashMap<String, String> getLocalityGroupPrimaryKeyColumns(
      final KijiTableLayout layout
  ) {
    final LinkedHashMap<String, String> columns = getEntityIdColumnTypes(layout);
    columns.put(FAMILY_COL, BYTES_TYPE);
    columns.put(QUALIFIER_COL, BYTES_TYPE);
    columns.put(VERSION_COL, LONG_TYPE);
    return columns;
  }

  /**
   * Translates an EntityID ComponentType into a CQL type.
   *
   * @param type of entity id component to get CQL type for.
   * @return the CQL type of the provided ComponentType.
   */
  public static String getCQLType(ComponentType type) {
    switch (type) {
      case INTEGER: return INT_TYPE;
      case LONG: return LONG_TYPE;
      case STRING: return STRING_TYPE;
      default: throw new IllegalArgumentException();
    }
  }

  /**
   * Return the ordered list of columns in the partition key for the table layout.
   *
   * @param layout to return partition key columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getPartitionKeyColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: return Lists.newArrayList(RAW_KEY_COL);
      case FORMATTED:
        return transformToColumns(
            keyFormat.getComponents().subList(0, keyFormat.getRangeScanStartIndex()));
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }

  /**
   * Get the ordered list of cluster columns originating from the entity ID. This is the set of
   * 'scannable' entity ID components.
   *
   * @param layout The layou of the table.
   * @return the cluster columns of the table from the entity ID.
   */
  private static List<String> getEntityIdClusterColumns(KijiTableLayout layout) {
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    switch (keyFormat.getEncoding()) {
      case RAW: {
        return Lists.newArrayList();
      }
      case FORMATTED: {
        int size = keyFormat.getComponents().size();
        int start = keyFormat.getRangeScanStartIndex();
        if (start == size) {
          return Lists.newArrayList();
        } else {
          return transformToColumns(
              keyFormat
                  .getComponents()
                  .subList(keyFormat.getRangeScanStartIndex(), keyFormat.getComponents().size()));
        }
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", keyFormat.getEncoding()));
    }
  }


  /**
   * Return the ordered list of cluster columns for the table layout.
   *
   * @param layout to return cluster columns for.
   * @return the primary key columns for the layout.
   */
  public static List<String> getLocalityGroupClusterColumns(KijiTableLayout layout) {
    List<String> columns = getEntityIdClusterColumns(layout);
    columns.add(FAMILY_COL);
    columns.add(QUALIFIER_COL);
    columns.add(VERSION_COL);
    return columns;
  }

  /**
   * Return the CQL token column for a Kiji table layout.
   *
   * @param layout to create CQL token column for.
   * @return the CQL token column for the layout.
   */
  public static String getTokenColumn(KijiTableLayout layout) {
    return String.format("token(%s)", COMMA_JOINER.join(getPartitionKeyColumns(layout)));
  }

  /**
   * Given the name of an entity ID component, returns the corresponding Cassandra column name.
   *
   * Inserts a prefix to make sure that the column names for entity ID components don't conflict
   * with CQL reserved words or with other column names in Kiji Cassandra tables.
   *
   * @param entityIDComponentName The name of the entity ID component.
   * @return the name of the Cassandra column for this component.
   */
  public static String translateEntityIDComponentNameToColumnName(
      final String entityIDComponentName
  ) {
    return "eid_" + entityIDComponentName;
  }

  /**
   * Transforms a list of RowKeyComponents into a list of the column names.
   *
   * @param components to transform into columns.
   * @return a list of columns.
   */
  private static List<String> transformToColumns(List<RowKeyComponent> components) {
    List<String> list = Lists.newArrayList();
    for (RowKeyComponent component : components) {
      list.add(translateEntityIDComponentNameToColumnName(component.getName()));
    }
    return list;
  }

  /**
   * Returns a 'CREATE TABLE' statement for the provided table name and table layout.
   *
   * @param tableName of table to be created.
   * @param layout of kiji table.
   * @return a CQL 'CREATE TABLE' statement which will create the provided table.
   */
  public static String getCreateLocalityGroupTableStatement(
      final CassandraTableName tableName,
      final KijiTableLayout layout
  ) {
    Preconditions.checkArgument(tableName.isLocalityGroup(),
        "Table name '%s' is not for a locality group table.", tableName);

    LinkedHashMap<String, String> columns = getLocalityGroupPrimaryKeyColumns(layout);
    columns.put(VALUE_COL, BYTES_TYPE);

    // statement being built:
    //  "CREATE TABLE ${tableName} (
    //   ${PKColumn1} ${PKColumn1Type}, ${PKColumn2} ${PKColumn2Type}..., ${VALUE_COL} ${valueType}
    //   PRIMARY KEY ((${PartitionKeyComponent1} ${type}, ${PartitionKeyComponent2} ${type}...),
    //                ${ClusterColumn1} ${type}, ${ClusterColumn2} ${type}..))
    //   WITH CLUSTERING
    //   ORDER BY (${ClusterColumn1} ASC, ${ClusterColumn2} ASC..., ${VERSION_COL} DESC);

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(tableName).append(" (");

    COMMA_JOINER.withKeyValueSeparator(" ").appendTo(sb, columns);

    sb.append(", PRIMARY KEY ((");
    COMMA_JOINER.appendTo(sb, getPartitionKeyColumns(layout));
    sb.append(")");

    List<String> clusterColumns = getLocalityGroupClusterColumns(layout);
    if (clusterColumns.size() > 0) {
      sb.append(", ");
    }
    COMMA_JOINER.appendTo(sb, clusterColumns);

    sb.append(")) WITH CLUSTERING ORDER BY (");
    Joiner.on(" ASC, ").appendTo(sb, clusterColumns);

    sb.append(" DESC);");

    String query = sb.toString();

    LOG.info("Prepared query string for table create: {}", query);

    return query;
  }

  /**
   * Returns a CQL statement which drop a table.
   *
   * @param table The table to delete.
   * @return A CQL statement to drop the provided table.
   */
  public static String getDropTableStatement(CassandraTableName table) {
    return String.format("DROP TABLE IF EXISTS %s;", table);
  }
}
