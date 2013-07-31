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

package org.kiji.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.hive.utils.KijiDataRequestSerializer;

/**
 * A serializer and deserializer for reading from and writing to Kiji tables in Hive.
 *
 * Main entry point for the Kiji Hive Adapter.
 */
public class KijiTableSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableSerDe.class);

  // Property for specifying which columns are used within a Hive view.
  public static final String LIST_COLUMN_EXPRESSIONS = "kiji.columns";

  // Property for specifying which column represents the EntityId's shell string.
  // Cannot be specified at the same time as LIST_ENTITY_ID_COMPONENTS.
  public static final String ENTITY_ID_SHELL_STRING = "kiji.entity.id.shell.string";

  // Property specifying a list of Hive columns which represent the EntityId.
  // Cannot be specified at the same time as ENTITY_ID_SHELL_STRING.
  // TODO(KIJIHIVE-30): this feature isn't yet supported, but can come as a later patch.
  // Make a ticket and prioritize it accordingly.
  public static final String LIST_ENTITY_ID_COMPONENTS = "kiji.entity.id.columns";

  /**
   * This contains all the information about a Hive table we need to interact with a Kiji table.
   */
  private HiveTableDescription mHiveTableDescription;

  /** {@inheritDoc} */
  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    // Read from the magic property that contains the hive table definition's column names.
    final List<String> columnNames = readPropertyList(properties, Constants.LIST_COLUMNS);

    // Read from the magic property that contains the hive table definition's column types.
    final String columnTypes = properties.getProperty(Constants.LIST_COLUMN_TYPES);

    // Read from a property we require that contains the expressions specifying the data to map.
    Preconditions.checkArgument(properties.containsKey(LIST_COLUMN_EXPRESSIONS),
        "SERDEPROPERTIES missing configuration for property: {}", LIST_COLUMN_EXPRESSIONS);
    final List<String> columnExpressions = readPropertyList(properties, LIST_COLUMN_EXPRESSIONS);

    // Check that at least one of LIST_ENTITY_ID_COMPONENTS or ENTITY_ID_SHELL_STRING is
    // unspecified.
    Preconditions.checkArgument(!properties.containsKey(ENTITY_ID_SHELL_STRING)
        || !properties.containsKey(LIST_ENTITY_ID_COMPONENTS),
        "SERDEPROPERTIES cannot specify both: %s and %x.",
        ENTITY_ID_SHELL_STRING,
        LIST_ENTITY_ID_COMPONENTS);

    // Read from an optional property that contains the shell string representing the EntityId to
    // write back to Kiji with.
    String entityIdShellString = properties.getProperty(ENTITY_ID_SHELL_STRING);

    mHiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(TypeInfoUtils.getTypeInfosFromTypeString(columnTypes))
        .withColumnExpressions(columnExpressions)
        .withEntityIdShellStringColumn(entityIdShellString)
        .build();

    if (!mHiveTableDescription.isWritable()) {
      LOG.warn("Neither {} nor {} unspecified, so this Hive view of a KijiTable is read only.",
          ENTITY_ID_SHELL_STRING,
          LIST_ENTITY_ID_COMPONENTS);
    }

    try {
      if (null == conf) {
        conf = new HBaseConfiguration();
      }
      conf.set(KijiTableInputFormat.CONF_KIJI_DATA_REQUEST,
          KijiDataRequestSerializer.serialize(mHiveTableDescription.getDataRequest()));
    } catch (IOException e) {
      throw new SerDeException("Unable to construct the data request.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Result.class;
  }

  /** {@inheritDoc} */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (!mHiveTableDescription.isWritable()) {
      throw new SerDeException("KijiTable has no EntityId mapping and is not writable.");
    }
    try {
      return mHiveTableDescription.createWritableObject(obj, objInspector);
    } catch (IOException e) {
      throw new SerDeException("Error writing data from the HBase result", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    final KijiRowDataWritable result = (KijiRowDataWritable) blob;
    try {
      return mHiveTableDescription.createDataObject(result);
    } catch (IOException e) {
      throw new SerDeException("Error reading data from the HBase result", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return mHiveTableDescription.getObjectInspector();
  }

  /** {@inheritDoc} */
  @Override
  public SerDeStats getSerDeStats() {
    // We don't support statistics.
    return null;
  }

  /**
   * Reads a comma-separated list of strings from a properties object.
   *
   * @param properties The properties object to read from.
   * @param name The field name to read from.
   * @return A list of the comma-separated fields in the property value.
   */
  private static List<String> readPropertyList(Properties properties, String name) {
    return Arrays.asList(properties.getProperty(name).split(","));
  }
}
