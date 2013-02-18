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

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.NoSuchColumnException;

/**
 * This producer copies data from one family or column to another without modification.
 *
 * <p>To use this producer, you must specify an <i>input</i> and an <i>output</i>.  The
 * input may be a single column of the form <i>"family:qualifier"</i>, or an entire family
 * of the form <i>"family"</i>.  The input will be copied to the target output column or
 * family.</p>
 *
 * <p>To specify the input column name, set the configuration variable
 * <i>identity.producer.input</i>.  The output column name is set with the configuration
 * variable <i>identity.producer.output</i>.</p>
 */
public class IdentityProducer extends KijiProducer {
  public static final String CONF_INPUT = "identity.producer.input";
  public static final String CONF_OUTPUT = "identity.producer.output";

  private KijiColumnName mInputColumn;
  private KijiColumnName mOutputColumn;

  /** A cache for the reader schemas of the input columns, initialized in the first produce(). */
  private SchemaCache mSchemaCache;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);

    // Validate that they are either both families or both columns.
    if (mInputColumn.isFullyQualified() != mOutputColumn.isFullyQualified()) {
      throw new RuntimeException(
          "Input and output must both be a specific column, or both be a family");
    }
  }

  /**
   * Sets the input column name.
   *
   * @param column The input column.
   */
  @HadoopConf(key=CONF_INPUT, usage="The input column name.")
  protected void setInputColumn(String column) {
    if (null == column || column.isEmpty()) {
      throw new RuntimeException("Must specify " + CONF_INPUT);
    }
    mInputColumn = new KijiColumnName(column);
  }

  /**
   * Sets the output column name.
   *
   * @param column The output column.
   */
  @HadoopConf(key=CONF_OUTPUT, usage="The output column name.")
  protected void setOutputColumn(String column) {
    if (null == column || column.isEmpty()) {
      throw new RuntimeException("Must specify " + CONF_OUTPUT);
    }
    mOutputColumn = new KijiColumnName(column);
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
        .add(mInputColumn.getFamily(), mInputColumn.getQualifier());
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mOutputColumn.toString();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    if (null == mSchemaCache) {
      mSchemaCache = new SchemaCache(input, mInputColumn.getFamily());
    }

    if (!mInputColumn.isFullyQualified()) {
      // Copy the entire family.
      for (String qualifier : input.getQualifiers(mInputColumn.getFamily())) {
        KijiColumnName sourceColumn = new KijiColumnName(mInputColumn.getFamily(), qualifier);
        produceAllVersions(input, context, sourceColumn);
      }
    } else {
      // Copy just a specific column.
      produceAllVersions(input, context, mInputColumn);
    }
  }

  /**
   * Produces all data from a given column name into the output column.
   *
   * @param input The input row.
   * @param context The producer context used to write.
   * @param columnName The column to read from.
   * @throws IOException If there is an IO error.
   */
  private void produceAllVersions(
      KijiRowData input, ProducerContext context, KijiColumnName columnName)
      throws IOException {
    Schema schema = mSchemaCache.get(input, columnName);
    for (long timestamp : input.getTimestamps(columnName.getFamily(), columnName.getQualifier())) {
      // Read the data from the input column.
      Object data = input.getValue(
          mInputColumn.getFamily(), columnName.getQualifier(), timestamp);

      // Write the data to the output column.
      if (!mOutputColumn.isFullyQualified()) {
        context.put(columnName.getQualifier(), timestamp, new DecodedCell<Object>(schema, data));
      } else {
        context.put(timestamp, new DecodedCell<Object>(schema, data));
      }
    }
  }

  /**
   * A cache of schemas, so we don't need to re-query for them at every row.
   */
  private static class SchemaCache {
    /** Whether the input column is a map type family. */
    private final boolean mIsMapTypeFamily;

    /** A cache of reader schemas keyed by column name. */
    private final Map<KijiColumnName, Schema> mCache;

    /**
     * Creates a new <code>SchemaCache</code> instance.
     *
     * @param rowData A kiji row data.
     * @param inputFamily The family to read from schemas from.
     * @throws IOException If there is an error.
     */
    public SchemaCache(KijiRowData rowData, String inputFamily) throws IOException {
      // To optimize, we need to figure out whether this is map-type family.  If so, we
      // don't need to query for the reader schema for each qualifier; it's going to be
      // the same every time.  Since the column qualifier "" (empty) is only valid for map
      // type families, we will get a NoSuchColumnException if it is a group type family.
      boolean isMapTypeFamily = true;
      try {
        rowData.getReaderSchema(inputFamily, "");
      } catch (NoSuchColumnException e) {
        isMapTypeFamily = false;
      }

      mIsMapTypeFamily = isMapTypeFamily;
      mCache = new HashMap<KijiColumnName, Schema>();
    }

    /**
     * Gets the reader schema for a column.
     *
     * @param input The kiji row data.
     * @param columnName The name of a column.
     * @return The reader schema for that column, according to the table layout.
     * @throws IOException If the schema cannot be retrieved.
     */
    public Schema get(KijiRowData input, KijiColumnName columnName) throws IOException {
      KijiColumnName cacheKey = columnName;
      if (mIsMapTypeFamily) {
        // We can ignore the qualifier in the column name, since all reader schemas within
        // a map-type family are the same.
        cacheKey = new KijiColumnName(columnName.getFamily());
      }

      Schema cacheResult = mCache.get(cacheKey);
      if (null != cacheResult) {
        // Cache hit, yay!
        return cacheResult;
      }

      // Cache miss. Query for the reader schema.
      Schema schema = input.getReaderSchema(columnName.getFamily(), columnName.getQualifier());

      // Put it in the cache, and return.
      mCache.put(cacheKey, schema);
      return schema;
    }
  }
}
