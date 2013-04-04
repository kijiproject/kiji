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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.utils.DataRequestOptimizer;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Manages the description of the hive table providing the "view" of a KijiTable.
 */
public final class HiveTableDescription {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableDescription.class);

  /** Describes the types of the columns in the table (it is a struct type). */
  private final StructTypeInfo mTypeInfo;

  /** Responsible for reading hive column data from a deserialized in-memory row object. */
  private final ObjectInspector mObjectInspector;

  /** The column expressions describing how to map data from the Kiji table into the columns. */
  private final List<KijiRowExpression> mExpressions;

  /** The data request we'll use to read from the kiji table. */
  private final KijiDataRequest mDataRequest;

  /** The schema table of the kiji instance. */
  private final KijiSchemaTable mSchemaTable;

  /** The layout of the table we are reading from. */
  private final KijiTableLayout mTableLayout;

  /** A cell decoder factory the decodes Avro cells stored in Kiji using the Avro generic api. */
  private final KijiCellDecoderFactory mCellDecoderFactory;

  /** Builder for constructing a HiveTableDescription. */
  public static final class HiveTableDescriptionBuilder {
    private List<String> mColumnNames;
    private List<TypeInfo> mColumnTypes;
    private List<String> mColumnExpressions;
    private KijiSchemaTable mSchemaTable;
    private KijiTableLayout mTableLayout;
    private KijiCellDecoderFactory mCellDecoderFactory;

    /** True if we already built an object. */
    private boolean mIsBuilt = false;

    /**
     * Sets the Hive column names.
     *
     * @param columnNames The column names.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withColumnNames(List<String> columnNames) {
      checkNotBuilt();
      mColumnNames = columnNames;
      return this;
    }

    /**
     * Sets the Hive column types.
     *
     * @param columnTypes The column types.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withColumnTypes(List<TypeInfo> columnTypes) {
      checkNotBuilt();
      mColumnTypes = columnTypes;
      return this;
    }

    /**
     * Sets the Kiji row expressions that map data to the Hive columns.
     *
     * @param columnExpressions The Kiji row expressions.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withColumnExpressions(List<String> columnExpressions) {
      checkNotBuilt();
      mColumnExpressions = columnExpressions;
      return this;
    }

    /**
     * Sets the Kiji schema table.
     *
     * @param schemaTable The schema table.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withSchemaTable(KijiSchemaTable schemaTable) {
      checkNotBuilt();
      mSchemaTable = schemaTable;
      return this;
    }

    /**
     * Sets the Kiji table layout.
     *
     * @param tableLayout The layout.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withTableLayout(KijiTableLayout tableLayout) {
      checkNotBuilt();
      mTableLayout = tableLayout;
      return this;
    }

    /**
     * Sets the Kiji cell decoder factory.
     *
     * @param cellDecoderFactory A cell decoder factory.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withCellDecoderFactory(
        KijiCellDecoderFactory cellDecoderFactory) {
      checkNotBuilt();
      mCellDecoderFactory = cellDecoderFactory;
      return this;
    }

    /**
     * Validates the builder arguments and builds the HiveTableDescription.
     *
     * @return HiveTableDescription with the specified parameters.
     */
    public HiveTableDescription build() {
      Preconditions.checkArgument(mColumnNames.size() == mColumnTypes.size(),
          "Unable to read the hive column names and types.");
      Preconditions.checkArgument(mColumnNames.size() == mColumnExpressions.size(),
          "Incorrect number of column expressions specified. "
              + "There must be one expression per column in the hive table.");

      checkNotBuilt();
      mIsBuilt = true;
      return new HiveTableDescription(this);
    }

    /**
     * @throws IllegalStateException after the KijiDataRequest has been built with {@link #build()}.
     *     Prevents reusing this builder.
     */
    private void checkNotBuilt() {
      Preconditions.checkState(!mIsBuilt,
          "HiveTableDescription builder cannot be used after build() is invoked.");
    }
  }

  /**
   * Constructs a new HiveTableDescriptionBuilder.
   *
   * @return a new KijiTablePoolBuilder with the default options.
   */
  public static HiveTableDescriptionBuilder newBuilder() {
    return new HiveTableDescriptionBuilder();
  }

  /**
   * Constructs a new HiveTableDescription with the specified parameters.  This class should not
   * be instantiated outside of the builder {@link HiveTableDescriptionBuilder}.
   *
   * @param builder HiveTableDescriptionBuilder which contains the configuration parameters to build
   *                this HiveTableDescription with.
   */
  private HiveTableDescription(HiveTableDescriptionBuilder builder) {

    mTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        builder.mColumnNames, builder.mColumnTypes);
    mObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(mTypeInfo);
    mExpressions = new ArrayList<KijiRowExpression>();
    for (int i = 0; i < builder.mColumnExpressions.size(); i++) {
      final String expression = builder.mColumnExpressions.get(i);
      final TypeInfo typeInfo = builder.mColumnTypes.get(i);
      mExpressions.add(new KijiRowExpression(expression, typeInfo));
    }

    mDataRequest = DataRequestOptimizer.getDataRequest(mExpressions);
    mSchemaTable = builder.mSchemaTable;
    mTableLayout = builder.mTableLayout;
    mCellDecoderFactory = builder.mCellDecoderFactory;
  }

  /**
   * Gets the data request required to provide data to this Hive table.
   *
   * @return The data request.
   */
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /**
   * Gets the object inspector that can read column data from an in-memory row object.
   *
   * @return The object inspector.
   */
  public ObjectInspector getObjectInspector() {
    return mObjectInspector;
  }

  /**
   * Creates the in-memory row object that contains the column data in the hive table.
   *
   * <p>The returned object will be given to the object inspector for
   * extracting column data. Since our object inspector is the
   * standard java inspector, the structure of the object returned
   * should match the data types specified in the hive table schema.</p>
   *
   * @param result The HBase data from the row.
   * @return An object representing the row.
   * @throws IOException If there is an IO error.
   */
  public Object createDataObject(Result result) throws IOException {
    // The top-level object needs to be a List because it represents
    // the columns in the row.
    List<Object> columnData = new ArrayList<Object>();

    //TODO This should be done without a deprecated class, but this allows us to not duplicate
    //code from the HBaseKijiRowData for decoding purposes.
    final KijiRowData kijiRowData = new HBaseKijiRowData(
        mDataRequest,
        mCellDecoderFactory,
        mTableLayout,
        result,
        mSchemaTable);
    for (KijiRowExpression expression : mExpressions) {
      columnData.add(expression.evaluate(kijiRowData));
    }

    return columnData;
  }
}
