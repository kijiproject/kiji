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
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

import org.kiji.hive.io.EntityIdWritable;
import org.kiji.hive.io.KijiCellWritable;
import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.hive.utils.DataRequestOptimizer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Manages the description of the Hive table providing the "view" of a KijiTable.
 */
public final class HiveTableDescription {
  // TODO: This class could be removed since it no longer persists the TableLayout information
  // necessary for decoding.

  /** Describes the types of the columns in the table (it is a struct type). */
  private final StructTypeInfo mTypeInfo;

  /** Responsible for reading hive column data from a deserialized in-memory row object. */
  private final ObjectInspector mObjectInspector;

  /** The column expressions describing how to map data from the Kiji table into the columns. */
  private final List<KijiRowExpression> mExpressions;

  /** The column that contains the shell string used for determining the EntityId for writes. */
  private final Integer mEntityIdShellStringIndex;

  /** The data request we'll use to read from the kiji table. */
  private final KijiDataRequest mDataRequest;

  /** Builder for constructing a HiveTableDescription. */
  public static final class HiveTableDescriptionBuilder {
    private List<String> mColumnNames;
    private List<TypeInfo> mColumnTypes;
    private List<String> mColumnExpressions;

    private String mEntityIdShellStringColumn;

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
     * Sets the Hive column to be used for determining the shell string used for finding EntityId
     * to write with in Hive.
     *
     * @param entityIdShellStringColumn The Hive column representing the EntityId shell string.
     * @return This instance.
     */
    public HiveTableDescriptionBuilder withEntityIdShellStringColumn(
        String entityIdShellStringColumn) {
      checkNotBuilt();
      mEntityIdShellStringColumn = entityIdShellStringColumn;
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
              + "There must be one expression per column in the Hive table.");

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

    String entityIdShellStringColumn = builder.mEntityIdShellStringColumn;
    if (entityIdShellStringColumn != null) {
      mEntityIdShellStringIndex = builder.mColumnNames.indexOf(entityIdShellStringColumn);
      Preconditions.checkState(-1 != mEntityIdShellStringIndex,
          "EntityIdColumn %s not found in column list.", entityIdShellStringColumn);
    } else {
      mEntityIdShellStringIndex = null;
    }
    // TODO(KIJIHIVE-30) Process EntityId component columns here.

    mDataRequest = DataRequestOptimizer.getDataRequest(mExpressions);
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
   * Creates the in-memory row object that contains the column data in the Hive table.
   *
   * <p>The returned object will be given to the object inspector for
   * extracting column data. Since our object inspector is the
   * standard java inspector, the structure of the object returned
   * should match the data types specified in the Hive table schema.</p>
   *
   * @param kijiRowData The HBase data from the row.
   * @return An object representing the row.
   * @throws IOException If there is an IO error.
   */
  public Object createDataObject(KijiRowDataWritable kijiRowData) throws IOException {
    // The top-level object needs to be a List because it represents
    // the columns in the row.
    List<Object> columnData = Lists.newArrayList();
    for (KijiRowExpression expression : mExpressions) {
      columnData.add(expression.evaluate(kijiRowData));
    }

    return columnData;
  }

  /**
   * Creates the in-memory row object that contains the column data in the Hive table.
   *
   * <p>The returned object will be given to the object inspector for
   * extracting column data. Since our object inspector is the
   * standard java inspector, the structure of the object returned
   * should match the data types specified in the Hive table schema.</p>
   *
   * @param columnData The HBase data from the row.
   * @param objectInspector The object inspector defining the format of the columnData.
   * @return An object representing the row.
   * @throws IOException If there is an IO error.
   */
  public KijiRowDataWritable createWritableObject(
      Object columnData, ObjectInspector objectInspector) throws IOException {

    Preconditions.checkArgument(objectInspector instanceof StandardStructObjectInspector);
    StandardStructObjectInspector structObjectInspector =
        (StandardStructObjectInspector) objectInspector;

    // Hive passes us a struct that should have all columns that are specified in the Hive table
    // description.
    Preconditions.checkState(
        mExpressions.size() == structObjectInspector.getAllStructFieldRefs().size(),
        "Table has {} columns, but query has {} columns",
        mExpressions.size(),
        structObjectInspector.getAllStructFieldRefs().size());

    List<Object> structColumnData = structObjectInspector.getStructFieldsDataAsList(columnData);
    Object entityIdShellStringObject = structColumnData.get(mEntityIdShellStringIndex);
    Text entityIdShellString = new Text((String) entityIdShellStringObject);
    EntityIdWritable entityIdWritable = new EntityIdWritable(entityIdShellString.toString());

    // TODO(KIJIHIVE-30) Process EntityId component columns here.

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> writableData = Maps.newHashMap();
    for (int c=0; c < mExpressions.size(); c++) {
      if (mExpressions.get(c).isCellData()) {
        ObjectInspector colObjectInspector =
            structObjectInspector.getAllStructFieldRefs().get(c).getFieldObjectInspector();
        Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> writableTimeseriesData =
            mExpressions.get(c).convertToTimeSeries(colObjectInspector, structColumnData.get(c));
        for (KijiColumnName kijiColumnName : writableTimeseriesData.keySet()) {
          NavigableMap<Long, KijiCellWritable> columnTimeseries =
              writableTimeseriesData.get(kijiColumnName);

          if (writableData.containsKey(kijiColumnName)) {
            // Merge these timeseries together.
            writableData.get(kijiColumnName).putAll(columnTimeseries);
          } else {
            writableData.put(kijiColumnName, columnTimeseries);
          }
        }
      }
    }

    KijiRowDataWritable kijiRowData = new KijiRowDataWritable(entityIdWritable, writableData);
    return kijiRowData;
  }

  /**
   * Determines whether or not this HiveTableDescription is writable or not.
   *
   * @return whether this Hive table description is a writable one or not.
   */
  public boolean isWritable() {
    if (mEntityIdShellStringIndex != null) {
      return true;
    }
    return false;
  }
}
