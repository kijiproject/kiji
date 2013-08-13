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

package org.kiji.hive.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.util.TimestampComparator;

/**
 * Writable version of the data stored within a KijiRowData.  Contains a subset of methods
 * which are necessary for the Hive adapter.
 */
public class KijiRowDataWritable implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiRowDataWritable.class);

  private EntityIdWritable mEntityId;

  // Backing store of the cell data contained in this row expressed as Writables.
  private Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> mWritableData;

  // Decoded data to be used by clients.  Lazily initialized, since these objects can be created
  // expressly for serialization.
  private Map<KijiColumnName, NavigableMap<Long, Object>> mDecodedData;

  // Schema data required decoding Avro data within cells.
  private Map<KijiColumnName, Schema> mSchemas;

  /** Required so that this can be built by WritableFactories. */
  public KijiRowDataWritable() {
  }

  /**
   * Construct a KijiRowDataWritable from the Writable objects generated from Hive.
   *
   * @param entityIdWritable that maps to the row key.
   * @param writableData of column to timeseries data.
   */
  public KijiRowDataWritable(EntityIdWritable entityIdWritable,
      Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> writableData) {
    mEntityId = entityIdWritable;
    mWritableData = writableData;
  }

  /**
   * Constructs a KijiRowDataWritable from a existing KijiRowData.
   * @param rowData the source of the fields to copy.
   * @throws IOException if there is an error loading the table layout.
   */
  public KijiRowDataWritable(KijiRowData rowData) throws IOException {
    Preconditions.checkArgument(rowData instanceof HBaseKijiRowData,
        "KijiRowData must be an instance of HBaseKijiRowData to read TableLayout information.");

    mEntityId = new EntityIdWritable(rowData.getEntityId());
    mWritableData = Maps.newHashMap();
    mSchemas = Maps.newHashMap();

    HBaseKijiRowData hBaseKijiRowData = (HBaseKijiRowData) rowData;
    for (FamilyLayout familyLayout : hBaseKijiRowData.getTableLayout().getFamilies()) {
      String family = familyLayout.getName();
      for (String qualifier : rowData.getQualifiers(family)) {
        if (rowData.getCells(family, qualifier) != null) {
          KijiColumnName column = new KijiColumnName(family, qualifier);
          NavigableMap<Long, KijiCellWritable> data =
              convertCellsToWritable(rowData.getCells(family, qualifier));

          mWritableData.put(column, data);

          Schema schema = rowData.getReaderSchema(family, qualifier);
          mSchemas.put(column, schema);
        }
      }
    }
  }

  /**
   * Returns an unmodifiable map of column names to timeseries of KijiCell data.  Note that the
   * individual timeseries are mutable collections.
   *
   * @return map of KijiColumnName to timeseries of data.
   */
  public Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> getData() {
    return Collections.unmodifiableMap(mWritableData);
  }

  /**
   * Converts a timeseries of KijiCell data into a Writable timeseries for serialization.
   * @param timeseries from KijiRowData.
   * @return a Writable timeseries suitable for serialization.
   */
  private NavigableMap<Long, KijiCellWritable> convertCellsToWritable(
      NavigableMap<Long, KijiCell<Object>> timeseries) {
    NavigableMap<Long, KijiCellWritable> writableTimeseries =
        Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, KijiCell<Object>> entry : timeseries.entrySet()) {
      Long timestamp = entry.getKey();
      KijiCellWritable kijiCellWritable = new KijiCellWritable(entry.getValue());
      writableTimeseries.put(timestamp, kijiCellWritable);
    }
    return writableTimeseries;
  }

  /**
   * Extracts the cells from a Writable timeseries.
   * @param writableTimeseries generated from {@link #convertCellsToWritable}
   * @return timeseries data without Writable wrappers.
   */
  private NavigableMap<Long, Object> extractCellsfromWritable(
      NavigableMap<Long, KijiCellWritable> writableTimeseries) {
    Preconditions.checkNotNull(writableTimeseries);
    NavigableMap<Long, Object> timeseries = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, KijiCellWritable> entry : writableTimeseries.entrySet()) {
      Long timestamp = entry.getKey();
      KijiCellWritable kijiCellWritable = entry.getValue();
      Object cellData = kijiCellWritable.getData();
      timeseries.put(timestamp, cellData);
    }
    return timeseries;
  }

  /** @return decoded cell data(initializing it if necessary). */
  private Map<KijiColumnName, NavigableMap<Long, Object>> getDecodedData() {
    if (mDecodedData == null) {
      Preconditions.checkNotNull(mWritableData);
      mDecodedData = Maps.newHashMap();
      for (KijiColumnName column : mWritableData.keySet()) {
        NavigableMap<Long, KijiCellWritable> writableTimeSeries = mWritableData.get(column);
        mDecodedData.put(column, extractCellsfromWritable(writableTimeSeries));
      }
    }
    return mDecodedData;
  }

  /** @return The row key. */
  public EntityIdWritable getEntityId() {
    return mEntityId;
  }

  /**
   * Determines whether a particular column family has data in this row.
   *
   * @param family Column family to check for.
   * @return Whether the specified column family has data in this row.
   */
  public boolean containsColumn(String family) {
    for (KijiColumnName column : mWritableData.keySet()) {
      if (family.equals(column.getFamily())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines whether a particular column has data in this row.
   *
   * @param family Column family of the column to check for.
   * @param qualifier Column qualifier of the column to check for.
   * @return Whether the specified column has data in this row.
   */
  public boolean containsColumn(String family, String qualifier) {
    KijiColumnName column = new KijiColumnName(family, qualifier);
    return mWritableData.keySet().contains(column);
  }

  /**
   * Gets the set of column qualifiers that exist in a column family in this row.
   *
   * @param family Column family to get column qualifiers from.
   * @return Set of column qualifiers that exist in the <code>family</code> column family.
   */
  public NavigableSet<String> getQualifiers(String family) {
    NavigableSet<String> qualifiers = Sets.newTreeSet();
    for (KijiColumnName column : getDecodedData().keySet()) {
      if (family.equals(column.getFamily())) {
        qualifiers.add(column.getQualifier());
      }
    }
    return qualifiers;
  }

  /**
   * Gets the reader schema for a column as declared in the layout of the table this row
   * comes from.
   *
   * @param family Column family of the desired column schema.
   * @param qualifier Column qualifier of the desired column schema.
   * @return Avro reader schema for the column.
   * @throws IOException If there is an error or the column does not exist.
   * @see org.kiji.schema.layout.KijiTableLayout
   */
  public Schema getReaderSchema(String family, String qualifier) throws IOException {
    KijiColumnName column = new KijiColumnName(family, qualifier);
    return mSchemas.get(column);
  }

  /**
   * Gets all data stored within the specified column family.
   *
   * @param family Map type column family of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column family.
   * @throws IOException If there is an error.
   */
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException {
    NavigableMap<String, NavigableMap<Long, T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      NavigableMap<Long, T> values = getValues(family, qualifier);
      result.put(qualifier, values);
    }
    return result;
  }

  /**
   * Gets all data stored within the specified column.
   *
   * @param family Column family of the desired data.
   * @param qualifier Column qualifier of the desired data.
   * @param <T> Type of the data stored at the specified coordinates.
   * @return A sorted map containing the data stored in the specified column.
   * @throws IOException If there is an error.
   */
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier) throws IOException {
    KijiColumnName column = new KijiColumnName(family, qualifier);
    return (NavigableMap<Long, T>) getDecodedData().get(column);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    EntityIdWritable entityIdWritable = (EntityIdWritable) mEntityId;
    entityIdWritable.write(out);

    WritableUtils.writeVInt(out, mWritableData.size());
    for (Map.Entry<KijiColumnName, NavigableMap<Long, KijiCellWritable>> entry
        : mWritableData.entrySet()) {
      WritableUtils.writeString(out, entry.getKey().getName());

      NavigableMap<Long, KijiCellWritable> data = entry.getValue();
      WritableUtils.writeVInt(out, data.size()); // number in the timeseries
      for (Map.Entry<Long, KijiCellWritable> cellEntry : data.entrySet()) {
        WritableUtils.writeVLong(out, cellEntry.getKey());
        cellEntry.getValue().write(out);
      }
    }

    WritableUtils.writeVInt(out, mSchemas.size());
    for (Map.Entry<KijiColumnName, Schema> entry : mSchemas.entrySet()) {
      WritableUtils.writeString(out, entry.getKey().getName());
      WritableUtils.writeString(out, entry.getValue().toString());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    EntityIdWritable entityIdWritable =
        (EntityIdWritable) WritableFactories.newInstance(EntityIdWritable.class);
    entityIdWritable.readFields(in);
    mEntityId = entityIdWritable;

    int numDecodedData = WritableUtils.readVInt(in);

    // We need to dirty the decoded data so that these objects can be reused.
    mDecodedData = null;

    mWritableData = Maps.newHashMap();
    for (int c = 0; c < numDecodedData; c++) {
      String columnText = WritableUtils.readString(in);
      KijiColumnName column = new KijiColumnName(columnText);

      NavigableMap<Long, KijiCellWritable> data = Maps.newTreeMap();
      int numCells = WritableUtils.readVInt(in);
      for (int d = 0; d < numCells; d++) {
        long ts = WritableUtils.readVLong(in);
        KijiCellWritable cellWritable =
            (KijiCellWritable) WritableFactories.newInstance(KijiCellWritable.class);
        cellWritable.readFields(in);
        data.put(ts, cellWritable);
      }

      mWritableData.put(column, data);
    }

    mSchemas = Maps.newHashMap();
    int numSchemas = WritableUtils.readVInt(in);
    for (int c=0; c < numSchemas; c++) {
      String columnText = WritableUtils.readString(in);
      KijiColumnName column = new KijiColumnName(columnText);
      String schemaString = WritableUtils.readString(in);
      Schema schema = new Schema.Parser().parse(schemaString);
      mSchemas.put(column, schema);
    }
  }
}
