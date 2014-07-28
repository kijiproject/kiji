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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.hive.utils.AvroTypeAdapter;
import org.kiji.schema.KijiCell;

/**
 * Writable version of the data stored within a KijiCell.
 */
public class KijiCellWritable implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiCellWritable.class);

  private long mTimestamp;
  private Schema mSchema;
  private Object mData;

  /** Required so that this can be built by WritableFactories. */
  public KijiCellWritable() {}

  /**
   * Constructs a KijiCellWritable from an existing KijiCell.
   *
   * @param kijiCell from a KijiRowData.
   */
  public KijiCellWritable(KijiCell kijiCell) {
    mTimestamp = kijiCell.getTimestamp();
    mSchema = kijiCell.getWriterSchema();
    mData = kijiCell.getData();
  }

  /**
   * Constructor for a KijiCellWritable from a Hive representation of a cell.  This is
   * typically a Hive struct containing two fields, one for the timestamp and one for the object.
   *
   * @param timestampedCellObjectInspector StructObjectInspector for this Hive object
   * @param hiveObj representing a struct containing a Hive timestamp and data pair.
   */
  public KijiCellWritable(StructObjectInspector timestampedCellObjectInspector, Object hiveObj) {
    List<Object> timestampedCellFields =
        timestampedCellObjectInspector.getStructFieldsDataAsList(hiveObj);
    if (timestampedCellFields.isEmpty()) {
      LOG.warn("Passed in Hive object is empty.  Returning an empty KijiCellWritable");
      mData = null;
    } else {
      Preconditions.checkState(timestampedCellFields.size() == 2,
          "KijiCellWritable must be created with exactly 2 fields.  Found %s",
          timestampedCellFields.size());
      Timestamp timestampObject = (Timestamp) timestampedCellFields.get(0);
      mTimestamp = timestampObject.getTime();
      mData = timestampedCellFields.get(1);
    }

    StructField dataStructField = timestampedCellObjectInspector.getAllStructFieldRefs().get(1);
    mSchema = AvroTypeAdapter.get().toAvroSchema(dataStructField.getFieldObjectInspector());
  }

  /**
   * @return The timestamp associated with this cell.
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * @return the schema associated with this cell.
   */
  public Schema getSchema() {
    return mSchema;
  }

  /**
   * @return the cell content.
   */
  public Object getData() {
    return mData;
  }

  /**
   * @return if this cell has data in it.
   */
  public boolean hasData() {
    return null != mData;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVLong(out, mTimestamp);
    WritableUtils.writeString(out, mSchema.toString());
    writeData(out, mData, mSchema);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mTimestamp = WritableUtils.readVLong(in);
    String schemaString = WritableUtils.readString(in);
    mSchema = new Schema.Parser().parse(schemaString);
    mData = readData(in, mSchema);
  }

  /**
   * Reads and converts data according to the specified schema.
   *
   * @param out DataOutput to serialize this object into.
   * @param data data to be serialized.
   * @param schema Schema to be used for serializing this data.
   * @throws IOException if there was an error writing.
   */
  private static void writeData(DataOutput out, Object data, Schema schema) throws IOException {
    switch(schema.getType()) {
      case INT:
        Integer intData = (Integer) data;
        WritableUtils.writeVInt(out, intData);
        break;
      case LONG:
        Long longData = (Long) data;
        WritableUtils.writeVLong(out, longData);
        break;
      case DOUBLE:
        Double doubleData = (Double) data;
        DoubleWritable doubleWritable = new DoubleWritable(doubleData);
        doubleWritable.write(out);
        break;
      case ENUM:
      case STRING:
        String stringData = data.toString();
        WritableUtils.writeString(out, stringData);
        break;
      case FLOAT:
        Float floatData = (Float) data;
        FloatWritable floatWritable = new FloatWritable(floatData);
        floatWritable.write(out);
        break;
      case ARRAY:
        List<Object> listData = (List<Object>) data;
        WritableUtils.writeVInt(out, listData.size());
        for (Object listElement : listData) {
          writeData(out, listElement, schema.getElementType());
        }
        break;
      case RECORD:
        IndexedRecord recordData = (IndexedRecord) data;
        WritableUtils.writeVInt(out, schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
          WritableUtils.writeString(out, field.name());
          writeData(out, recordData.get(field.pos()), field.schema());
        }
        break;
      case MAP:
        Map<String, Object> mapData = (Map<String, Object>) data;
        WritableUtils.writeVInt(out, mapData.size());
        for (Map.Entry<String, Object> entry : mapData.entrySet()) {
          WritableUtils.writeString(out, entry.getKey());
          writeData(out, entry.getValue(), schema.getValueType());
        }
        break;
      case UNION:
        final Integer tag = GenericData.get().resolveUnion(schema, data);
        WritableUtils.writeVInt(out, tag);
        Schema unionSubSchema = schema.getTypes().get(tag);
        writeData(out, data, unionSubSchema);
        break;
      case BYTES:
        byte[] bytesData = (byte[]) data;
        WritableUtils.writeCompressedByteArray(out, bytesData);
        break;
      case BOOLEAN:
        Boolean booleanData = (Boolean) data;
        BooleanWritable booleanWritable = new BooleanWritable(booleanData);
        booleanWritable.write(out);
        break;
      case NULL:
        // Don't need to write anything for null.
        break;
      case FIXED:
      default:
        throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
    }
  }

  /**
   * Reads and converts data according to the specified schema.
   *
   * @param in DataInput to deserialize this object from.
   * @param schema Schema to be used for deserializing this data.
   * @return the data read and converted according to the schema.
   * @throws IOException if there was an error reading.
   */
  private static Object readData(DataInput in, Schema schema) throws IOException {
    switch (schema.getType()) {
      case INT:
        Integer intData = WritableUtils.readVInt(in);
        return intData;
      case LONG:
        Long longData = WritableUtils.readVLong(in);
        return longData;
      case DOUBLE:
        DoubleWritable doubleWritable =
            (DoubleWritable) WritableFactories.newInstance(DoubleWritable.class);
        doubleWritable.readFields(in);
        return doubleWritable.get();
      case ENUM:
      case STRING:
        String stringData = WritableUtils.readString(in);
        return stringData;
      case FLOAT:
        FloatWritable floatWritable =
            (FloatWritable) WritableFactories.newInstance(FloatWritable.class);
        floatWritable.readFields(in);
        return floatWritable.get();
      case ARRAY:
        List<Object> listData = Lists.newArrayList();
        Integer numElements = WritableUtils.readVInt(in);
        for (int c=0; c < numElements; c++) {
          Object listElement = readData(in, schema.getElementType());
          listData.add(listElement);
        }
        return listData;
      case RECORD:
        GenericRecord recordData = new GenericData.Record(schema);
        Integer numFields = WritableUtils.readVInt(in);
        for (int c=0; c < numFields; c++) {
          String fieldName = WritableUtils.readString(in);
          Object fieldData = readData(in, schema.getField(fieldName).schema());
          recordData.put(fieldName, fieldData);
        }
        return recordData;
      case MAP:
        Map<String, Object> mapData = Maps.newHashMap();
        Integer numEntries = WritableUtils.readVInt(in);
        for (int c=0; c < numEntries; c++) {
          String key = WritableUtils.readString(in);
          Object value = readData(in, schema.getValueType());
          mapData.put(key, value);
        }
        return mapData;
      case UNION:
        Integer tag = WritableUtils.readVInt(in);
        Schema unionSubSchema = schema.getTypes().get(tag);
        Object unionData = readData(in, unionSubSchema);
        return unionData;
      case BYTES:
        byte[] bytesData = WritableUtils.readCompressedByteArray(in);
        return bytesData;
      case BOOLEAN:
        BooleanWritable booleanWritable =
            (BooleanWritable) WritableFactories.newInstance(BooleanWritable.class);
        booleanWritable.readFields(in);
        return booleanWritable.get();
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
    }
  }
}
