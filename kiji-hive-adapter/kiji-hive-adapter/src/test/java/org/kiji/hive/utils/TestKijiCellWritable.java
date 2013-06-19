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

package org.kiji.hive.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import org.kiji.hive.io.KijiCellWritable;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCell;

public class TestKijiCellWritable {

  private static final Long TIMESTAMP_VALUE = 12345L;
  private static final String STRING_VALUE = "a magic string";
  private static final Integer INT_VALUE = 24601;
  private static final Double DOUBLE_VALUE = 24.56d;
  private static final Long LONG_VALUE = 31336L;
  private static final Float FLOAT_VALUE = 13.37f;
  private static final byte[] BYTES_VALUE = STRING_VALUE.getBytes();
  private static final Boolean BOOLEAN_VALUE = true;

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema LIST_SCHEMA = Schema.createArray(INT_SCHEMA);
  private static final Schema MAP_SCHEMA = Schema.createMap(LONG_SCHEMA);
  private static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
  private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);


  @Test
  public void testIntegerCell() throws IOException {
    final KijiCell<Integer> cell1 =
        new KijiCell<Integer>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Integer>(INT_SCHEMA, INT_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(INT_SCHEMA, cell1Decoded.getSchema());
    assertEquals(INT_VALUE, (Integer) cell1Decoded.getData());
  }

  @Test
  public void testLongCell() throws IOException {
    final KijiCell<Long> cell1 =
        new KijiCell<Long>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Long>(LONG_SCHEMA, LONG_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(LONG_SCHEMA, cell1Decoded.getSchema());
    assertEquals(LONG_VALUE, (Long) cell1Decoded.getData());
  }

  @Test
  public void testDoubleCell() throws IOException {
    final KijiCell<Double> cell1 =
        new KijiCell<Double>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Double>(DOUBLE_SCHEMA, DOUBLE_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(DOUBLE_SCHEMA, cell1Decoded.getSchema());
    assertEquals(DOUBLE_VALUE, (Double) cell1Decoded.getData());
  }

  @Test
  public void testStringCell() throws IOException {
    final KijiCell<String> cell1 =
        new KijiCell<String>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<String>(STRING_SCHEMA, STRING_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(STRING_SCHEMA, cell1Decoded.getSchema());
    assertEquals(STRING_VALUE, (String) cell1Decoded.getData());
  }

  @Test
  public void testFloatCell() throws IOException {
    final KijiCell<Float> cell1 =
        new KijiCell<Float>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Float>(FLOAT_SCHEMA, FLOAT_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(FLOAT_SCHEMA, cell1Decoded.getSchema());
    assertEquals(FLOAT_VALUE, (Float) cell1Decoded.getData());
  }

  @Test
  public void testArrayCell() throws IOException {
    List<Integer> listData = Lists.newArrayList(4, 8, 15, 16, 23, 42);
    final KijiCell<List<Integer>> cell1 =
        new KijiCell<List<Integer>>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<List<Integer>>(LIST_SCHEMA, listData));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(LIST_SCHEMA, cell1Decoded.getSchema());
    List<Integer> decodedListData = (List<Integer>) cell1Decoded.getData();
    for (int c=0; c < listData.size(); c++) {
      assertEquals(listData.get(c), decodedListData.get(c));
    }
  }

  @Test
  public void testRecordCell() throws IOException {
    // Construct the Avro schema for testing
    List<Schema.Field> mFields = Lists.newArrayList(
        new Schema.Field("a", STRING_SCHEMA, null, null),
        new Schema.Field("b", INT_SCHEMA, null, null),
        new Schema.Field("c", DOUBLE_SCHEMA, null, null)
    );
    Schema recordSchema = Schema.createRecord("name", null, null, false);
    recordSchema.setFields(mFields);

    // Construct the record
    GenericRecord recordData = new GenericData.Record(recordSchema);
    recordData.put("a", STRING_VALUE);
    recordData.put("b", INT_VALUE);
    recordData.put("c", DOUBLE_VALUE);
    final KijiCell<GenericRecord> cell1 = new KijiCell<GenericRecord>("family", "qualifier",
        TIMESTAMP_VALUE, new DecodedCell<GenericRecord>(recordSchema, recordData));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(recordSchema, cell1Decoded.getSchema());
    GenericRecord decodedRecord = (GenericRecord) cell1Decoded.getData();
    for (Schema.Field field : mFields) {
      assertEquals(recordData.get(field.pos()), decodedRecord.get(field.pos()));
    }
  }

  @Test
  public void testMapCell() throws IOException {
    Map<String, Long> mapData = Maps.newHashMap();
    final KijiCell<Map<String, Long>> cell1 = new KijiCell<Map<String, Long>>("family",
        "qualifier", TIMESTAMP_VALUE, new DecodedCell<Map<String, Long>>(MAP_SCHEMA, mapData));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(MAP_SCHEMA, cell1Decoded.getSchema());
    Map<String, Long> decodedMapData = (Map<String, Long>) cell1Decoded.getData();
    assertEquals(mapData.size(), decodedMapData.size());
    for (String key : mapData.keySet()) {
      assertEquals(mapData.get(key), decodedMapData.get(key));
    }
  }

  @Test
  public void testPrimitiveUnionCell() throws IOException {
    // Construct the Avro schema for testing
    List<Schema> unionSchemas = Lists.newArrayList(STRING_SCHEMA, INT_SCHEMA, DOUBLE_SCHEMA);
    Schema unionSchema = Schema.createUnion(unionSchemas);

    // Make sure that we can decode a string
    final KijiCell<String> cell1 = new KijiCell<String>("family", "qualifier", TIMESTAMP_VALUE,
        new DecodedCell<String>(unionSchema, STRING_VALUE));
    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(unionSchema, cell1Decoded.getSchema());
    assertEquals(STRING_VALUE, (String) cell1Decoded.getData());

    // Make sure that we can decode an integer
    final KijiCell<Integer> cell2 =
        new KijiCell<Integer>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Integer>(unionSchema, INT_VALUE));
    KijiCellWritable cell2Writable = new KijiCellWritable(cell2);
    byte[] cell2Bytes = ByteWritable.serialize(cell2Writable);
    KijiCellWritable cell2Decoded = ByteWritable.asWritable(cell2Bytes, KijiCellWritable.class);
    assertEquals(TIMESTAMP_VALUE, (Long) cell2Decoded.getTimestamp());
    assertEquals(unionSchema, cell2Decoded.getSchema());
    assertEquals(INT_VALUE, (Integer) cell2Decoded.getData());

    // Make sure that we can decode a double
    final KijiCell<Double> cell3 =
        new KijiCell<Double>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Double>(unionSchema, DOUBLE_VALUE));
    KijiCellWritable cell3Writable = new KijiCellWritable(cell3);
    byte[] cell3Bytes = ByteWritable.serialize(cell3Writable);
    KijiCellWritable cell3Decoded = ByteWritable.asWritable(cell3Bytes, KijiCellWritable.class);
    assertEquals(TIMESTAMP_VALUE, (Long) cell3Decoded.getTimestamp());
    assertEquals(unionSchema, cell3Decoded.getSchema());
    assertEquals(DOUBLE_VALUE, (Double) cell3Decoded.getData());
  }

  @Test
  public void testBytesCell() throws IOException {
    final KijiCell<byte[]> cell1 =
        new KijiCell<byte[]>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<byte[]>(BYTES_SCHEMA, BYTES_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(BYTES_SCHEMA, cell1Decoded.getSchema());
    assertTrue(Arrays.equals(BYTES_VALUE, (byte[]) cell1Decoded.getData()));
  }

  @Test
  public void testBooleanCell() throws IOException {
    final KijiCell<Boolean> cell1 =
        new KijiCell<Boolean>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Boolean>(BOOLEAN_SCHEMA, BOOLEAN_VALUE));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(BOOLEAN_SCHEMA, cell1Decoded.getSchema());
    assertEquals(BOOLEAN_VALUE, (Boolean) cell1Decoded.getData());
  }

  @Test
  public void testNullCell() throws IOException {
    final KijiCell<Void> cell1 =
        new KijiCell<Void>("family", "qualifier", TIMESTAMP_VALUE,
            new DecodedCell<Void>(NULL_SCHEMA, null));

    KijiCellWritable cell1Writable = new KijiCellWritable(cell1);
    byte[] cell1Bytes = ByteWritable.serialize(cell1Writable);
    KijiCellWritable cell1Decoded = ByteWritable.asWritable(cell1Bytes, KijiCellWritable.class);

    assertEquals(TIMESTAMP_VALUE, (Long) cell1Decoded.getTimestamp());
    assertEquals(NULL_SCHEMA, cell1Decoded.getSchema());
    assertEquals(null, cell1Decoded.getData());
  }
}
