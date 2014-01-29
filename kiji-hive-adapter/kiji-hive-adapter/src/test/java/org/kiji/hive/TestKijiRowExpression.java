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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.hive.io.KijiCellWritable;
import org.kiji.hive.io.KijiRowDataWritable;
import org.kiji.hive.utils.HiveTypes.HiveList;
import org.kiji.hive.utils.HiveTypes.HiveMap;
import org.kiji.hive.utils.HiveTypes.HiveStruct;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Tests for parsing and evaluation of KijiRowExpressions.
 */
public class TestKijiRowExpression extends KijiClientTest {
  private EntityId mEntityId;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  // ObjectInspectors to be created once and used in the tests.qgqq
  private ObjectInspector mColumnFlatValueObjectInspector;
  private ObjectInspector mColumnAllValuesObjectInspector;
  private ObjectInspector mFamilyFlatValueObjectInspector;
  private ObjectInspector mFamilyAllValuesObjectInspector;

  @Before
  public final void setupKijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    mTable = mKiji.openTable("row_data_test_table");
    mEntityId = mTable.getEntityId("row1");
    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      // Map type column family
      writer.put(mEntityId, "map", "qualA", 1L, 5);
      writer.put(mEntityId, "map", "qualA", 2L, 6);
      writer.put(mEntityId, "map", "qualA", 3L, 7);
      writer.put(mEntityId, "map", "qualB", 2L, 8);

      // Regular column family
      writer.put(mEntityId, "family", "qual0", 1L, "a");
      writer.put(mEntityId, "family", "qual0", 2L, "b");
      writer.put(mEntityId, "family", "qual0", 3L, "c");
      writer.put(mEntityId, "family", "qual1", 2L, "d");
    } finally {
      writer.close();
    }
    mReader = mTable.openTableReader();

    // Create a ObjectInspector for timeseries data that we expect to get from Hive.
    List<String> columnNames = Lists.newArrayList("timestamp", "value");
    List<ObjectInspector> objectInspectors = Lists.newArrayList();
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
    objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    // Setup relevant ObjectInspectors

    // STRUCT<TIMESTAMP, cell>
    mColumnFlatValueObjectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            columnNames,
            objectInspectors);

    // ARRAY<STRUCT<TIMESTAMP, cell>>
    mColumnAllValuesObjectInspector =
        ObjectInspectorFactory.getStandardListObjectInspector(
            mColumnFlatValueObjectInspector);

    // MAP<STRING, STRUCT<TIMESTAMP, cell>>
    mFamilyFlatValueObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            mColumnFlatValueObjectInspector);

    // MAP<STRING, ARRAY<STRUCT<TIMESTAMP, cell>>>
    mFamilyAllValuesObjectInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            mColumnAllValuesObjectInspector);
  }

  @After
  public final void teardownKijiInstance() throws IOException {
    mReader.close();
    mTable.release();
    mKiji.deleteTable("row_data_test_table");
  }

  @Test
  public void testEntityIdExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression(":entity_id", TypeInfos.ENTITY_ID);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(0, kijiDataRequest.getColumns().size());

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    String result = (String) kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(mEntityId.toShellString(), result);
  }

  @Test
  public void testFamilyAllValuesExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("map", TypeInfos.FAMILY_MAP_ALL_VALUES);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveMap resultMap = (HiveMap) kijiRowExpression.evaluate(kijiRowDataWritable);
    HiveList qualAList = (HiveList) resultMap.get("qualA");
    assertEquals(3, qualAList.size());

    HiveList qualBList = (HiveList) resultMap.get("qualB");
    assertEquals(1, qualBList.size());
  }

  @Test
  public void testFamilyAllValuesExpressionWrites() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("map", TypeInfos.FAMILY_MAP_ALL_VALUES);
    List<Object> hiveCellA1 = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCellA2 = createHiveTimestampedCell(5L, "e");
    List<Object> hiveCellsA = Lists.newArrayList();
    hiveCellsA.add(hiveCellA1);
    hiveCellsA.add(hiveCellA2);
    List<Object> hiveCellB1 = createHiveTimestampedCell(4L, "d");
    List<Object> hiveCellB2 = createHiveTimestampedCell(6L, "f");
    List<Object> hiveCellsB = Lists.newArrayList();
    hiveCellsB.add(hiveCellB1);
    hiveCellsB.add(hiveCellB2);

    Map<String, Object> hiveData = Maps.newHashMap();
    hiveData.put("qualA", hiveCellsA);
    hiveData.put("qualB", hiveCellsB);

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> expressionData =
        kijiRowExpression.convertToTimeSeries(mFamilyAllValuesObjectInspector, hiveData);

    KijiColumnName kijiColumnNameA = new KijiColumnName("map", "qualA");
    assertTrue(expressionData.containsKey(kijiColumnNameA));
    NavigableMap<Long, KijiCellWritable> timeseriesA = expressionData.get(kijiColumnNameA);
    validateCell(hiveCellA1, timeseriesA);
    validateCell(hiveCellA2, timeseriesA);

    KijiColumnName kijiColumnNameB = new KijiColumnName("map", "qualB");
    assertTrue(expressionData.containsKey(kijiColumnNameB));
    NavigableMap<Long, KijiCellWritable> timeseriesB = expressionData.get(kijiColumnNameB);
    validateCell(hiveCellB1, timeseriesB);
    validateCell(hiveCellB2, timeseriesB);

  }

  @Test
  public void testFamilyFlatValueExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("map[0]", TypeInfos.FAMILY_MAP_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveMap resultMap = (HiveMap) kijiRowExpression.evaluate(kijiRowDataWritable);
    HiveStruct qualA = (HiveStruct) resultMap.get("qualA");
    assertEquals(new Date(3L), qualA.get(0));
    assertEquals(7, qualA.get(1));

    HiveStruct qualB = (HiveStruct) resultMap.get("qualB");
    assertEquals(new Date(2L), qualB.get(0));
    assertEquals(8, qualB.get(1));
  }

  @Test
  public void testFamilyFlatValueExpressionWrites() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("map[0]", TypeInfos.FAMILY_MAP_FLAT_VALUE);
    List<Object> hiveCellA = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCellB = createHiveTimestampedCell(4L, "d");

    Map<String, Object> hiveData = Maps.newHashMap();
    hiveData.put("qualA", hiveCellA);
    hiveData.put("qualB", hiveCellB);

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> expressionData =
        kijiRowExpression.convertToTimeSeries(mFamilyFlatValueObjectInspector, hiveData);

    KijiColumnName kijiColumnNameA = new KijiColumnName("map", "qualA");
    assertTrue(expressionData.containsKey(kijiColumnNameA));
    NavigableMap<Long, KijiCellWritable> timeseriesA = expressionData.get(kijiColumnNameA);
    validateCell(hiveCellA, timeseriesA);

    KijiColumnName kijiColumnNameB = new KijiColumnName("map", "qualB");
    assertTrue(expressionData.containsKey(kijiColumnNameB));
    NavigableMap<Long, KijiCellWritable> timeseriesB = expressionData.get(kijiColumnNameB);
    validateCell(hiveCellB, timeseriesB);
  }

  @Test
  public void testColumnAllValuesExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveList resultList = (HiveList) kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(3, resultList.size());
  }

  @Test
  public void testColumnAllValueExpressionWrites() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);
    List<Object> hiveCell1 = createHiveTimestampedCell(3L, "c");
    List<Object> hiveCell2 = createHiveTimestampedCell(4L, "d");

    List<Object> hiveData = Lists.newArrayList();
    hiveData.add(hiveCell1);
    hiveData.add(hiveCell2);

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> expressionData =
        kijiRowExpression.convertToTimeSeries(mColumnAllValuesObjectInspector, hiveData);

    KijiColumnName kijiColumnName = new KijiColumnName("family", "qual0");
    assertTrue(expressionData.containsKey(kijiColumnName));
    NavigableMap<Long, KijiCellWritable> timeseries = expressionData.get(kijiColumnName);
    validateCell(hiveCell1, timeseries);
    validateCell(hiveCell2, timeseries);
  }

  @Test
  public void testColumnFlatValueExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[0]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(new Date(3L), resultStruct.get(0));
    assertEquals("c", resultStruct.get(1));
  }

  @Test
  public void testColumnFlatValueExpressionWrites() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[0]", TypeInfos.COLUMN_FLAT_VALUE);

    List<Object> hiveCell = createHiveTimestampedCell(3L, "c");

    Map<KijiColumnName, NavigableMap<Long, KijiCellWritable>> expressionData =
        kijiRowExpression.convertToTimeSeries(mColumnFlatValueObjectInspector, hiveCell);
    KijiColumnName kijiColumnName = new KijiColumnName("family", "qual0");
    assertTrue(expressionData.containsKey(kijiColumnName));
    NavigableMap<Long, KijiCellWritable> timeseries = expressionData.get(kijiColumnName);
    validateCell(hiveCell, timeseries);
  }

  @Test
  public void testColumnFlatValueFieldExpression() {
    // This expression returns the struct type, and then Hive extracts the relevant part
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[0].value", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier())
              .getMaxVersions());
    }
  }

  @Test
  public void testColumnFlatValueTimestampExpression() {
    // This expression returns the struct type, and then Hive extracts the relevant part
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[0].ts", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testDefaultVersions() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0", TypeInfos.COLUMN_ALL_VALUES);

    // Test that the KijiDataRequest was constructed correctly
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(
          HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnSpecificFlatValueExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[1]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertEquals(2,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(new Date(2L), resultStruct.get(0));
    assertEquals("b", resultStruct.get(1));
  }

  @Test
  public void testColumnOldestFlatValueExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qual0[-1]", TypeInfos.COLUMN_FLAT_VALUE);

    // Test that the KijiDataRequest was constructed correctly
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qual0"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(new Date(1L), resultStruct.get(0));
    assertEquals("a", resultStruct.get(1));
  }

  @Test
  public void testInvalidIndex() {
    try {
      final KijiRowExpression kijiRowExpression =
          new KijiRowExpression("family:qual0[-2].timestamp", TypeInfos.COLUMN_FLAT_VALUE);
      fail("Should fail with a RuntimeException");
    } catch (RuntimeException re) {
      assertEquals("Invalid index(must be >= -1): -2", re.getMessage());
    }
  }

  /**
   * Helper method to create Hive timestampped cells from a timestamp and data object.
   *
   * @param timestamp of the Hive cell as a Long.
   * @param data Object representing the data
   * @return a List(which is a Hive struct) representing what Hive might pass into the Hive Adapter.
   */
  private static List<Object> createHiveTimestampedCell(Long timestamp, Object data) {
    List<Object> hiveTimestamppedCell = Lists.newArrayList();
    hiveTimestamppedCell.add(new Timestamp(timestamp));
    hiveTimestamppedCell.add(data);
    return hiveTimestamppedCell;
  }

  /**
   * Helper method to validate that a particular Hive cell is the Writable time series.
   *
   * @param expectedCell List containing a timeseries and data object.
   * @param actual timeseries of KijiCellWritables.
   */
  private static void validateCell(List<Object> expectedCell,
                                   NavigableMap<Long, KijiCellWritable> actual) {
    assertEquals(2, expectedCell.size());
    long timestamp = ((Timestamp) expectedCell.get(0)).getTime();
    Object data = expectedCell.get(1);

    assertTrue(actual.containsKey(timestamp));
    KijiCellWritable kijiCellWritable = actual.get(timestamp);
    assertEquals((long) timestamp, kijiCellWritable.getTimestamp());
    assertEquals(data, kijiCellWritable.getData());
  }
}
