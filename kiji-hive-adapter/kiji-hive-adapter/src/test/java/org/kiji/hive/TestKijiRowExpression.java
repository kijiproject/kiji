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
import java.util.Date;

import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.hive.utils.HiveTypes.HiveList;
import org.kiji.hive.utils.HiveTypes.HiveMap;
import org.kiji.hive.utils.HiveTypes.HiveStruct;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
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

  @Before
  public final void setupKijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));
    mTable = mKiji.openTable("table");
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
  }

  @After
  public final void teardownKijiInstance() throws IOException {
    mReader.close();
    mTable.release();
    mKiji.deleteTable("table");
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
    HiveMap resultMap = (HiveMap) kijiRowExpression.evaluate(kijiRowData);
    HiveList qualAList = (HiveList) resultMap.get("qualA");
    assertEquals(3, qualAList.size());

    HiveList qualBList = (HiveList) resultMap.get("qualB");
    assertEquals(1, qualBList.size());
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
    HiveMap resultMap = (HiveMap) kijiRowExpression.evaluate(kijiRowData);
    HiveStruct qualA = (HiveStruct) resultMap.get("qualA");
    assertEquals(new Date(3L), qualA.get(0));
    assertEquals(7, qualA.get(1));

    HiveStruct qualB = (HiveStruct) resultMap.get("qualB");
    assertEquals(new Date(2L), qualB.get(0));
    assertEquals(8, qualB.get(1));
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
    HiveList resultList = (HiveList) kijiRowExpression.evaluate(kijiRowData);
    assertEquals(3, resultList.size());
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
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowData);
    assertEquals(new Date(3L), resultStruct.get(0));
    assertEquals("c", resultStruct.get(1));
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
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowData);
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
    HiveStruct resultStruct = (HiveStruct) kijiRowExpression.evaluate(kijiRowData);
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
}
