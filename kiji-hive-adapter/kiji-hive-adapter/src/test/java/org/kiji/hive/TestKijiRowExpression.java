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

import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import org.kiji.schema.KijiDataRequest;

/**
 * Tests for parsing of KijiRowExpressions into the corresponding KijiDataRequests.
 */
public class TestKijiRowExpression {
  @Test
  public void testFamilyAllValuesExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family", TypeInfos.FAMILY_MAP_ALL_VALUES);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());

    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testFamilyFlatValueExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family[0]", TypeInfos.FAMILY_MAP_FLAT_VALUE);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());

    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertFalse(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnAllValuesExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier", TypeInfos.COLUMN_ALL_VALUES);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));

    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnFlatValueExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[0]", TypeInfos.COLUMN_FLAT_VALUE);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));

    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnFlatValueFieldExpression() {
    // This expression returns the struct type, and then Hive extracts the relevant part
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[0].value", TypeInfos.COLUMN_FLAT_VALUE);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
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
        new KijiRowExpression("family:qualifier[0].ts", TypeInfos.COLUMN_FLAT_VALUE);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(1,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testDefaultVersions() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier", TypeInfos.COLUMN_ALL_VALUES);
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertTrue(column.getColumnName().isFullyQualified());
      assertEquals(
          HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnSpecificFlatValueExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[5]", TypeInfos.COLUMN_FLAT_VALUE);
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertEquals(6,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testColumnOldestFlatValueExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[-1]", TypeInfos.COLUMN_FLAT_VALUE);
    KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
    for (KijiDataRequest.Column column : kijiDataRequest.getColumns()) {
      assertEquals(HConstants.ALL_VERSIONS,
          kijiDataRequest.getColumn(column.getFamily(), column.getQualifier()).getMaxVersions());
    }
  }

  @Test
  public void testInvalidIndex() {
    try {
      final KijiRowExpression kijiRowExpression =
          new KijiRowExpression("family:qualifier[-2].timestamp", TypeInfos.COLUMN_FLAT_VALUE);
      fail("Should fail with a RuntimeException");
    } catch (RuntimeException re) {
      assertEquals("Invalid index(must be >= -1): -2", re.getMessage());
    }
  }
}
