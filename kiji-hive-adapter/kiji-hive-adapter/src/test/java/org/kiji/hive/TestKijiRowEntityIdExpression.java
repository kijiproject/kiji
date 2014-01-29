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

import java.io.IOException;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.hive.io.KijiRowDataWritable;
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
public class TestKijiRowEntityIdExpression extends KijiClientTest {
  private EntityId mEntityId;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public final void setupKijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mKiji = getKiji();
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF));
    mTable = mKiji.openTable("table");
    mEntityId = mTable.getEntityId("dummy", "str1", "str2", 1, 1500L);
    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      // Regular column family
      writer.put(mEntityId, "family", "column", 1L, "a");
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
  public void testEntityIdExpression() throws IOException {
    runEntityIdTest(":entity_id", TypeInfos.ENTITY_ID, "['dummy', 'str1', 'str2', 1, 1500]");
  }

  @Test
  public void testEntityIdStringComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[0]", TypeInfos.ENTITY_ID_STRING_COMPONENT, "dummy");
  }

  @Test
  public void testEntityIdIntComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[3]", TypeInfos.ENTITY_ID_INT_COMPONENT, 1);
  }

  @Test
  public void testEntityIdLongComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[4]", TypeInfos.ENTITY_ID_LONG_COMPONENT, 1500L);
  }

  private void runEntityIdTest(String expression, TypeInfo hiveType, Object expectedResult)
      throws IOException {
    final KijiRowExpression kijiRowExpression = new KijiRowExpression(expression, hiveType);

    // Test that the KijiDataRequest was constructed correctly
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(0, kijiDataRequest.getColumns().size());

    // Test that the data returned from this request is decoded properly
    KijiRowData kijiRowData = mReader.get(mEntityId, kijiDataRequest);
    KijiRowDataWritable kijiRowDataWritable = new KijiRowDataWritable(kijiRowData, mReader);
    Object result = kijiRowExpression.evaluate(kijiRowDataWritable);
    assertEquals(expectedResult, result);
  }
}
