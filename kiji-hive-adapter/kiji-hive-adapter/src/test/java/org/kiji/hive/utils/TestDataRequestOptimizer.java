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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Test;

import org.kiji.hive.KijiRowExpression;
import org.kiji.schema.KijiDataRequest;

public class TestDataRequestOptimizer {
  private static final TypeInfo UNVALIDATED_TYPE_INFO = null;

  @Test
  public void testEmpty() throws IOException {
    List<KijiRowExpression> rowExpressionList = Lists.newArrayList();
    KijiDataRequest kijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertTrue(kijiDataRequest.isEmpty());
  }

  @Test
  public void testSingleExpression() throws IOException {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("info:name", UNVALIDATED_TYPE_INFO);
    List<KijiRowExpression> rowExpressionList = Lists.newArrayList(kijiRowExpression);

    KijiDataRequest kijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("info", "name"));
    assertNull(kijiDataRequest.getColumn("info", "address"));
  }

  @Test
  public void testMergeMultipleExpressions() throws IOException {
    final KijiRowExpression nameRowExpression =
        new KijiRowExpression("info:email", UNVALIDATED_TYPE_INFO);
    final KijiRowExpression emailRowExpression =
        new KijiRowExpression("info:name", UNVALIDATED_TYPE_INFO);
    List<KijiRowExpression> rowExpressionList =
        Lists.newArrayList(nameRowExpression, emailRowExpression);

    KijiDataRequest kijiDataRequest = DataRequestOptimizer.getDataRequest(rowExpressionList);
    assertEquals(2, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("info", "name"));
    assertNotNull(kijiDataRequest.getColumn("info", "email"));
    assertNull(kijiDataRequest.getColumn("info", "address"));
  }
}
