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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.Test;

import org.kiji.schema.KijiDataRequest;

public class TestKijiRowExpression {
  private static final TypeInfo UNVALIDATED_TYPE_INFO = null;

  @Test
  public void testFamilyExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());

    final KijiDataRequest.Column column = kijiDataRequest.getColumns().iterator().next();
    assertEquals("family", column.getFamily());
    assertNull(column.getQualifier());
  }

  @Test
  public void testFamilySpecificExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family[0]", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());

    final KijiDataRequest.Column column = kijiDataRequest.getColumns().iterator().next();
    assertEquals("family", column.getFamily());
    assertNull(column.getQualifier());
  }

  @Test
  public void testFamilyQualifierExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
  }

  @Test
  public void testFamilyQualifierSpecificExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[0]", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
  }

  @Test
  public void testFamilyQualifierFieldExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[0].field", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
  }

  @Test
  public void testFamilyQualifierTimestampExpression() {
    final KijiRowExpression kijiRowExpression =
        new KijiRowExpression("family:qualifier[0].timestamp", UNVALIDATED_TYPE_INFO);
    final KijiDataRequest kijiDataRequest = kijiRowExpression.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("family", "qualifier"));
  }
}
