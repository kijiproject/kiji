/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.scoring.impl;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.lib.NewerThan;

public class TestMapFamilyFreshening extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestMapFamilyFreshening.class);
  private static final String LAYOUT_PATH = "test-map-family-freshening.json";
  private static final String TABLE_NAME = "test_map_family_freshening";

  private static final String ENTITY = "foo";
  private static final String MAP = "map";
  private static final String QUAL0 = "qual0";
  private static final String QUAL1 = "qual1";
  private static final String INFO = "info";
  private static final String NAME = "name";
  private static final String EMAIL = "email";
  private static final String FOO_VAL = "foo-val";
  private static final String NEW_VAL = "new-val";
  private static final String FOO_NAME = "foo-name";
  private static final String NEW_NAME = "new-name";
  private static final Map<String, String> EMPTY = Maps.newHashMap();

  private static final class TestMapFamilyScoreFunction extends ScoreFunction<String> {
    public KijiDataRequest getDataRequest(
        final FreshenerContext context
    ) throws IOException {
      return KijiDataRequest.empty();
    }

    public TimestampedValue<String> score(
        final KijiRowData dataToScore,
        final FreshenerContext context
    ) throws IOException {
      return TimestampedValue.create("new-val");
    }
  }

  private static final class TestNameScoreFunction extends ScoreFunction<String> {
    public KijiDataRequest getDataRequest(
        final FreshenerContext context
    ) throws IOException {
      return KijiDataRequest.empty();
    }
    public TimestampedValue<String> score(
        final KijiRowData dataToScore,
        final FreshenerContext context
    ) throws IOException {
      return TimestampedValue.create("new-name");
    }
  }

  private KijiTable mTable = null;
  private KijiTableReader mReader = null;
  private EntityId mEid = null;

  @Before
  public void setupTestMapFamilyFreshening() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(LAYOUT_PATH))
            .withRow(ENTITY)
                .withFamily(INFO)
                    .withQualifier(NAME).withValue(10, FOO_NAME)
                .withFamily(MAP)
                    .withQualifier(QUAL0).withValue(10, FOO_VAL)
                    .withQualifier(QUAL1).withValue(5, FOO_VAL)
        .build();
    mTable = getKiji().openTable(TABLE_NAME);
    mReader = mTable.openTableReader();
    mEid = mTable.getEntityId(ENTITY);
  }

  @After
  public void cleanupTestMapFamilyFreshening() throws IOException {
    mReader.close();
    mTable.release();
  }

  private void test(
      final KijiFreshnessPolicy policy,
      final boolean partialFreshening,
      final KijiDataRequest request,
      final String qual0Expected,
      final String qual1Expected,
      final String nameExpected
  ) throws IOException {
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          TABLE_NAME,
          new KijiColumnName(MAP),
          policy,
          new TestMapFamilyScoreFunction(),
          EMPTY,
          false,
          false);
      manager.registerFreshener(
          TABLE_NAME,
          new KijiColumnName(INFO, NAME),
          policy,
          new TestNameScoreFunction(),
          EMPTY,
          false,
          false);
    } finally {
      manager.close();
    }
    final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
        .withTable(mTable)
        .withTimeout(500)
        .withPartialFreshening(partialFreshening)
        .build();
    try {
      freshReader.get(mEid, request);
      final KijiDataRequest complete = KijiDataRequest.builder().addColumns(
          ColumnsDef.create().addFamily(MAP).add(INFO, NAME)
      ).build();
      final KijiRowData data = mReader.get(mEid, complete);
      Assert.assertEquals(qual0Expected, data.getMostRecentValue(MAP, QUAL0).toString());
      Assert.assertEquals(qual1Expected, data.getMostRecentValue(MAP, QUAL1).toString());
      Assert.assertEquals(nameExpected, data.getMostRecentValue(INFO, NAME).toString());
    } finally {
      freshReader.close();
    }
  }

  @Test
  public void testSingleQualifiedColumnStaleWithPartial() throws IOException {
    test(new NewerThan(20), true, KijiDataRequest.create(MAP, QUAL0), NEW_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testSingleQualifiedColumnStaleWithoutPartial() throws IOException {
    test(new NewerThan(20), false, KijiDataRequest.create(MAP, QUAL0), NEW_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testSingleQualifiedColumnFreshWithPartial() throws IOException {
    test(new NewerThan(1), true, KijiDataRequest.create(MAP, QUAL0), FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testSingleQualifiedColumn() throws IOException {
    test(new NewerThan(1), false, KijiDataRequest.create(MAP, QUAL0), FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsStaleWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(20), false, request, NEW_VAL, NEW_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsStaleWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(20), false, request, NEW_VAL, NEW_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsFreshWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(1), false, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsFreshWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(1), false, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testSingleQualifiedColumnWithGroupColumnStaleWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(20), true, request, NEW_VAL, FOO_VAL, NEW_NAME);
  }

  @Test
  public void testSingleQualifiedColumnWithGroupColumnStaleWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(20), false, request, NEW_VAL, FOO_VAL, NEW_NAME);
  }

  @Test
  public void testSingleQualifiedColumnWithGroupColumnFreshWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(1), true, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testSingleQualifiedColumnWithGroupColumnFreshWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(1), false, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsWithGroupColumnStaleWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(20), true, request, NEW_VAL, NEW_VAL, NEW_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsWithGroupColumnStaleWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(20), false, request, NEW_VAL, NEW_VAL, NEW_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsWithGroupColumnFreshWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(1), true, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testTwoQualifiedColumnsWithGroupColumnFreshWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(INFO, NAME).add(MAP, QUAL0)
    ).build();
    test(new NewerThan(1), false, request, FOO_VAL, FOO_VAL, FOO_NAME);
  }

  @Test
  public void testOneFreshOneStaleWithPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(7), true, request, FOO_VAL, NEW_VAL, FOO_NAME);
  }

  @Test
  public void testOneFreshOneStaleWithoutPartial() throws IOException {
    final KijiDataRequest request = KijiDataRequest.builder().addColumns(
        ColumnsDef.create().add(MAP, QUAL0).add(MAP, QUAL1)
    ).build();
    test(new NewerThan(7), false, request, FOO_VAL, NEW_VAL, FOO_NAME);
  }

  @Test
  public void testRequestFamily() throws IOException {
    test(new NewerThan(20), false, KijiDataRequest.create(MAP), FOO_VAL, FOO_VAL, FOO_NAME);
  }
}
