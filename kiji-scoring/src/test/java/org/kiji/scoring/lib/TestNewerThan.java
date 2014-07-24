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

package org.kiji.scoring.lib;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.impl.InternalFreshenerContext;
import org.kiji.scoring.impl.NullCounterManager;

/**
 * Test the behavior of the stock NewerThan KijiFreshnessPolicy.
 */
public final class TestNewerThan extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestNewerThan.class);

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;
  private FreshKijiTableReader mFreshReader;

  @Before
  public void setupTestNewerThan() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(5L, "foo-val")
                    .withQualifier("visits").withValue(1L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "bar-val")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
    mFreshReader = FreshKijiTableReader.Builder.create()
        .withTable(mTable)
        .withTimeout(1000)
        .build();
  }

  @After
  public void teardownTestNewerThan() throws Exception {
    mReader.close();
    mFreshReader.close();
    mTable.release();
    mKiji.release();
  }

  @Test
  public void testIsFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final KijiRowData rowData = mReader.get(eid, request);
    final Map<String, String> parameters = Maps.newHashMap();
    parameters.put(NewerThan.NEWER_THAN_KEY, String.valueOf(1));
    final FreshenerContext context1 = InternalFreshenerContext.create(
        KijiColumnName.create("info", "name"),
        parameters,
        NullCounterManager.get());
    final NewerThan policy = new NewerThan();
    policy.setup(context1);

    assertTrue(policy.isFresh(rowData, context1));

    parameters.put(NewerThan.NEWER_THAN_KEY, String.valueOf(10));
    final FreshenerContext context10 = InternalFreshenerContext.create(
        KijiColumnName.create("info", "name"),
        parameters,
        NullCounterManager.get());

    policy.setup(context10);
    assertFalse(policy.isFresh(rowData, context10));
  }
}
