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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReaderFactory;
import org.kiji.scoring.FreshKijiTableReaderFactory.FreshReaderFactoryType;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.impl.InternalPolicyContext;

/**
 * Test the behavior of the stock ShelfLife KijiFreshnessPolicy.
 */
public class TestShelfLife {
  private static final Logger LOG = LoggerFactory.getLogger(TestNewerThan.class);

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;
  private FreshKijiTableReader mFreshReader;

  @Before
  public void setupTestShelfLife() throws Exception {
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
    mFreshReader = FreshKijiTableReaderFactory.getFactory(FreshReaderFactoryType.LOCAL).
        openReader(mTable, 1000);
  }

  @After
  public void teardownTestShelfLife() throws Exception {
    mReader.close();
    mFreshReader.close();
    mTable.release();
  }

  @Test
  public void testIsFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final KijiRowData rowData = mReader.get(eid, request);
    final PolicyContext context =
        new InternalPolicyContext(request, new KijiColumnName("info", "name"), mKiji.getConf());
    final ShelfLife policy = new ShelfLife();
    policy.deserialize(String.format("{\"shelfLife\":%d}", Long.MAX_VALUE));

    assertTrue(policy.isFresh(rowData, context));

    policy.deserialize(String.format("{\"shelfLife\":%d}", Long.MIN_VALUE));
    assertFalse(policy.isFresh(rowData, context));
  }

  @Test
  public void testSerializeDeserialize() {
    final ShelfLife policy = new ShelfLife(10);
    // Serialize the state of the policy.
    final String state = policy.serialize();
    LOG.info(state);
    // Deserialize the state back into the policy.
    policy.deserialize(state);
    // Serialize the newly stored state from the policy.
    final String loopedState = policy.serialize();
    // Assert that the loop has not mutated the state.
    assertEquals(state, loopedState);
  }
}
