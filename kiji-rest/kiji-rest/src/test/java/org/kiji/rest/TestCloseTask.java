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

package org.kiji.rest;

import java.io.PrintWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.rest.tasks.CloseTask;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Tests the Close task.
 */
public class TestCloseTask extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCloseTask.class);
  private static final String TABLE_NAME = "test_close_task_table";

  private ManagedKijiClient mKijiClient;
  private CloseTask mCloseTask;
  private Kiji mKiji;

  @Before
  public void setUp() throws Exception {
    final KijiURI clusterURI = createTestHBaseURI();
    // Create the instance
    mKiji = createTestKiji(clusterURI);
      TableLayoutDesc layout =
          KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");
      layout.setName(TABLE_NAME);
      mKiji.createTable(layout);

    mKijiClient = new ManagedKijiClient(clusterURI);
    mKijiClient.start();
    mCloseTask = new CloseTask(mKijiClient);
  }

  @After
  public void tearDown() throws Exception {
    mKijiClient.stop();
  }

  @Test
  public void testCloseInstance() throws Exception {
    final String instanceName = mKiji.getURI().getInstance();
    final Kiji kiji = mKijiClient.getKiji(instanceName);

    Assert.assertEquals(ImmutableList.of(TABLE_NAME), kiji.getTableNames());
    Assert.assertEquals(ImmutableSet.of(instanceName), mKijiClient.getInstances());

    mCloseTask.execute(
        ImmutableMultimap.of(CloseTask.INSTANCE_KEY, instanceName),
        new PrintWriter(System.out));

    // Kiji should be closed at this point
    try {
      kiji.getTableNames();
      Assert.fail("A call to a Kiji method succeeded when it should have failed.");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot get table names in Kiji instance"));
    }
  }

  @Test
  public void testCloseTable() throws Exception {
    final String instanceName = mKiji.getURI().getInstance();
    final Kiji kiji = mKijiClient.getKiji(instanceName);
    final KijiTable table = mKijiClient.getKijiTable(instanceName, TABLE_NAME);
    table.retain().release(); // Will throw if closed

    mCloseTask.execute(
        ImmutableMultimap.of(
            CloseTask.INSTANCE_KEY, instanceName, CloseTask.TABLE_KEY, TABLE_NAME),
        new PrintWriter(System.out));

    // Kiji should still be open
    Assert.assertEquals(ImmutableList.of(TABLE_NAME), kiji.getTableNames());

    // table should be closed at this point
    try {
      table.retain().release(); // Will throw
    } catch (IllegalStateException e) {
      return; // good
    }
    Assert.fail();
  }
}
