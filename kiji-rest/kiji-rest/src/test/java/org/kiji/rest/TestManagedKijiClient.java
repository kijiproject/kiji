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

package org.kiji.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Tests the ManagedKijiClient.
 */
public class TestManagedKijiClient extends KijiClientTest {
  private static final Set<String> INSTANCE_TABLES =  ImmutableSet.of("t_1", "t_2", "t_3");

  private KijiURI mClusterURI;
  private Set<String> mInstanceNames;
  private ManagedKijiClient mKijiClient;

  /**
   * Install tables to the supplied Kiji instances.
   *
   * @param kijis to have tables installed in.
   */
  private static void installTables(Set<Kiji> kijis) throws IOException {
    TableLayoutDesc layout = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");

    for (Kiji kiji : kijis) {
      InstanceBuilder builder = new InstanceBuilder(kiji);
      for (String table : INSTANCE_TABLES) {
        layout.setName(table);
        builder.withTable(layout);
      }
      builder.build();
    }
  }

  @Before
  public void setUp() throws Exception {
    mClusterURI = createTestHBaseURI();

    Set<Kiji> kijis = Sets.newHashSet();
    kijis.add(createTestKiji(mClusterURI));
    kijis.add(createTestKiji(mClusterURI));
    kijis.add(createTestKiji(mClusterURI));
    installTables(kijis);

    ImmutableSet.Builder<String> instances = ImmutableSet.builder();
    for (Kiji kiji : kijis) {
      instances.add(kiji.getURI().getInstance());
    }

    mInstanceNames = instances.build();
    mKijiClient = new ManagedKijiClient(mClusterURI);
    mKijiClient.start();
  }

  @After
  public void tearDown() throws Exception {
    mKijiClient.stop();
  }

  @Test
  public void testHasAllInstances() throws Exception {
    Assert.assertEquals(mInstanceNames, Sets.newHashSet(mKijiClient.getInstances()));
  }

  @Test
  public void testShouldAllowAccessToVisibleInstancesOnly() throws Exception {
    KijiURI clusterURI = createTestHBaseURI();

    Set<Kiji> kijis = Sets.newHashSet();
    // Create two instances with arbitrary names.
    kijis.add(createTestKiji(clusterURI));
    kijis.add(createTestKiji(clusterURI));
    installTables(kijis);

    Set<String> visibleInstances = Sets.newHashSet();
    ImmutableSet.Builder<String> instances = ImmutableSet.builder();
    for (Kiji kiji : kijis) {
      instances.add(kiji.getURI().getInstance());
    }
    final ImmutableSet<String> instanceNames = instances.build();
    // Select only the first instance to be visible.
    visibleInstances.add(instanceNames.iterator().next());

    ManagedKijiClient kijiClient = new ManagedKijiClient(
        clusterURI,
        ManagedKijiClient.DEFAULT_TIMEOUT,
        visibleInstances);
    kijiClient.start();

    try {
      assertEquals(1, kijiClient.getInstances().size());
    } finally {
      kijiClient.stop();
    }
  }

  @Test
  public void testKeepsKijisCached() throws Exception {
    for (String instance : mInstanceNames) {
      assertTrue(mKijiClient.getKiji(instance) == mKijiClient.getKiji(instance));
    }
  }

  @Test
  public void testKeepsKijiTablesCached() throws Exception {
    for (String instance : mInstanceNames) {
      for (String table : INSTANCE_TABLES) {
        assertTrue(mKijiClient.getKijiTable(instance, table)
            == mKijiClient.getKijiTable(instance, table));
      }
    }
  }

  @Test
  public void testKeepsReadersCached() throws Exception {
    for (String instance : mInstanceNames) {
      for (String table : INSTANCE_TABLES) {
        assertTrue(mKijiClient.getKijiTableReader(instance, table)
            == mKijiClient.getKijiTableReader(instance, table));
      }
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetKijiInvalidInstanceForbidden() throws Exception {
    try {
      mKijiClient.getKiji("foo");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetKijiTableInvalidInstanceForbidden() throws Exception {
    try {
      mKijiClient.getKijiTable("foo", "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetKijiTableInvalidTableNotFound() throws Exception {
    try {
      mKijiClient.getKijiTable(mInstanceNames.iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }


  @Test(expected = WebApplicationException.class)
  public void testGetReaderInvalidInstanceForbidden() throws Exception {
    try {
      mKijiClient.getKijiTableReader("foo", "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetReaderInvalidTableNotFound() throws Exception {
    try {
      mKijiClient.getKijiTableReader(mInstanceNames.iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test
  public void testShouldRefreshInstances() throws Exception {
    Kiji kiji = createTestKiji(mClusterURI);
    String instance = kiji.getURI().getInstance();

    Thread.sleep(50); // Give the ZK callback a chance to trigger
    assertTrue(mKijiClient.getInstances().contains(instance));

    deleteTestKiji(kiji);
    Thread.sleep(50); // Give the ZK callback a chance to trigger
    assertFalse(mKijiClient.getInstances().contains(instance));
  }
}
