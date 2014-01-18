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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Tests the ManagedKijiClient.
 */
public class TestManagedKijiClient {
  private static final Map<String, ? extends List<String>> INSTANCE_TABLES = ImmutableMap.of(
          "inst1", ImmutableList.of("t_1_1", "t_1_2", "t_1_3"),
          "inst2", ImmutableList.of("t_2_1", "t_2_2", "t_2_3"),
          "inst3", ImmutableList.of("t_3_1", "t_3_2", "t_3_3"));

  private static final Map<String, ? extends List<String>> ALT_INSTANCE_TABLES = ImmutableMap.of(
      "inst4", ImmutableList.of("t_4_1", "t_4_2", "t_4_3"),
      "inst5", ImmutableList.of("t_5_1", "t_5_2", "t_5_3"),
      "inst6", ImmutableList.of("t_6_1", "t_6_2", "t_6_3"));

  private ManagedKijiClient mKijiClient;
  private ManagedKijiClient mKijiClientForRefreshes;

  /**
   * Build fake instances and tables from a given a map of instance names to lists of table names.
   *
   * @param instancesSpecMap map of instance names to lists of table names.
   * @return set of fake instances with tables.
   * @throws Exception if instances or tables can not be build.
   */
  public static Set<KijiURI> buildInstances(
      final Map<String, ? extends List<String>> instancesSpecMap) throws Exception {
    List<Kiji> kijis = Lists.newArrayList();
    TableLayoutDesc layout = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");
    for (String instance : instancesSpecMap.keySet()) {
      InstanceBuilder kiji = new InstanceBuilder(instance);
      for (String table : instancesSpecMap.get(instance)) {
        layout.setName(table);
        kiji.withTable(layout);
      }
      kijis.add(kiji.build());
    }

    Set<KijiURI> uris = Sets.newHashSet();
    for (Kiji kiji : kijis) {
      uris.add(kiji.getURI());
    }
    return uris;
  }

  @Before
  public void setUp() throws Exception {
    mKijiClient = new ManagedKijiClient(buildInstances(INSTANCE_TABLES));
    mKijiClient.start();
    mKijiClientForRefreshes = new ManagedKijiClient(buildInstances(INSTANCE_TABLES));
    mKijiClientForRefreshes.start();
  }

  @After
  public void tearDown() throws Exception {
    mKijiClient.stop();
  }

  @Test
  public void testHasAllInstances() throws Exception {
    for (String uri : mKijiClient.getInstances()) {
      assertTrue(INSTANCE_TABLES.keySet().contains(uri));
    }
  }

  @Test
  public void testKeepsKijisCached() throws Exception {
    for (String instance : INSTANCE_TABLES.keySet()) {
      assertTrue(mKijiClient.getKiji(instance) == mKijiClient.getKiji(instance));
    }
  }

  @Test
  public void testKeepsSchemaTablesCached() throws Exception {
    for (String instance : INSTANCE_TABLES.keySet()) {
      assertTrue(mKijiClient.getKijiSchemaTable(instance)
          == mKijiClient.getKijiSchemaTable(instance));
    }
  }

  @Test
  public void testKeepsKijiTablesCached() throws Exception {
    for (String instance : INSTANCE_TABLES.keySet()) {
      for (String table : INSTANCE_TABLES.get(instance)) {
        assertTrue(mKijiClient.getKijiTable(instance, table)
            == mKijiClient.getKijiTable(instance, table));
      }
    }
  }

  @Test
  public void testKeepsFreshenersCached() throws Exception {
    for (String instance : INSTANCE_TABLES.keySet()) {
      for (String table : INSTANCE_TABLES.get(instance)) {
        assertTrue(mKijiClient.getFreshKijiTableReader(instance, table)
            == mKijiClient.getFreshKijiTableReader(instance, table));
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
      mKijiClient.getKijiTable(INSTANCE_TABLES.keySet().iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetFreshenerInvalidInstanceForbidden() throws Exception {
    try {
      mKijiClient.getFreshKijiTableReader("foo", "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test(expected = WebApplicationException.class)
  public void testGetFreshenerInvalidTableNotFound() throws Exception {
    try {
      mKijiClient.getFreshKijiTableReader(INSTANCE_TABLES.keySet().iterator().next(), "bar");
    } catch (WebApplicationException e) {
      assertEquals(Response.Status.NOT_FOUND.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test
  public void testShouldRefreshInstances() throws Exception {
    // Originally contains instances from INSTANCE_TABLES.
    for (String uri : mKijiClientForRefreshes.getInstances()) {
      assertTrue(INSTANCE_TABLES.keySet().contains(uri));
    }
    // Refresh.
    mKijiClientForRefreshes.refreshInstances(buildInstances(ALT_INSTANCE_TABLES));
    // Now contains no instances from INSTANCE_TABLES.
    for (String uri : mKijiClientForRefreshes.getInstances()) {
      assertFalse(INSTANCE_TABLES.keySet().contains(uri));
    }
    // Contains instances from ALT_INSTANCE_TABLES.
    for (String uri : mKijiClientForRefreshes.getInstances()) {
      assertTrue(ALT_INSTANCE_TABLES.keySet().contains(uri));
    }
    // Keeps kijis cached.
    for (String instance : ALT_INSTANCE_TABLES.keySet()) {
      assertTrue(mKijiClientForRefreshes.getKiji(instance)
          == mKijiClientForRefreshes.getKiji(instance));
    }
    // Keeps schema tables cached.
    for (String instance : ALT_INSTANCE_TABLES.keySet()) {
      assertTrue(mKijiClientForRefreshes.getKijiSchemaTable(instance)
          == mKijiClientForRefreshes.getKijiSchemaTable(instance));
    }
    // Keeps tables cached.
    for (String instance : ALT_INSTANCE_TABLES.keySet()) {
      for (String table : ALT_INSTANCE_TABLES.get(instance)) {
        assertTrue(mKijiClientForRefreshes.getKijiTable(instance, table)
            == mKijiClientForRefreshes.getKijiTable(instance, table));
      }
    }
    // Keeps fresheners cached.
    for (String instance : ALT_INSTANCE_TABLES.keySet()) {
      for (String table : ALT_INSTANCE_TABLES.get(instance)) {
        assertTrue(mKijiClientForRefreshes.getFreshKijiTableReader(instance, table)
            == mKijiClientForRefreshes.getFreshKijiTableReader(instance, table));
      }
    }
  }
}
