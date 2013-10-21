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

  private ManagedKijiClient mKijiClient;

  @Before
  public void setUp() throws Exception {
    List<Kiji> kijis = Lists.newArrayList();
    TableLayoutDesc layout = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");
    for (String instance : INSTANCE_TABLES.keySet()) {
      InstanceBuilder kiji = new InstanceBuilder(instance);
      for (String table : INSTANCE_TABLES.get(instance)) {
        layout.setName(table);
        kiji.withTable(layout);
      }
      kijis.add(kiji.build());
    }

    Set<KijiURI> uris = Sets.newHashSet();
    for (Kiji kiji : kijis) {
      uris.add(kiji.getURI());
    }

    mKijiClient = new ManagedKijiClient(uris);
    mKijiClient.start();
  }

  @After
  public void tearDown() throws Exception {
    mKijiClient.stop();
  }

  @Test
  public void testHasAllInstances() throws Exception {
    for (KijiURI uri : mKijiClient.getInstances()) {
      assertTrue(INSTANCE_TABLES.keySet().contains(uri.getInstance()));
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
}
