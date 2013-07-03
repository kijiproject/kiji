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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Mocked KijiClient for use for testing KijiREST.  Only uses a single instance on fake-hbase.
 */
public class FakeKijiClient implements KijiClient {
  private final Map<String, Kiji> mKijis;

  FakeKijiClient() {
    mKijis = Maps.newHashMap();
  }

  FakeKijiClient(Kiji... kijis) {
    mKijis = Maps.newHashMap();
    for (Kiji kiji : kijis) {
      mKijis.put(kiji.getURI().getInstance(), kiji);
    }
  }

  @Override
  public Kiji getKiji(String instance) {
    // Always returns the fake Kiji that this was initialized with.
    return mKijis.get(instance);
  }

  @Override
  public Collection<KijiURI> getInstances() {
    // Always return a singleton of the fake Kiji that this was initialized with.
    Collection<KijiURI> instances = Sets.newHashSet();
    for (Kiji kiji : mKijis.values()) {
      instances.add(kiji.getURI());
    }

    return instances;
  }

  @Override
  public KijiTable getKijiTable(String instance, String table) {
    try {
      return mKijis.get(instance).openTable(table);
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
