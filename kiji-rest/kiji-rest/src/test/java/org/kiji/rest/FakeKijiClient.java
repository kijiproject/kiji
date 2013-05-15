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
import java.util.Collections;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Mocked KijiClient for use for testing KijiREST.  Only uses a single instance on fake-hbase.
 */
public class FakeKijiClient implements KijiClient {
  private final Kiji mKiji;

  FakeKijiClient(Kiji kiji) {
    mKiji = kiji;
  }

  @Override
  public Kiji getKiji(String instance) {
    // Always returns the fake Kiji that this was initialized with.
    return mKiji;
  }

  @Override
  public Collection<KijiURI> getInstances() {
    // Always return a singleton of the fake Kiji that this was initialized with.
    return Collections.singleton(mKiji.getURI());
  }

  @Override
  public KijiTable getKijiTable(String instance, String table) {
    try {
      return mKiji.openTable(table);
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
