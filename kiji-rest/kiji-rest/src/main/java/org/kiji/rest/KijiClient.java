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

import java.util.Collection;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Interface for Kiji clients that are utilized by KijiREST resources.
 */
public interface KijiClient {
  /**
   * Gets a Kiji object for the specified instance.  Client is responsible for releasing the
   * Kiji instance when done.
   *
   * @param instance of the Kiji to request.
   * @return Kiji object
   * @throws javax.ws.rs.WebApplicationException if there is an error getting the instance OR
   *    if the instance requested is unavailable for handling via REST.
   */
  Kiji getKiji(String instance);

  /** @return a collection of instances served by this client. */
  Collection<KijiURI> getInstances();

  /**
   * Gets a Kiji table.  Caller is responsible for releasing the table when done.
   *
   * @param instance in which this table resides
   * @param table name of the requested table
   * @return KijiTable object
   * @throws javax.ws.rs.WebApplicationException if there is an error.
   */
  KijiTable getKijiTable(String instance, String table);
}
