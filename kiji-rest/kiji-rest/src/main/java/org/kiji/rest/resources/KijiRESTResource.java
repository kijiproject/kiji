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

package org.kiji.rest.resources;

import static org.kiji.rest.resources.ResourceConstants.API_ENTRY_POINT;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.yammer.metrics.annotation.Timed;

import org.kiji.rest.core.ContentReturnable;
import org.kiji.rest.core.ElementReturnable;
import org.kiji.rest.core.Returnable;

/**
 * This REST resource interacts with the Kiji cluster.
 * This resource is served whenever requests are made using the following
 * resource identifiers:
 *
 * <ul>
 *   <li>/v1/</li>
 *   <li>/v1/&lt;singleton&gt;.</li>
 * </ul>
 */
@Path(API_ENTRY_POINT)
@Produces(MediaType.APPLICATION_JSON)
public class KijiRESTResource {
  /**
   * The entry point if no resource is identified.
   * @return A default Returnable message.
   */
  @GET
  @Timed
  public Returnable namespace() {
    ContentReturnable version = new ContentReturnable("KijiREST");
    return version;
  }

  /**
   * Singleton resource identifying the version information.
   * @return A Returnable message containing version information.
   */
  @Path("version")
  @GET
  @Timed
  public Returnable version() {
    ContentReturnable version = new ContentReturnable("Version");
    version.add(new ElementReturnable("0.1.0"));
    return version;
  }
}
