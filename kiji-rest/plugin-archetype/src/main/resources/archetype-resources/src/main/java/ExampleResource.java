#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package};

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.yammer.metrics.annotation.Timed;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.rest.KijiClient;
import org.kiji.schema.Kiji;
import org.kiji.schema.avro.TableLayoutDesc;

/**
 * This REST endpoint is the dashboard
 * <p/>
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/&lt;instance&gt;/tables/&lt;table&gt;
 */
@Path("/exampleplugin")
@Produces(MediaType.APPLICATION_JSON)
@ApiAudience.Public
public class ExampleResource {
  private final KijiClient mKijiClient;

  /**
   * Default constructor.
   *
   * @param kijiClient that this should use for connecting to Kiji.
   */
  public ExampleResource(KijiClient kijiClient) {
    mKijiClient = kijiClient;
  }

  /**
   * GETs the plugin landing page
   * @return the plugin landing page
   */
  @GET
  @Timed
  @ApiStability.Evolving
  public String getHelloWorld() {
    return "Hello World!";
  }
}
