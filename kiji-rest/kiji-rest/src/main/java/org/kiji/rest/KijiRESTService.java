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

import java.util.List;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import org.kiji.rest.health.InstanceHealthCheck;
import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.KijiRESTResource;
import org.kiji.rest.resources.TableResource;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURI.KijiURIBuilder;

/**
 * Service to provide REST access to a list of Kiji instances.
 * The configuration is parametrized by a file that contains the cluster
 * address and the list of instances.
 */
public class KijiRESTService extends Service<KijiRESTConfiguration> {
  /**
   * Main method entry point into the KijiREST service.
   *
   * @param args The server token and the path to the YAML configuration file.
   * @throws Exception Prevents the REST service from starting.
   */
  public static void main(final String[] args) throws Exception {
    new KijiRESTService().run(args);
  }

  /** {@inheritDoc} */
  @Override
  public final void initialize(final Bootstrap<KijiRESTConfiguration> bootstrap) {
    bootstrap.setName("kiji-rest");
  }

  /** {@inheritDoc} */
  @Override
  public final void run(final KijiRESTConfiguration configuration, final Environment environment) {
    final List<String> instances = configuration.getInstances();

    // Load health checks for the visible instances.
    final KijiURIBuilder clusterURI = KijiURI.newBuilder(configuration.getClusterURI());
    for (String instance : instances) {
      environment.addHealthCheck(new InstanceHealthCheck(clusterURI
              .withInstanceName(instance)
              .build()));
    }

    // Load resources.
    // TODO Load row-level resources, etc.
    environment.addResource(new KijiRESTResource());
    environment.addResource(new InstanceResource());
    environment.addResource(new TableResource());
  }
}
