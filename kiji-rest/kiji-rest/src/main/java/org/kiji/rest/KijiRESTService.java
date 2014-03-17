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
import java.util.Iterator;
import java.util.Set;

import javax.ws.rs.ext.ExceptionMapper;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Lookups;
import org.kiji.rest.health.KijiClientHealthCheck;
import org.kiji.rest.plugins.KijiRestPlugin;
import org.kiji.rest.resources.RefreshInstancesTask;
import org.kiji.schema.KijiURI;

/**
 * Service to provide REST access to a list of Kiji instances.
 * The configuration is parametrized by a file that contains the cluster
 * address and the list of instances.
 */
public class KijiRESTService extends Service<KijiRESTConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(KijiRESTService.class);

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

    // Initialize plugins.
    for (KijiRestPlugin plugin : Lookups.get(KijiRestPlugin.class)) {
      LOG.info("Initializing plugin {}", plugin.getClass());
      plugin.initialize(bootstrap);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IOException when instance in configuration can not be opened and closed.
   */
  @Override
  public final void run(final KijiRESTConfiguration configuration, final Environment environment)
      throws IOException {
    final KijiURI clusterURI = KijiURI.newBuilder(configuration.getClusterURI()).build();

    final ManagedKijiClient managedKijiClient = new ManagedKijiClient(clusterURI);
    environment.manage(managedKijiClient);

    // Setup the health checker for the KijiClient
    environment.addHealthCheck(new KijiClientHealthCheck(managedKijiClient));

    // Remove all built-in Dropwizard ExceptionHandler.
    // Always depend on custom ones.
    // Inspired by Jeremy Whitlock's suggestion on thoughtspark.org.
    Set<Object> jerseyResources = environment.getJerseyResourceConfig().getSingletons();
    Iterator<Object> jerseyResourcesIterator = jerseyResources.iterator();
    while (jerseyResourcesIterator.hasNext()) {
      Object jerseyResource = jerseyResourcesIterator.next();
      if (jerseyResource instanceof ExceptionMapper
          && jerseyResource.getClass().getName().startsWith("com.yammer.dropwizard.jersey")) {
        jerseyResourcesIterator.remove();
      }
    }

    // Load admin task to manually refresh instances.
    environment.addTask(new RefreshInstancesTask(managedKijiClient));

    // Load resources.
    for (KijiRestPlugin plugin : Lookups.get(KijiRestPlugin.class)) {
      LOG.info("Loading plugin {}", plugin.getClass());
      plugin.install(managedKijiClient, configuration, environment);
    }

    // Allow global CORS filter. CORS off by default.
    if (configuration.getCORS()) {
      environment.addFilter(
          CrossOriginFilter.class,
          configuration.getHttpConfiguration().getRootPath());
      LOG.info("Global cross-origin resource sharing is allowed.");
    }
  }
}
