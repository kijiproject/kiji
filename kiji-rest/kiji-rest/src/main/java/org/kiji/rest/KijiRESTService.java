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

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.ws.rs.ext.ExceptionMapper;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Lookups;
import org.kiji.rest.InstanceUtil.InstancesMapTo;
import org.kiji.rest.health.InstanceHealthCheck;
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
   * Update instances every hour.
   */
  public static final long INSTANCE_REFRESH_PERIOD_MINUTES = 60; //ms

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

    // Load specified instances and health checks for each.
    final Set<KijiURI> instances = InstanceUtil.getInstances(
        clusterURI,
        new InstancesMapTo<KijiURI>() {
          public KijiURI apply(KijiURI instanceURI) throws IOException {
            InstanceUtil.openAndCloseInstance(instanceURI);
            LOG.info("Loading instance {} upon startup.", instanceURI.toOrderedString());
            environment.addHealthCheck(new InstanceHealthCheck(instanceURI));
            return instanceURI;
          }
        });

    final ManagedKijiClient managedKijiClient = new ManagedKijiClient(instances);
    environment.manage(managedKijiClient);

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

    // Update instances periodically.
    final RefreshInstances instanceRefresher =
        new RefreshInstances(clusterURI, managedKijiClient);
    ScheduledExecutorService scheduler = environment
        .managedScheduledExecutorService("instance_refresh_scheduler", 1);
    scheduler.scheduleAtFixedRate(
        instanceRefresher,
        INSTANCE_REFRESH_PERIOD_MINUTES, // Start a period from now.
        INSTANCE_REFRESH_PERIOD_MINUTES,
        MINUTES);

    // Load admin task to manually update instances.
    environment.addTask(new RefreshInstancesTask(instanceRefresher));

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

  /**
   * A runnable "daemon" class which checks for the latest available Kiji instances
   * and appropriately updates the ManagedKijiClient.
   */
  public static class RefreshInstances implements Runnable {
    private final ManagedKijiClient mKijiClient;
    private final KijiURI mClusterURI;

    /**
     * Construct class with the KijiURI of the cluster and the ManagedKijiClient.
     *
     * @param clusterURI pointing to the HBase/Kiji cluster.
     * @param kijiClient containing cached referenced to instances and table readers.
     */
    public RefreshInstances(final KijiURI clusterURI, final ManagedKijiClient kijiClient) {
      this.mKijiClient = kijiClient;
      this.mClusterURI = clusterURI;
    }

    /**
     * Reload list of instances and update the managed Kiji client with the updated list.
     */
    public void run() {
      synchronized (LOG) {
        try {
          mKijiClient.refreshInstances(InstanceUtil.getInstances(
              mClusterURI,
              new InstancesMapTo<KijiURI>() {
                public KijiURI apply(KijiURI instanceURI) throws IOException {
                  try {
                    InstanceUtil.openAndCloseInstance(instanceURI);
                    LOG.info("Loading instance {} upon refresh.", instanceURI.toOrderedString());
                  } catch (Exception e) {
                    LOG.info("Instance refresh may cause an exception if "
                        + "an instance is currently being installed or deleted.",
                        e);
                  }
                  return instanceURI;
                }
              }));
        } catch (Exception e) {
          LOG.info("Instance refresh may cause an exception if "
              + "an instance is currently being installed or deleted.",
              e);
        }
      }
    }
  }
}
