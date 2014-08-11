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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Lookups;
import org.kiji.rest.health.KijiClientHealthCheck;
import org.kiji.rest.plugins.KijiRestPlugin;
import org.kiji.rest.representations.KijiRestEntityId;
import org.kiji.rest.representations.SchemaOption;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.rest.serializers.JsonToKijiRestEntityId;
import org.kiji.rest.serializers.JsonToSchemaOption;
import org.kiji.rest.serializers.KijiRestEntityIdToJson;
import org.kiji.rest.serializers.SchemaOptionToJson;
import org.kiji.rest.serializers.TableLayoutToJsonSerializer;
import org.kiji.rest.serializers.Utf8ToJsonSerializer;
import org.kiji.rest.tasks.CloseTask;
import org.kiji.rest.tasks.RefreshInstancesTask;
import org.kiji.rest.tasks.ShutdownTask;
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
    final ManagedKijiClient managedKijiClient = new ManagedKijiClient(configuration);
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
    // Load admin task to manually close instances and tables.
    environment.addTask(new CloseTask(managedKijiClient));
    // Load admin task to manually shutdown the system.
    environment.addTask(new ShutdownTask(managedKijiClient, configuration));

    // Adds custom serializers.
    registerSerializers(environment.getObjectMapperFactory());

    // Adds exception mappers to print better exception messages to the client than what
    // Dropwizard does by default.
    environment.addProvider(new GeneralExceptionMapper());

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
   * Registers custom serializers with the Jackson ObjectMapper via DropWizard's
   * ObjectMapperFactory. This is used by both the service initialization and the test
   * setup method to ensure consistency between test and production.
   *
   * @param mapperFactory is the ObjectMapperFactory.
   */
  public static void registerSerializers(ObjectMapperFactory mapperFactory) {
    // TODO: Add a module to convert btw Avro's specific types and JSON. The default
    // mapping seems to throw an exception.
    SimpleModule module = new SimpleModule("KijiRestModule", new Version(1, 0, 0, null,
        "org.kiji.rest", "serializers"));
    module.addSerializer(new AvroToJsonStringSerializer());
    module.addSerializer(new Utf8ToJsonSerializer());
    module.addSerializer(new TableLayoutToJsonSerializer());
    module.addSerializer(new SchemaOptionToJson());
    module.addDeserializer(SchemaOption.class, new JsonToSchemaOption());
    module.addSerializer(new KijiRestEntityIdToJson());
    module.addDeserializer(KijiRestEntityId.class, new JsonToKijiRestEntityId());
    mapperFactory.registerModule(module);
  }
}
