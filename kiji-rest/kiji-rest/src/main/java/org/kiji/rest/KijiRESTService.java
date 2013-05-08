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
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Sets;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;

import org.kiji.rest.core.WebAppExceptionMapper;
import org.kiji.rest.health.InstanceHealthCheck;
import org.kiji.rest.resources.EntityIdResource;
import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.InstancesResource;
import org.kiji.rest.resources.KijiRESTResource;
import org.kiji.rest.resources.RowResource;
import org.kiji.rest.resources.RowsResource;
import org.kiji.rest.resources.TableResource;
import org.kiji.rest.resources.TablesResource;
import org.kiji.rest.serializers.AvroToJsonSerializer;
import org.kiji.rest.serializers.Utf8ToJsonSerializer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

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
    registerSerializers(bootstrap.getObjectMapperFactory());
  }

  public static final void registerSerializers(ObjectMapperFactory mapperFactory) {
    //Need to add a module to convert btw Avro's specific types and JSON. The default
    //mapping seems to throw an exception.
    SimpleModule module = new SimpleModule("KijiRestModule", new Version(1, 0, 0, null,
        "org.kiji.rest", "avroToJson"));
    module.addSerializer(new AvroToJsonSerializer());
    module.addSerializer(new Utf8ToJsonSerializer());
    mapperFactory.registerModule(module);
  }

  /** {@inheritDoc}
   * @throws IOException when instance in configuration can not be opened and closed.
   */
  @Override
  public final void run(final KijiRESTConfiguration configuration, final Environment environment)
      throws IOException {
    final List<String> instanceStrings = configuration.getInstances();

    final Set<KijiURI> instances = Sets.newHashSet();

    // Load health checks for the visible instances.
    final KijiURI clusterURI = KijiURI.newBuilder(configuration.getClusterURI()).build();
    for (String instance : instanceStrings) {
      final KijiURI instanceURI = KijiURI.newBuilder(clusterURI).withInstanceName(instance).build();
      // Check existence of instance by opening and closing.
      final Kiji kiji = Kiji.Factory.open(instanceURI);
      kiji.release();
      instances.add(instanceURI);
      environment.addHealthCheck(new InstanceHealthCheck(instanceURI));
    }
    //Add exception mappers to print better exception messages to the client than what
    //Dropwizard does by default.
    environment.addProvider(new WebAppExceptionMapper());

    // Load resources.
    environment.addResource(new KijiRESTResource());
    environment.addResource(new InstancesResource(clusterURI, instances));
    environment.addResource(new InstanceResource(clusterURI, instances));
    environment.addResource(new TableResource(clusterURI, instances));
    environment.addResource(new TablesResource(clusterURI, instances));
    environment.addResource(new RowsResource(clusterURI, instances,
        environment.getObjectMapperFactory().build()));
    environment.addResource(new RowResource(clusterURI, instances));
    environment.addResource(new EntityIdResource(clusterURI, instances));
  }
}
