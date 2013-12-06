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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.delegation.Lookups;
import org.kiji.rest.health.InstanceHealthCheck;
import org.kiji.rest.plugins.KijiRestPlugin;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.util.ResourceUtils;

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

    // If configuration does not specify visible instances, load all.
    final Set<String> instanceStrings;
    if (null == configuration.getInstances() || configuration.getInstances().isEmpty()) {
      LOG.info("Instances not specified in configuration. Default to loading all instances.");
      instanceStrings = getInstanceNames(clusterURI);
    } else {
      instanceStrings = configuration.getInstances();
    }

    // Load specified instances and health checks for each.
    final Set<KijiURI> instances = Sets.newHashSet();
    for (String instance : instanceStrings) {
      final KijiURI instanceURI = KijiURI.newBuilder(clusterURI).withInstanceName(instance).build();
      // Check existence of instance by opening and closing.
      final Kiji kiji = Kiji.Factory.open(instanceURI);
      kiji.release();
      instances.add(instanceURI);
      LOG.info("Adding instance {}", instanceURI.toOrderedString());
      environment.addHealthCheck(new InstanceHealthCheck(instanceURI));
    }

    ManagedKijiClient kijiClient = new ManagedKijiClient(instances);
    environment.manage(kijiClient);

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

    // Load resources.
    for (KijiRestPlugin plugin : Lookups.get(KijiRestPlugin.class)) {
      LOG.info("Loading plugin {}", plugin.getClass());
      plugin.install(kijiClient, configuration, environment);
    }
  }

  /**
   * Returns a set of instance names.
   *
   * @param hbaseURI URI of the HBase instance to list the content of.
   * @return set of instance names.
   * @throws IOException on I/O error.
   */
  private static Set<String> getInstanceNames(final KijiURI hbaseURI) throws IOException {
    // TODO(SCHEMA-188): Consolidate this logic in a single central place:
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM,
        Joiner.on(",").join(hbaseURI.getZookeeperQuorumOrdered()));
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseURI.getZookeeperClientPort());
    final HBaseAdmin hbaseAdmin =
        HBaseFactory.Provider.get().getHBaseAdminFactory(hbaseURI).create(conf);

    try {
      final Set<String> instanceNames = Sets.newHashSet();
      for (HTableDescriptor hTableDescriptor : hbaseAdmin.listTables()) {
        final String instanceName = parseInstanceName(hTableDescriptor.getNameAsString());
        if (null != instanceName) {
          instanceNames.add(instanceName);
        }
      }
      return instanceNames;
    } finally {
      ResourceUtils.closeOrLog(hbaseAdmin);
    }
  }

  /**
   * Parses a table name for a kiji instance name.
   *
   * @param kijiTableName The table name to parse
   * @return instance name (or null if none found)
   */
  private static String parseInstanceName(final String kijiTableName) {
    final String[] parts = StringUtils.split(kijiTableName, '\u0000', '.');
    if (parts.length < 3 || !KijiURI.KIJI_SCHEME.equals(parts[0])) {
      return null;
    }
    return parts[1];
  }
}
