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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.yammer.dropwizard.testing.ResourceTest;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.plugins.StandardKijiRestPlugin;
import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.InstancesResource;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;

/**
 * Test class for the Row resource.
 *
 */
public class TestInstancesResources extends ResourceTest {

  private final KijiClientTest mDelegate = new KijiClientTest();
  private ManagedKijiClient mClient;
  private Set<String> mInstances;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    mDelegate.setupKijiTest();

    KijiURI clusterURI = mDelegate.createTestHBaseURI();

    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
        clusterURI.getZooKeeperEnsemble(), new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
    zkClient
        .create()
        .creatingParentsIfNeeded()
        .forPath(ZooKeeperMonitor.INSTANCES_ZOOKEEPER_PATH + "/non_existant_instance");
    zkClient.close();

    Kiji kiji1 = mDelegate.createTestKiji(clusterURI);
    Kiji kiji2 = mDelegate.createTestKiji(clusterURI);

    ImmutableSet.Builder<String> instances = ImmutableSet.builder();
    instances.add(kiji1.getURI().getInstance());
    instances.add(kiji2.getURI().getInstance());
    mInstances = instances.build();

    StandardKijiRestPlugin.registerSerializers(this.getObjectMapperFactory());
    mClient = new ManagedKijiClient(clusterURI);
    mClient.start();

    InstanceResource instanceResource = new InstanceResource(mClient);
    InstancesResource instancesResource = new InstancesResource(mClient);
    addResource(instanceResource);
    addResource(instancesResource);
  }

  /**
   * Runs after each test.
   *
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception {
    mClient.stop();
    mDelegate.teardownKijiTest();
  }

  @Test
  public void testShouldFetchAllAvailableInstances() throws Exception {
    final URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    @SuppressWarnings("unchecked") final List<Map<String, String>> instances =
        (List<Map<String, String>>) client().resource(resourceURI).get(List.class);

    final Set<String> instanceNames = Sets.newHashSet();
    for (Map<String, String> instance : instances) {
      instanceNames.add(instance.get("name"));
    }

    assertEquals(mInstances, instanceNames);
  }
}
