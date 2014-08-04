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
import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.Sets;
import com.yammer.dropwizard.testing.ResourceTest;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.InstancesResource;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MetadataBackup;

/**
 * Test class for the Row resource.
 *
 */
public class TestInstancesResources extends ResourceTest {

  private final KijiClientTest mDelegate = new KijiClientTest();
  private ManagedKijiClient mClient;
  private ManagedKijiClient mNullInstanceClient;
  private Set<String> mInstances;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    mDelegate.setupKijiTest();

    KijiURI clusterURI = mDelegate.createTestHBaseURI();

    Kiji kiji1 = mDelegate.createTestKiji(clusterURI);
    Kiji kiji2 = mDelegate.createTestKiji(clusterURI);

    mInstances = Sets.newHashSet(kiji1.getURI().getInstance(), kiji2.getURI().getInstance());

    KijiRESTService.registerSerializers(this.getObjectMapperFactory());
    mClient = new ManagedKijiClient(clusterURI, ManagedKijiClient.DEFAULT_TIMEOUT, mInstances);
    mClient.start();

    mNullInstanceClient = new ManagedKijiClient(clusterURI,
        ManagedKijiClient.DEFAULT_TIMEOUT,
        null);
    mNullInstanceClient.start();

    addResource(new InstanceResource(mNullInstanceClient));
    addResource(new InstancesResource(mClient));
  }

  /**
   * Runs after each test.
   *
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception {
    mClient.stop();
    mNullInstanceClient.stop();
    mDelegate.teardownKijiTest();
  }

  @Test
  public void testShouldFetchNoInstancesAsConfigured() throws Exception {

    Set<String> oldInstances = new HashSet<String>(mInstances);

    mInstances.clear();
    mClient.refreshInstances();

    final URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    @SuppressWarnings("unchecked")
    final List<Map<String, String>> instances =
        (List<Map<String, String>>) client().resource(resourceURI).get(List.class);

    assertEquals(0, instances.size());
    mInstances.addAll(oldInstances);
    mClient.refreshInstances();

    // Test the restoration to ensure no side effects for future tests.
    testShouldFetchAllAvailableInstances();
  }

  @Test
  public void testShouldFetchAllAvailableInstances() throws Exception {
    final URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    @SuppressWarnings("unchecked")
    final List<Map<String, String>> instances =
        (List<Map<String, String>>) client().resource(resourceURI).get(List.class);

    final Set<String> instanceNames = Sets.newHashSet();
    for (Map<String, String> instance : instances) {
      instanceNames.add(instance.get("name"));
    }

    assertEquals(mInstances, instanceNames);

    // Ensure that we can fetch something from the instance resource for each instance.
    for (String instance : instanceNames) {
      URI instanceURI = UriBuilder.fromResource(InstanceResource.class).build(instance);
      MetadataBackup backup = client().resource(instanceURI).get(MetadataBackup.class);
      assertNotNull(backup);
    }
  }
}
