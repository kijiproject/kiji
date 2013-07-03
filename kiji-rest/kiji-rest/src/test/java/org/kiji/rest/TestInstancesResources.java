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

import com.google.common.collect.Sets;
import com.yammer.dropwizard.testing.ResourceTest;

import org.junit.After;
import org.junit.Test;

import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.InstancesResource;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Test class for the Row resource.
 *
 */
public class TestInstancesResources extends ResourceTest {

  private Kiji[] mFakeKijis = null;

  /**
   * Opens a new unique test Kiji instance, creating it if necessary.
   *
   * Each call to this method returns a fresh new Kiji instance.
   * All generated Kiji instances are automatically cleaned up by KijiClientTest.
   *
   * @return a fresh new Kiji instance.
   * @throws Exception on error.
   */
  public Kiji createTestKiji() throws Exception {
    final String hbaseAddress = String.format(".fake.%s-%d", "kiji_rest", 0);
    final KijiURI uri = KijiURI.newBuilder(String.format("kiji://%s/%s", hbaseAddress, "default"))
        .build();
    final Kiji kiji = Kiji.Factory.open(uri);

    return kiji;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    mFakeKijis = new Kiji[2];
    InstanceBuilder builder = new InstanceBuilder("default");
    mFakeKijis[0] = builder.build();
    Set<KijiURI> mValidInstances = Sets.newHashSet();

    TableLayoutDesc desc = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");

    mFakeKijis[0].createTable(desc);
    mValidInstances.add(mFakeKijis[0].getURI());

    builder = new InstanceBuilder("other");
    mFakeKijis[1] = builder.build();

    KijiRESTService.registerSerializers(this.getObjectMapperFactory());
    KijiClient kijiClient = new FakeKijiClient(mFakeKijis);
    InstanceResource instanceResource = new InstanceResource(kijiClient);
    InstancesResource instancesResource = new InstancesResource(kijiClient);
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
    for (Kiji fakeKiji : mFakeKijis) {
      fakeKiji.release();
    }
  }

  @Test
  public void testShouldFetchAllAvailableInstances() throws Exception {
    URI resourceURI = UriBuilder.fromResource(InstancesResource.class).build();
    List<Map<String, String>> instances =
        client().resource(resourceURI).get(List.class);
    assertEquals(2, instances.size());
    assertEquals(instances.get(0).get("name"), mFakeKijis[0].getURI().getInstance());
    assertEquals(instances.get(1).get("name"), mFakeKijis[1].getURI().getInstance());
  }
}
