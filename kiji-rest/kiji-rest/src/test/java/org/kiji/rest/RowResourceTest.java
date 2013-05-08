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

import java.util.Set;

import com.google.common.collect.Sets;
import com.yammer.dropwizard.testing.ResourceTest;

import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.core.TestKijiRestCell;
import org.kiji.rest.core.TestKijiRestRow;
import org.kiji.rest.resources.RowResource;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Test class for the Row resource.
 *
 */
public class RowResourceTest extends ResourceTest {

  Kiji mFakeKiji = null;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    InstanceBuilder builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    System.out.println(mFakeKiji.getURI().toString());
    Set<KijiURI> mValidInstances = Sets.newHashSet();

    TableLayoutDesc desc = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");

    mFakeKiji.createTable(desc);
    mValidInstances.add(mFakeKiji.getURI());
    KijiRESTService.registerSerializers(this.getObjectMapperFactory());
    RowResource resource = new RowResource(mFakeKiji.getURI(), mValidInstances);
    addResource(resource);
  }


  /**
   * Runs after each test.
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception
  {
    mFakeKiji.release();
  }

  @Test
  public void testRowResource() throws Exception
  {
    //Create a simple row.
    KijiTable fakeTable = mFakeKiji.openTable("sample_table");
    EntityId eid = fakeTable.getEntityId(12345l);
    String hexRowKey = Hex.encodeHexString(eid.getHBaseRowKey());
    String resourceURI = "/v1/instances/default/tables/sample_table/rows/"+ hexRowKey;
    KijiTableWriter writer = fakeTable.openTableWriter();
    writer.put(eid, "group_family", "string_qualifier", "some_value");
    writer.close();
    TestKijiRestRow returnRow = client().resource(resourceURI)
        .get(TestKijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    TestKijiRestCell cell = returnRow.getCells().get(0);
    assertEquals("some_value", cell.getValue().toString());
  }
}
