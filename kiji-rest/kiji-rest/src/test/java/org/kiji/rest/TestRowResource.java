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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Set;

import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.resources.RowResource;
import org.kiji.rest.sample_avro.PickBan;
import org.kiji.rest.sample_avro.Team;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Test class for the Row resource.
 */
public class TestRowResource extends ResourceTest {

  public static final String EXTENSIVE_COLUMN_TEST = ":.:.?&;& /\\\n~!@#$%^&*()_+{}|[]\\;';'\"\"";

  private Kiji mFakeKiji = null;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    InstanceBuilder builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    Set<KijiURI> mValidInstances = Sets.newHashSet();

    TableLayoutDesc desc = KijiTableLayouts.getLayout("org/kiji/rest/layouts/sample_table.json");

    mFakeKiji.createTable(desc);
    mValidInstances.add(mFakeKiji.getURI());

    // Add some data
    KijiTable fakeTable = mFakeKiji.openTable("sample_table");
    EntityId eid = fakeTable.getEntityId(12345L);
    KijiTableWriter writer = fakeTable.openTableWriter();
    writer.put(eid, "group_family", "string_qualifier", "some_value");
    writer.put(eid, "group_family", "long_qualifier", 1000L);
    Team t = new Team();
    t.setBarracksStatus(1234L);
    t.setComplete(12345L);
    t.setId(1L);
    t.setName("Team Name");
    writer.put(eid, "group_family", "team_qualifier", t);

    CellSpec spec = fakeTable.getLayout().getCellSpec(
        new KijiColumnName("group_family:inline_record"));
    Schema schema = spec.getAvroSchema();
    GenericData.Record genericRecord = new GenericData.Record(schema);
    genericRecord.put("username", "some_user");
    genericRecord.put("num_purchases", 10L);
    writer.put(eid, "group_family", "inline_record", genericRecord);

    PickBan ban = new PickBan();
    ban.setHeroId(1L);
    ban.setIsPick(false);
    ban.setOrder(2L);
    ban.setTeam(3L);

    writer.put(eid, "pick_bans", "ban_pick_1", ban);

    writer.put(eid, "strings", "apple iphone", "iphone");
    writer.put(eid, "longs", "some other qualifier", 1000L);

    // Using the group family that stores strings, let's create multiple versions of a single
    // cell
    EntityId eid2 = fakeTable.getEntityId(2345L);
    writer.put(eid2, "group_family", "string_qualifier", "some_value");
    Thread.sleep(5);
    writer.put(eid2, "group_family", "string_qualifier", "some_value1");
    Thread.sleep(5);
    writer.put(eid2, "group_family", "string_qualifier", "some_value2");
    Thread.sleep(5);
    writer.put(eid2, "group_family", "string_qualifier", "some_value3");
    Thread.sleep(5);
    writer.put(eid2, "group_family", "string_qualifier", "some_value4");

    // Let's write out known timestamps so that we can do timerange queries
    EntityId eid3 = fakeTable.getEntityId(56789L);
    writer.put(eid3, "group_family", "string_qualifier", 1, "some_value");
    writer.put(eid3, "group_family", "string_qualifier", 2, "some_value1");
    writer.put(eid3, "group_family", "string_qualifier", 3, "some_value2");
    writer.put(eid3, "group_family", "string_qualifier", 4, "some_value3");
    writer.put(eid3, "group_family", "string_qualifier", 5, "some_value4");

    writer.close();
    fakeTable.release();

    KijiRESTService.registerSerializers(this.getObjectMapperFactory());
    ManagedKijiClient kijiClient = new ManagedKijiClient(Sets.newHashSet(mFakeKiji.getURI()));
    kijiClient.start();
    RowResource resource = new RowResource(kijiClient);
    addResource(resource);
  }

  protected final String getHBaseRowKeyHex(String table, Object... components) throws IOException {
    KijiTable fakeTable = mFakeKiji.openTable(table);
    EntityId eid = fakeTable.getEntityId(components);
    String hexRowKey = Hex.encodeHexString(eid.getHBaseRowKey());
    fakeTable.release();
    return hexRowKey;
  }

  /**
   * Runs after each test.
   *
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception {
    mFakeKiji.release();
  }

  @Test
  public void testShouldFetchAllCellsForGivenRow() throws Exception {
    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);
    URI resourceURI = UriBuilder.fromResource(RowResource.class).build("default", "sample_table",
        hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // 4 unique column families.
    assertEquals(4, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchASingleStringCellFromGroupFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:string_qualifier")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromGroupFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test group qualifier, long type
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:long_qualifier")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromGroupFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test group qualifier, specific avro type
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:team_qualifier")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    ObjectMapper mapper = new ObjectMapper();

    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("team_qualifier").get(0).getValue());
    assertEquals(1234, node.get("barracks_status").get("long").asLong());
  }

  @Test
  public void testShouldFetchASingleGenericAvroCellFromGroupFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test group qualifier, generic avro type
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:inline_record")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("inline_record").get(0).getValue());
    assertEquals("some_user", node.get("username").asText());

  }

  @Test
  public void testShouldFetchASingleStringCellFromMapFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test map qualifier, string type
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "strings:apple%20iphone").build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("iphone", returnRow.getCells().get("strings").get("apple iphone").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromMapFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test map qualifier, long type
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "longs:some%20other%20qualifier")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get("longs").get("some other qualifier").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromMapFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    // Test map qualifier, specific Avro
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "pick_bans:ban_pick_1").build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("pick_bans").get("ban_pick_1")
        .get(0).getValue());
    assertEquals(1, node.get("hero_id").get("long").asLong());
  }

  @Test
  public void testShouldFetchAllQualifiersForAGroupFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class).queryParam("cols", "group_family")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().get("group_family").size());
  }

  @Test
  public void testShouldFetchAllQualifiersForAMapFamily() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class).queryParam("cols", "strings")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchAllVersions() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 2345L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "all")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().get("group_family").get("string_qualifier").size());
  }

  @Test
  public void testShouldFetchTheLatestVersion() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 2345L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "1")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value4", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchAllCellsByTime() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "10")
        .queryParam("timerange", "1..6")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().get("group_family").get("string_qualifier").size());
    assertEquals("some_value4", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchSingleCellByTime() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "1")
        .queryParam("timerange", "2..3")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value1", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldThrowExceptionWhenAllColumnsRequestedNotPresent() throws Exception {
    String hexRowKey = getHBaseRowKeyHex("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_familyy")
        .build("default", "sample_table", hexRowKey);

    try {
      client().resource(resourceURI).get(KijiRestRow.class);
      fail("GET succeeded when it should have failed because of a column not existing.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testShouldReadUnionType() throws Exception {

    String hexRowKey = getHBaseRowKeyHex("sample_table", 12345L);
    KijiTable table = mFakeKiji.openTable("sample_table");
    EntityId eid = table.getEntityId(12345L);

    KijiTableWriter writer = table.openTableWriter();
    writer.put(eid, "group_family", "union_qualifier", "Some String");
    writer.close();
    table.release();

    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:union_qualifier")
        .queryParam("versions", "1")
        .build("default", "sample_table", hexRowKey);

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("Some String", returnRow.getCells().get("group_family").get("union_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testSingleCellPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54321L);

    // Put.
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .queryParam("timestamp", "3141592")
        .build("default", "sample_table", hexRowKey);

    Object target = client().resource(resourceURI).put(Object.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("cols", "group_family:long_qualifier")
        .build("default", "sample_table", hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
  }

  @Test
  public void testMultipleCellPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54323L);
    Team putTeam = new Team();
    putTeam.setBarracksStatus(94103L);
    putTeam.setId(88L);
    putTeam.setName("Windrunners");

    // Put.
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .queryParam("group_family:string_qualifier", "helloworld")
        .queryParam("group_family:team_qualifier",
            URLEncoder.encode(AvroToJsonStringSerializer.getJsonString(putTeam), "UTF-8"))
        .queryParam("timestamp", "3141592").build("default", "sample_table", hexRowKey);

    Object target = client().resource(resourceURI).put(Object.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class)
        .build("default", "sample_table", hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("team_qualifier").get(0).getValue());
    assertEquals(94103L, node.get("barracks_status").get("long").asLong());
    assertEquals(88L, node.get("id").get("long").asLong());
    assertEquals("Windrunners", node.get("name").get("string").asText());
  }

  @Test
  public void testGenericAvroPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54324L);
    KijiTableLayout layout = KijiTableLayouts
        .getTableLayout("org/kiji/rest/layouts/sample_table.json");
    CellSpec spec = layout.getCellSpec(new KijiColumnName("group_family:inline_record"));
    GenericData.Record genericRecord = new GenericData.Record(spec.getAvroSchema());
    genericRecord.put("username", "gumshoe");
    genericRecord.put("num_purchases", 5647382910L);

    // Put.
    URI resourceURI = UriBuilder
        .fromResource(RowResource.class)
        .queryParam("group_family:inline_record",
            URLEncoder.encode(AvroToJsonStringSerializer.getJsonString(genericRecord), "UTF-8"))
        .queryParam("timestamp", "3141592").build("default", "sample_table", hexRowKey);

    Object target = client().resource(resourceURI).put(Object.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class).build("default", "sample_table",
        hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("inline_record").get(0).getValue());
    assertEquals("gumshoe", node.get("username").asText());
    assertEquals(5647382910L, node.get("num_purchases").asLong());
  }

  @Test
  public void testMapColumnPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54325L);
    String longsColumn = "%20";
    String longsColumnDec = " ";
    String bansColumnDec = "harkonnen:.:.?&;& ";
    String bansColumn = URLEncoder.encode(bansColumnDec, "UTF-8").replace("+", "%20");
    String stringsColumn = URLEncoder.encode(EXTENSIVE_COLUMN_TEST, "UTF-8").replace("+", "%20");

    PickBan putBan = new PickBan();
    putBan.setHeroId(1029384756L);
    putBan.setIsPick(true);
    putBan.setOrder(6L);
    putBan.setTeam(7654L);

    // Put.
    URI resourceURI = UriBuilder
        .fromResource(RowResource.class)
        .queryParam("longs:" + longsColumn, "987654567890")
        .queryParam("strings:" + stringsColumn, "helloworld")
        .queryParam("pick_bans:" + bansColumn,
            URLEncoder.encode(AvroToJsonStringSerializer.getJsonString(putBan), "UTF-8"))
        .queryParam("timestamp", "3141592").build("default", "sample_table", hexRowKey);

    Object target = client().resource(resourceURI).put(Object.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class).build("default", "sample_table",
        hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("longs").containsKey(longsColumnDec));
    assertEquals(987654567890L, returnRow.getCells().get("longs").get(longsColumnDec).get(0)
        .getValue());
    assertTrue(returnRow.getCells().get("strings").containsKey(EXTENSIVE_COLUMN_TEST));
    assertEquals("helloworld", returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST)
        .get(0).getValue());
    assertTrue(returnRow.getCells().get("pick_bans").containsKey(bansColumnDec));

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("pick_bans").get(bansColumnDec)
        .get(0).getValue());
    assertEquals(7654L, node.get("team").get("long").asLong());
    assertEquals(1029384756L, node.get("hero_id").get("long").asLong());
    assertEquals(6L, node.get("order").get("long").asLong());
    assertEquals(true, node.get("is_pick").get("boolean").asBoolean());
  }

  @Test
  public void testNonexistentColumnPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54323L);

    // Put.
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .queryParam("nonfamily:noncolumn", "helloworld")
        .queryParam("timestamp", 3141592)
        .build("default", "sample_table", hexRowKey);

    try {
      client().resource(resourceURI).put(Object.class);
      fail("PUT succeeded when it should have failed because of a column not existing.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testTimestampedPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54323L);
    String stringsCol = URLEncoder.encode(EXTENSIVE_COLUMN_TEST, "UTF-8").replace("+", "%20");

    // Put.
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .queryParam("timestamp.group_family:long_qualifier", "3141591")
        .queryParam("group_family:string_qualifier", "helloworld")
        .queryParam("strings:" + stringsCol, "sample_string")
        .queryParam("timestamp.strings:" + stringsCol, "2")
        .queryParam("timestamp.nonfamily:noncolumn", "12") // Should have no effect.
        .queryParam("timestamp", 3141592)
        .build("default", "sample_table", hexRowKey);

    Object target = client().resource(resourceURI).put(Object.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class).queryParam("timerange", "3141592..")
        .build("default", "sample_table", hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertEquals(3141592L, returnRow.getCells().get("group_family").get("string_qualifier").get(0)
        .getTimestamp().longValue());
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().get("group_family").get("string_qualifier").size());

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("timerange", "3141591..3141592").build("default", "sample_table", hexRowKey);
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("long_qualifier"));

    assertEquals(3141591L, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getTimestamp().longValue());
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());

    assertEquals(1, returnRow.getCells().get("group_family").get("long_qualifier").size());

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class).queryParam("timerange", "0..3")
        .build("default", "sample_table", hexRowKey);
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("strings").containsKey(EXTENSIVE_COLUMN_TEST));
    assertEquals(2L, returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST).get(0)
        .getTimestamp().longValue());
    assertEquals("sample_string", returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST)
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
  }

  @Test
  public void testNoTimestampPut() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 543233L);

    // Put.
    URI resourceURI = UriBuilder.fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .build("default", "sample_table", hexRowKey);

    try {
      client().resource(resourceURI).put(Object.class);
      fail("PUT succeeded instead of failing from unspecified timestamp.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testWriterSchemaPuts() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54400L);
    KijiTableLayout layout = KijiTableLayouts
        .getTableLayout("org/kiji/rest/layouts/sample_table.json");
    CellSpec spec = layout.getCellSpec(new KijiColumnName("group_family:inline_record"));
    GenericData.Record genericRecord = new GenericData.Record(spec.getAvroSchema());
    genericRecord.put("username", "gumshoe");
    genericRecord.put("num_purchases", 5647382910L);
    // This is just the reader schema inferred from the layout.
    String schemaInlineRecord = URLEncoder.encode(genericRecord.getSchema().toString(), "UTF-8");
    // TODO Try putting with a writer schema which is an extension of the reader schema.

    // Put.

    URI resourceURI = UriBuilder
        .fromResource(RowResource.class)
        .queryParam("group_family:long_qualifier", "123")
        .queryParam("schema.group_family:long_qualifier",
            "" + URLEncoder.encode("\"long\"", "UTF-8"))
        .queryParam("group_family:string_qualifier", "helloworld")
        .queryParam("schema.group_family:string_qualifier",
            "" + URLEncoder.encode("\"string\"", "UTF-8"))
        .queryParam("group_family:inline_record",
            URLEncoder.encode(AvroToJsonStringSerializer.getJsonString(genericRecord), "UTF-8"))
        .queryParam("schema.group_family:inline_record", schemaInlineRecord)
        .queryParam("schema.nonfamily:noncolumn", "blah") // Should have no effect.
        .queryParam("timestamp", "3141592")
        .build("default", "sample_table", hexRowKey);

    String target = client().resource(resourceURI).put(String.class);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowResource.class).build("default", "sample_table",
        hexRowKey);
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("long_qualifier"));
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("inline_record").get(0).getValue());
    assertEquals("gumshoe", node.get("username").asText());
    assertEquals(5647382910L, node.get("num_purchases").asLong());
  }
}
