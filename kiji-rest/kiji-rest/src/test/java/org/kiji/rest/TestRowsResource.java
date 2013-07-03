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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.resources.RowsResource;
import org.kiji.rest.sample_avro.PickBan;
import org.kiji.rest.sample_avro.Team;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Test class for the Row resource.
 *
 */
public class TestRowsResource extends ResourceTest {

  private Kiji mFakeKiji = null;

  private static final URI DEFAULT_ROWS_RESOURCE = UriBuilder
      .fromResource(RowsResource.class)
      .build("default", "sample_table");

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
    // KijiInstaller.get().install(uri);
    final Kiji kiji = Kiji.Factory.open(uri);

    return kiji;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    InstanceBuilder builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();
    // mFakeKiji = createTestKiji();
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
    KijiClient kijiClient = new FakeKijiClient(mFakeKiji);
    RowsResource resource = new RowsResource(kijiClient, this.getObjectMapperFactory().build());
    addResource(resource);
  }

  protected final String getHBaseRowKeyHex(String table, Object... components) throws IOException {
    KijiTable fakeTable = mFakeKiji.openTable(table);
    EntityId eid = fakeTable.getEntityId(components);
    String hexRowKey = Hex.encodeHexString(eid.getHBaseRowKey());
    fakeTable.release();
    return hexRowKey;
  }

  protected final String getEntityIdString(String table, Object... components) throws IOException {
    KijiTable fakeTable = mFakeKiji.openTable(table);
    EntityId eid = fakeTable.getEntityId(components);
    String entityIdString = URLEncoder.encode(eid.toShellString(), "UTF-8");
    fakeTable.release();
    return entityIdString;
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
    String eid = getEntityIdString("sample_table", 12345L);
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchASingleStringCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);
    // Test group qualifier, string type
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, long type
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:long_qualifier")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, specific avro type

    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:team_qualifier")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("team_qualifier").get(0).getValue());
    assertEquals(1234, node.get("barracks_status").get("long").asLong());
  }

  @Test
  public void testShouldFetchASingleGenericAvroCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, generic avro type
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:inline_record")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("inline_record").get(0).getValue());
    assertEquals("some_user", node.get("username").asText());
  }

  @Test
  public void testShouldFetchASingleStringCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, string type
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "strings:apple%20iphone")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("iphone", returnRow.getCells().get("strings").get("apple iphone").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, long type
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "longs:some%20other%20qualifier")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get("longs").get("some other qualifier").get(0)
        .getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, specific Avro
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "pick_bans:ban_pick_1")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("pick_bans").get("ban_pick_1")
        .get(0).getValue());
    assertEquals(1, node.get("hero_id").get("long").asLong());
  }

  @Test
  public void testShouldFetchAllQualifiersForAGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().get("group_family").size());
  }

  @Test
  public void testShouldFetchAllQualifiersForAMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "strings")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().get("strings").size());
  }

  @Test
  public void testShouldFetchAllVersions() throws Exception {

    String eid = getEntityIdString("sample_table", 2345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "all")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().get("group_family").get("string_qualifier").size());
  }

  @Test
  public void testShouldFetchTheLatestVersion() throws Exception {

    String eid = getEntityIdString("sample_table", 2345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("versions", "1")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().get("group_family").get("string_qualifier").size());
    assertEquals("some_value4", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchAllCellsByTime() throws Exception {

    String eid = getEntityIdString("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("timerange", "1..6")
        .queryParam("versions", "10")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().get("group_family").get("string_qualifier").size());
    assertEquals("some_value4", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldFetchSingleCellByTime() throws Exception {

    String eid = getEntityIdString("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("timerange", "2..3")
        .queryParam("versions", "1")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().get("group_family").get("string_qualifier").size());
    assertEquals("some_value1", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testShouldThrowExceptionWhenAllColumnsRequestedNotPresent() throws Exception {
    String eid = getEntityIdString("sample_table", 56789L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_familyy")
        .build("default", "sample_table");
    try {
      client().resource(resourceURI).get(KijiRestRow.class);
      fail("POST succeeded when it should have failed because of a column not existing.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testShouldSendAllRows() throws Exception {
    String out = client().resource(DEFAULT_ROWS_RESOURCE).get(String.class);
    assertEquals(3, out.split("\r\n").length);
  }

  @Test
  public void testShouldLimitRowsSent() throws Exception {
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).queryParam("limit", "1")
        .build("default", "sample_table");
    String out = client().resource(resourceURI).get(String.class);
    assertEquals(1, out.split("\r\n").length);
  }

  @Test
  public void testShouldReturnRowsInRange() throws Exception {
    String eid = getHBaseRowKeyHex("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("start_rk", eid)
        .queryParam("end_rk", "44018000000000003040")
        .build("default", "sample_table");

    KijiRestRow row = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(eid, row.getRowKey());
  }

  @Test
  public void testSingleCellPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54321L);
    String stringRowKey = getEntityIdString("sample_table", 54321L);

    KijiCell<String> postCell = fromInputs("group_family", "string_qualifier", 314592L,
        "helloworld");

    KijiRestRow postRow = new KijiRestRow(ToolUtils.createEntityIdFromUserInputs(
        URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    Object target = client().resource(DEFAULT_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testMultipleCellPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54323L);
    String stringRowKey = getEntityIdString("sample_table", 54323L);

    KijiCell<String> postCell1 = fromInputs("group_family", "string_qualifier", 314592L,
        "helloworld");
    KijiCell<Long> postCell2 = fromInputs("group_family", "long_qualifier", 314592L, 123L);

    Team postTeam = new Team();
    postTeam.setBarracksStatus(94103L);
    postTeam.setId(88L);
    postTeam.setName("Windrunners");

    KijiCell<JsonNode> postCell3 = fromInputs("group_family", "team_qualifier", 314592L,
        AvroToJsonStringSerializer.getJsonNode(postTeam));

    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
            KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell(postCell3);

    // Post.
    @SuppressWarnings("unchecked")
    Map<String, List<String>> target = client().resource(DEFAULT_ROWS_RESOURCE)
        .type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
        .post(Map.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).queryParam("eid", stringRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertEquals(target.get("targets").get(0), "/v1/instances/default/tables/sample_table/rows/"
        + hexRowKey);
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
  public void testGenericAvroPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54324L);
    String stringRowKey = getEntityIdString("sample_table", 54324L);
    KijiTableLayout layout = KijiTableLayouts
        .getTableLayout("org/kiji/rest/layouts/sample_table.json");
    CellSpec spec = layout.getCellSpec(new KijiColumnName("group_family:inline_record"));
    GenericData.Record genericRecord = new GenericData.Record(spec.getAvroSchema());
    genericRecord.put("username", "gumshoe");
    genericRecord.put("num_purchases", 5647382910L);
    KijiCell<JsonNode> postCell = fromInputs("group_family", "inline_record", 3141592L,
        AvroToJsonStringSerializer.getJsonNode(genericRecord));
    KijiRestRow postRow = new KijiRestRow(ToolUtils.createEntityIdFromUserInputs(
        URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .build("default", "sample_table");

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
  public void testMapColumnPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54325L);
    String stringRowKey = getEntityIdString("sample_table", 54325L);
    KijiCell<String> postCell1 = fromInputs("strings", TestRowResource.EXTENSIVE_COLUMN_TEST,
        3141592L, "helloworld");
    String longsColumnDec = " ";

    KijiCell<Long> postCell2 = fromInputs("longs", longsColumnDec, 3141592L, 987654567890L);
    String bansColumnDec = "harkonnen:.:.?&;& ";

    PickBan postBan = new PickBan();
    postBan.setHeroId(1029384756L);
    postBan.setIsPick(true);
    postBan.setOrder(6L);
    postBan.setTeam(7654L);

    KijiCell<JsonNode> postCell3 = fromInputs("pick_bans", bansColumnDec, 3141592L,
        AvroToJsonStringSerializer.getJsonNode(postBan));

    KijiRestRow postRow = new KijiRestRow(ToolUtils.createEntityIdFromUserInputs(
        URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell(postCell3);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("longs").containsKey(longsColumnDec));
    assertEquals(987654567890L, returnRow.getCells().get("longs").get(longsColumnDec).get(0)
        .getValue());
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(TestRowResource.EXTENSIVE_COLUMN_TEST));
    assertEquals("helloworld",
        returnRow.getCells().get("strings").get(TestRowResource.EXTENSIVE_COLUMN_TEST).get(0)
            .getValue());
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
  public void testNonexistentColumnPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 54322L);
    KijiCell<String> postCell = fromInputs("nonfamily", "noncolumn", 314592L, "hagar");
    KijiRestRow postRow = new KijiRestRow(ToolUtils.createEntityIdFromUserInputs(
        URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    try {
      URI resourceURI = UriBuilder.fromResource(RowsResource.class)
          .queryParam("eid", stringRowKey)
          .build("default", "sample_table");
      client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
          .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);
      fail("POST succeeded when it should have failed because of a column not existing.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testTimestampedPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 55025L);
    String stringRowKey = getEntityIdString("sample_table", 55025L);

    KijiCell<String> postCell1 = fromInputs("strings", TestRowResource.EXTENSIVE_COLUMN_TEST, 2L,
        "sample_string");
    KijiCell<Long> postCell2 = fromInputs("group_family", "long_qualifier", 3141591L, 123L);

    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
            KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell("group_family", "string_qualifier", null, "helloworld");

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "3141592..")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertTrue(returnRow.getCells().get("group_family").get("string_qualifier").get(0)
        .getTimestamp() > System.currentTimeMillis() - 3000);
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
    assertNull(returnRow.getCells().get("strings"));

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "3141591..3141592")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("long_qualifier"));
    assertEquals(3141591L, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getTimestamp().longValue());
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
    assertEquals(1, returnRow.getCells().size());

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "0..3")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(TestRowResource.EXTENSIVE_COLUMN_TEST));
    assertEquals(2L, returnRow.getCells().get("strings").get(TestRowResource.EXTENSIVE_COLUMN_TEST)
        .get(0).getTimestamp().longValue());
    assertEquals("sample_string",
        returnRow.getCells().get("strings").get(TestRowResource.EXTENSIVE_COLUMN_TEST).get(0)
            .getValue());
    assertEquals(1, returnRow.getCells().size());
  }

  private static <T> KijiCell<T> fromInputs(String family, String qualifier,
      Long timestamp, T value) {
    return new KijiCell<T>(family, qualifier, timestamp, new DecodedCell<T>(null, value));
  }

  @Test
  public void testBatchPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 55025L);
    String stringRowKey = getEntityIdString("sample_table", 55025L);
    String stringsColumn = TestRowResource.EXTENSIVE_COLUMN_TEST;
    KijiCell<String> postCell1 = fromInputs("strings", stringsColumn, 2L, "sample_string");
    KijiCell<Integer> postCell2 = fromInputs("group_family", "long_qualifier", 3141591L, 123);
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);

    List<KijiRestRow> postRows = Lists.newLinkedList();
    postRows.add(postRow);

    postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell2);
    postRows.add(postRow);

    postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell("group_family", "string_qualifier", null, "helloworld");
    postRows.add(postRow);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRows);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "3141592..")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertTrue(returnRow.getCells().get("group_family").get("string_qualifier").get(0)
        .getTimestamp() > System.currentTimeMillis() - 3000);
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
    assertNull(returnRow.getCells().get("strings"));

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "3141591..3141592")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("group_family").containsKey("long_qualifier"));
    assertEquals(3141591L, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getTimestamp().longValue());
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
    assertEquals(1, returnRow.getCells().size());

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .queryParam("timerange", "0..3")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(TestRowResource.EXTENSIVE_COLUMN_TEST));
    assertEquals(2L, returnRow.getCells().get("strings").get(TestRowResource.EXTENSIVE_COLUMN_TEST)
        .get(0).getTimestamp().longValue());
    assertEquals("sample_string",
        returnRow.getCells().get("strings").get(TestRowResource.EXTENSIVE_COLUMN_TEST).get(0)
            .getValue());
    assertEquals(1, returnRow.getCells().size());
  }
}
