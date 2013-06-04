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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Set;

import javax.ws.rs.core.MediaType;

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

import org.kiji.rest.representations.KijiRestCell;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.resources.RowsResource;
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
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Test class for the Row resource.
 *
 */
public class TestRowsResource extends ResourceTest {

  public static final String EXTENSIVE_COLUMN_TEST = ":.:.?&;& /\\\n~!@#$%^&*()_+{}|[]\\;';'\"\"";

  private Kiji mFakeKiji = null;

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
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(7, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchASingleStringCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);
    // Test group qualifier, string type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:string_qualifier";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, long type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:long_qualifier";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, specific avro type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:team_qualifier";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(0).getValue().toString());
    assertEquals(1234, node.get("barracks_status").get("long").asLong());
  }

  @Test
  public void testShouldFetchASingleGenericAvroCellFromGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test group qualifier, generic avro type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:inline_record";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(0).getValue().toString());
    assertEquals("some_user", node.get("username").asText());
  }

  @Test
  public void testShouldFetchASingleStringCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, string type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=strings:apple%20iphone";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("iphone", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleLongCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, long type
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=longs:some%20other%20qualifier";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals(1000, returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchASingleSpecificAvroCellFromMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    // Test map qualifier, specific Avro
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=pick_bans:ban_pick_1";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(0).getValue().toString());
    assertEquals(1, node.get("hero_id").get("long").asLong());
  }

  @Test
  public void testShouldFetchAllQualifiersForAGroupFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchAllQualifiersForAMapFamily() throws Exception {

    String eid = getEntityIdString("sample_table", 12345L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=strings";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchAllVersions() throws Exception {

    String eid = getEntityIdString("sample_table", 2345L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:string_qualifier&versions=all";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchTheLatestVersion() throws Exception {

    String eid = getEntityIdString("sample_table", 2345L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:string_qualifier&versions=1";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value4", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchAllCellsByTime() throws Exception {

    String eid = getEntityIdString("sample_table", 56789L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:string_qualifier&timerange=1..6&versions=10";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(5, returnRow.getCells().size());
    assertEquals("some_value4", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldFetchSingleCellByTime() throws Exception {

    String eid = getEntityIdString("sample_table", 56789L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_family:string_qualifier&timerange=2..3&versions=1";

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().size());
    assertEquals("some_value1", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testShouldThrowExceptionWhenAllColumnsRequestedNotPresent() throws Exception {
    String eid = getEntityIdString("sample_table", 56789L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + eid;
    resourceURI = resourceURI + "&cols=group_familyy";
    try {
      client().resource(resourceURI).get(KijiRestRow.class);
      fail("POST succeeded when it should have failed because of a column not existing.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testShouldSendAllRows() throws Exception {
    String resourceURI = "/v1/instances/default/tables/sample_table/rows";
    String out = client().resource(resourceURI).get(String.class);
    assertEquals(3, out.split("\n").length);
  }

  @Test
  public void testShouldLimitRowsSent() throws Exception {
    String resourceURI = "/v1/instances/default/tables/sample_table/rows?limit=1";
    String out = client().resource(resourceURI).get(String.class);
    assertEquals(1, out.split("\n").length);
  }

  @Test
  public void testShouldReturnRowsInRange() throws Exception {
    String eid = getHBaseRowKeyHex("sample_table", 12345L);

    String resourceURI = "/v1/instances/default/tables/sample_table/rows?start_rk=" + eid
        + "&end_rk=44018000000000003040";
    KijiRestRow row = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(eid, row.getRowKey());
  }

  @Test
  public void testSingleCellPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54321L);
    String stringRowKey = getEntityIdString("sample_table", 54321L);
    KijiRestCell postCell = new KijiRestCell(3141592L,
        "group_family", "string_qualifier", "helloworld");
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    String resourceURI = "/v1/instances/default/tables/sample_table/rows";
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey;
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals("helloworld", returnRow.getCells().get(0).getValue());
  }

  @Test
  public void testMultipleCellPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54323L);
    String stringRowKey = getEntityIdString("sample_table", 54323L);
    KijiRestCell postCell1 = new KijiRestCell(3141592L,
        "group_family", "string_qualifier", "helloworld");
    KijiRestCell postCell2 = new KijiRestCell(3141592L,
        "group_family", "long_qualifier", 123);
    Team postTeam = new Team();
    postTeam.setBarracksStatus(94103L);
    postTeam.setId(88L);
    postTeam.setName("Windrunners");
    KijiRestCell postCell3 = new KijiRestCell(3141592L,
        "group_family",
        "team_qualifier",
        AvroToJsonStringSerializer.getJsonString(postTeam));
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell(postCell3);

    // Post.
    String resourceURI = "/v1/instances/default/tables/sample_table/rows/";
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey;
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals(123, returnRow.getCells().get(0).getValue());
    assertEquals("helloworld", returnRow.getCells().get(1).getValue());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(2).getValue().toString());
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
    KijiRestCell postCell = new KijiRestCell(3141592L,
        "group_family",
        "inline_record",
        AvroToJsonStringSerializer.getJsonString(genericRecord));
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    String resourceURI = "/v1/instances/default/tables/sample_table/rows/";
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey;
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(0).getValue().toString());
    assertEquals("gumshoe", node.get("username").asText());
    assertEquals(5647382910L, node.get("num_purchases").asLong());
  }

  @Test
  public void testMapColumnPost() throws Exception {
    // Set up.
    String hexRowKey = getHBaseRowKeyHex("sample_table", 54325L);
    String stringRowKey = getEntityIdString("sample_table", 54325L);
    String stringsColumn = URLEncoder.encode(EXTENSIVE_COLUMN_TEST, "UTF-8");
    KijiRestCell postCell1 = new KijiRestCell(3141592L, "strings",
        URLDecoder.decode(stringsColumn, "UTF-8"), "helloworld");
    String longsColumn = URLEncoder.encode(" ", "UTF-8");
    KijiRestCell postCell2 = new KijiRestCell(3141592L, "longs",
        URLDecoder.decode(longsColumn, "UTF-8"), 987654567890L);
    String bansColumn = URLEncoder.encode("harkonnen:.:.?&;& ", "UTF-8");
    PickBan postBan = new PickBan();
    postBan.setHeroId(1029384756L);
    postBan.setIsPick(true);
    postBan.setOrder(6L);
    postBan.setTeam(7654L);
    KijiRestCell postCell3 = new KijiRestCell(3141592L,
        "pick_bans",
        URLDecoder.decode(bansColumn, "UTF-8"),
        AvroToJsonStringSerializer.getJsonString(postBan));
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell(postCell3);

    // Post.
    String resourceURI = "/v1/instances/default/tables/sample_table/rows/";
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey;
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals(URLDecoder.decode(longsColumn, "UTF-8"),
        returnRow.getCells().get(0).getColumnQualifier());
    assertEquals(987654567890L, returnRow.getCells().get(0).getValue());
    assertEquals(URLDecoder.decode(stringsColumn, "UTF-8"),
        returnRow.getCells().get(2).getColumnQualifier());
    assertEquals("helloworld", returnRow.getCells().get(2).getValue());
    assertEquals(URLDecoder.decode(bansColumn, "UTF-8"),
        returnRow.getCells().get(1).getColumnQualifier());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(returnRow.getCells().get(1).getValue().toString());
    assertEquals(7654L, node.get("team").get("long").asLong());
    assertEquals(1029384756L, node.get("hero_id").get("long").asLong());
    assertEquals(6L, node.get("order").get("long").asLong());
    assertEquals(true, node.get("is_pick").get("boolean").asBoolean());
  }

  @Test
  public void testNonexistentColumnPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 54322L);
    KijiRestCell postCell = new KijiRestCell(3141592L,
        "nonfamily",
        "noncolumn",
        "hagar");
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell);

    // Post.
    try {
      String resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey;
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
    String stringsColumn = EXTENSIVE_COLUMN_TEST;
    KijiRestCell postCell1 = new KijiRestCell(2L, "strings",
        stringsColumn, "sample_string");
    KijiRestCell postCell2 = new KijiRestCell(3141591L, "group_family", "long_qualifier", 123);
    KijiRestCell postCell3 = new KijiRestCell(null, "group_family",
        "string_qualifier", "helloworld");
    KijiRestRow postRow = new KijiRestRow(ToolUtils
        .createEntityIdFromUserInputs(URLDecoder.decode(stringRowKey, "UTF-8"),
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/sample_table.json")));
    postRow.addCell(postCell1);
    postRow.addCell(postCell2);
    postRow.addCell(postCell3);

    // Post.
    String resourceURI = "/v1/instances/default/tables/sample_table/rows/";
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey
        + "&timerange=3141592..";
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals("string_qualifier", returnRow.getCells().get(0).getColumnQualifier());
    assertTrue(returnRow.getCells().get(0).getTimestamp().longValue()
        > System.currentTimeMillis() - 3000);
    assertEquals("helloworld", returnRow.getCells().get(0).getValue());
    assertEquals(1, returnRow.getCells().size());

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey
        + "&timerange=3141591..3141592";
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals("long_qualifier", returnRow.getCells().get(0).getColumnQualifier());
    assertEquals(3141591L, returnRow.getCells().get(0).getTimestamp().longValue());
    assertEquals(123, returnRow.getCells().get(0).getValue());
    assertEquals(1, returnRow.getCells().size());

    // Retrieve.
    resourceURI = "/v1/instances/default/tables/sample_table/rows?eid=" + stringRowKey
        + "&timerange=0..3";
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString()
        .contains("/v1/instances/default/tables/sample_table/rows/" + hexRowKey));
    assertEquals(stringsColumn, returnRow.getCells().get(0).getColumnQualifier());
    assertEquals(2L, returnRow.getCells().get(0).getTimestamp().longValue());
    assertEquals("sample_string", returnRow.getCells().get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
  }
}
