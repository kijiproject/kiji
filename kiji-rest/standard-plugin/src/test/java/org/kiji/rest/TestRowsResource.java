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
import static org.junit.Assert.assertFalse;
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.yammer.dropwizard.testing.ResourceTest;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Test;

import org.kiji.rest.representations.KijiRestEntityId;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.representations.SchemaOption;
import org.kiji.rest.resources.RowsResource;
import org.kiji.rest.sample_avro.PickBan;
import org.kiji.rest.sample_avro.Team;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
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
 *
 */
public class TestRowsResource extends ResourceTest {

  private static final String UTF_8 = Charsets.UTF_8.name();

  private Kiji mFakeKiji = null;
  private KijiSchemaTable mSchemaTable = null;
  private ManagedKijiClient mKijiClient = null;

  private static final URI DEFAULT_ROWS_RESOURCE = UriBuilder
      .fromResource(RowsResource.class)
      .build("default", "sample_table");
  private static final URI PLAYERS_ROWS_RESOURCE = UriBuilder
      .fromResource(RowsResource.class)
      .build("default", "players");
  private static final String EXTENSIVE_COLUMN_TEST = ":.:.?&;& /\\\n~!@#$%^&*()_+{}|[]\\;';'\"\"";

  // Some commonly used schema options.
  private SchemaOption mStringOption = null;
  private SchemaOption mLongOption = null;

  /**
   * {@inheritDoc}
   */
  @Override
  protected void setUpResources() throws Exception {
    InstanceBuilder builder = new InstanceBuilder("default");
    mFakeKiji = builder.build();

    mSchemaTable = mFakeKiji.getSchemaTable();

    mStringOption = new SchemaOption(Schema.create(Type.STRING).toString());
    mLongOption = new SchemaOption(Schema.create(Type.LONG).toString());

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

    // Build another table with multi-component Eids.
    // Set up table and data.
    final KijiTableLayout playersTableLayout =
        KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/players_table.json");
    mFakeKiji.createTable(playersTableLayout.getDesc());
    final KijiTable playersTable = mFakeKiji.openTable("players");
    final KijiTableWriter playersTableWriter = playersTable.openTableWriter();
    playersTableWriter.put(
        playersTable.getEntityId("seleukos", "asia.central"),
        "info",
        "fullname",
        "Seleukos Nikator");
    playersTableWriter.put(
        playersTable.getEntityId("seleukos", "makedonia"),
        "info",
        "fullname",
        "Seleukos of Macedon");
    playersTableWriter.put(
        playersTable.getEntityId("cassander", "greece"),
        "info",
        "fullname",
        "Cassander");
    playersTableWriter.put(
        playersTable.getEntityId("antipater", "makedonia"),
        "info",
        "fullname",
        "Antipater");
    playersTableWriter.put(
        playersTable.getEntityId("ptolemaios", "africa.north"),
        "info",
        "fullname",
        "Ptolemy Soter");
    playersTableWriter.close();
    playersTable.release();

    KijiRESTService.registerSerializers(this.getObjectMapperFactory());
    mKijiClient = new ManagedKijiClient(mFakeKiji.getURI());
    mKijiClient.start();

    RowsResource resource = new RowsResource(mKijiClient, this.getObjectMapperFactory().build(),
        new FresheningConfiguration(false, 0));
    addResource(resource);
  }

  protected final String getHBaseRowKeyHex(String table, Object... components) throws IOException {
    KijiTable fakeTable = mFakeKiji.openTable(table);
    EntityId eid = fakeTable.getEntityId(components);
    String hexRowKey = Hex.encodeHexString(eid.getHBaseRowKey());
    fakeTable.release();
    return "hbase_hex=" + hexRowKey;
  }

  protected final String getUrlEncodedEntityIdString(String table, Object... components)
      throws IOException {
    return URLEncoder.encode(getEntityIdString(table, components), "UTF-8");
  }

  protected final String getEntityIdString(String table, Object... components) throws IOException {
    KijiTable fakeTable = mFakeKiji.openTable(table);
    EntityId eid = fakeTable.getEntityId(components);
    String entityIdString = eid.toShellString();
    fakeTable.release();
    return entityIdString;
  }

  protected final String createJsonArray(String... components) throws IOException {
    final JsonNodeFactory factory = new JsonNodeFactory(true);
    ArrayNode arrayNode = factory.arrayNode();
    for (String component : components) {
      JsonNode node;
      if (component.startsWith(KijiRestEntityId.HBASE_ROW_KEY_PREFIX)
          || component.startsWith(KijiRestEntityId.HBASE_HEX_ROW_KEY_PREFIX)) {
        node = factory.textNode(component);
      } else {
        node = stringToJsonNode(component);
      }
      arrayNode.add(node);
    }
    return arrayNode.toString();
  }

  protected final JsonNode stringToJsonNode(String input) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    final JsonParser parser = new JsonFactory().createJsonParser(input)
        .enable(Feature.ALLOW_COMMENTS)
        .enable(Feature.ALLOW_SINGLE_QUOTES)
        .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
    return mapper.readTree(parser);
  }

  /**
   * Runs after each test.
   *
   * @throws Exception
   */
  @After
  public void afterTest() throws Exception {
    mFakeKiji.release();
    mKijiClient.stop();
  }

  @Test
  public void testShouldFetchAllCellsForGivenRow() throws Exception {
    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);
    URI resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().size());
  }

  @Test
  public void testShouldFetchASingleStringCellFromGroupFamily() throws Exception {

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);
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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(4, returnRow.getCells().get("group_family").size());
  }

  @Test
  public void testShouldFetchAllQualifiersForAMapFamily() throws Exception {

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "strings")
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(1, returnRow.getCells().get("strings").size());
  }

  @Test
  public void testShouldFetchAllVersions() throws Exception {

    String eid = getUrlEncodedEntityIdString("sample_table", 2345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 2345L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 56789L);

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

    String eid = getUrlEncodedEntityIdString("sample_table", 56789L);

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
  public void testShouldPrintWildcardedColumn() throws Exception {
    // Request partial entity id.
    String eid = "[[],\"makedonia\"]";
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "info:fullname")
        .queryParam("versions", "1")
        .queryParam("timerange", "0..")
        .build("default", "players");

    // Check output.
    String returnRows = client().resource(resourceURI).get(String.class);
    assertTrue(returnRows.contains("antipater"));
    assertTrue(returnRows.contains("seleukos"));
    assertTrue(!returnRows.contains("ptolemaios"));
    assertTrue(!returnRows.contains("cassander"));

    // Request partial entity id.
    eid = "[\"seleukos\", []]";
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "info:fullname")
        .queryParam("versions", "1")
        .queryParam("timerange", "0..")
        .build("default", "players");

    // Check output.
    returnRows = client().resource(resourceURI).get(String.class);
    assertTrue(returnRows.contains("seleukos"));
    assertTrue(returnRows.contains("makedonia"));
    assertTrue(returnRows.contains("asia.central"));
    assertTrue(!returnRows.contains("antipater"));
    assertTrue(!returnRows.contains("africa.north"));
  }

  @Test
  public void  testShouldFailOnMalformedEids() throws Exception {
    // Try non-empty array element.
    String eid = "[[123]]";
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("timerange", "2..3")
        .queryParam("versions", "1")
        .build("default", "sample_table");
    try {
      client().resource(resourceURI).get(KijiRestRow.class);
      fail("GET succeeded when it should have failed because of a malformed eid.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }

    // Try unbalanced parentheses.
    eid = "[[]";
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .queryParam("timerange", "2..3")
        .queryParam("versions", "1")
        .build("default", "sample_table");
    try {
      client().resource(resourceURI).get(KijiRestRow.class);
      fail("GET succeeded when it should have failed because of a malformed eid.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  @Test
  public void testShouldThrowExceptionWhenAllColumnsRequestedNotPresent() throws Exception {
    String eid = getUrlEncodedEntityIdString("sample_table", 56789L);

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
  public void testShouldBulkFetchMultipleRows() throws Exception {

    String eid1 = getEntityIdString("sample_table", 12345L);
    String eid2 = getEntityIdString("sample_table", 2345L);
    String eids = createJsonArray(eid1, eid2);
    eids = URLEncoder.encode(eids, "UTF-8");

    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eids", eids)
        .build("default", "sample_table");

    String returnRows = client().resource(resourceURI).get(String.class);
    assertTrue(returnRows.contains("12345"));
    assertTrue(returnRows.contains("2345"));
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
        .queryParam("start_eid", eid)
        .queryParam("end_eid", "hbase_hex=44018000000000003040")
        .build("default", "sample_table");

    KijiRestRow row = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals("[12345]", row.getEntityId().getJsonEntityId().toString());
  }

  @Test
  public void testShouldReturnRangeFromMulticomponentKeyTable() throws Exception {
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("start_eid", "[\"seleukos\", \"a\"]")
        .queryParam("end_eid", "[\"seleukos\", \"z\"]")
        .build("default", "players");
    String returnRows = client().resource(resourceURI).get(String.class);
    assertTrue(returnRows.contains("asia.central"));
    assertTrue(returnRows.contains("makedonia"));
  }

  @Test
  public void testShouldReturnRangeWhenRightEndpointIsNotSpecified() throws Exception {
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("start_eid", "[\"antipater\"]")
        .build("default", "players");
    String returnRows = client().resource(resourceURI).get(String.class);
    // Contains antipater's and seleukos' data.
    assertTrue(returnRows.contains("asia.central"));
    assertTrue(returnRows.contains("antipater"));
    assertTrue(returnRows.contains("asia.central"));
    assertTrue(returnRows.contains("seleukos"));
    // Does not contain ptolemaios' and cassander's data.
    assertFalse(returnRows.contains("cassander"));
    assertFalse(returnRows.contains("ptolemaios"));
  }

  @Test
  public void testShouldReturnRangeWhenLeftEndpointIsNotSpecified() throws Exception {
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("end_eid", "[\"antipater\", \"makedonia\"]")
        .build("default", "players");
    String returnRows = client().resource(resourceURI).get(String.class);
    // Contains cassander's and ptolemaios' data.
    assertTrue(returnRows.contains("cassander"));
    assertTrue(returnRows.contains("ptolemaios"));
    // Does not contain seleukos' and antipater's data.
    assertFalse(returnRows.contains("seleukos"));
    assertFalse(returnRows.contains("antipater"));
  }

  @Test
  public void testSingleCellPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 54321L);
    String encodedEid = URLEncoder.encode(stringRowKey, "UTF-8");

    KijiCell<String> postCell = fromInputs("group_family", "string_qualifier", 314592L,
        "helloworld", Schema.create(Type.STRING));

    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));

    addCellToRow(postRow, postCell);

    // Post.
    Object target = client().resource(DEFAULT_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedEid)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
  }

  @Test
  public void testCounterPost() throws Exception {
    // Set up.
    KijiCell<Long> postCell = fromInputs("info",
        "logins",
        0L,
        0L,
        Schema.create(Type.LONG));
    String eid = "[\"menandros\",\"asia.minor\"]";

    KijiRestRow postRow = new KijiRestRow(KijiRestEntityId.create(stringToJsonNode(eid)));

    addCellToRow(postRow, postCell);

    // Post.
    Object target = client().resource(PLAYERS_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "players");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/players/rows?eid="
            + URLEncoder.encode(eid, UTF_8)));
    assertEquals(0, returnRow.getCells().get("info").get("logins").get(0).getValue());

    // Increment counter.
    KijiRestRow postRowIncrement = new KijiRestRow(KijiRestEntityId.create(stringToJsonNode(eid)));
    postRowIncrement.addCell("info",
        "logins",
        null,
        stringToJsonNode("{\"incr\":123}"),
        new SchemaOption(3));

    // Post.
    target = client().resource(PLAYERS_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRowIncrement);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "players");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/players/rows?eid="
            + URLEncoder.encode(eid, UTF_8)));
    assertEquals(123, returnRow.getCells().get("info").get("logins").get(0).getValue());

    // Decrement counter.
    KijiRestRow postRowDecrement = new KijiRestRow(KijiRestEntityId.create(stringToJsonNode(eid)));
    postRowDecrement.addCell("info",
        "logins",
        null,
        stringToJsonNode("{\"incr\": -122}"),
        new SchemaOption(3));

    // Post.
    target = client().resource(PLAYERS_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRowDecrement);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "players");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/players/rows?eid="
            + URLEncoder.encode(eid, UTF_8)));
    assertEquals(1, returnRow.getCells().get("info").get("logins").get(0).getValue());
  }

  @Test
  public void testShouldPostUnionType() throws Exception {
    String stringRowKey = getUrlEncodedEntityIdString("sample_table", 54321L);

    KijiCell<String> postCell = fromInputs("group_family", "union_qualifier", 314592L,
        "helloworld", Schema.create(Type.STRING));

    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    addCellToRow(postRow, postCell);

    // Post.
    client().resource(DEFAULT_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve from rows resource.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", stringRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    assertEquals("helloworld", returnRow.getCells()
        .get("group_family").get("union_qualifier").get(0).getValue());
  }

  @Test
  public void testMultipleCellPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 54323L);
    String encodedRowKey = URLEncoder.encode(stringRowKey, "UTF-8");

    KijiCell<String> postCell1 = fromInputs("group_family", "string_qualifier", 314592L,
        "helloworld", Schema.create(Type.STRING));
    KijiCell<Long> postCell2 = fromInputs("group_family", "long_qualifier", 314592L, 123L,
        Schema.create(Type.LONG));

    Team postTeam = new Team();
    postTeam.setBarracksStatus(94103L);
    postTeam.setId(88L);
    postTeam.setName("Windrunners");

    KijiCell<JsonNode> postCell3 = fromInputs("group_family", "team_qualifier", 314592L,
        AvroToJsonStringSerializer.getJsonNode(postTeam), postTeam.getSchema());

    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));

    addCellToRow(postRow, postCell1);
    addCellToRow(postRow, postCell2);
    addCellToRow(postRow, postCell3);

    // Post.
    @SuppressWarnings("unchecked")
    Map<String, List<String>> target = client().resource(DEFAULT_ROWS_RESOURCE)
        .type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
        .post(Map.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).queryParam("eid", encodedRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertEquals(target.get("targets").get(0), "/v1/instances/default/tables/sample_table/rows?eid="
        + URLEncoder.encode(stringRowKey, UTF_8));
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
    String stringRowKey = getEntityIdString("sample_table", 54324L);
    String encodedRowKey = URLEncoder.encode(stringRowKey, "UTF-8");

    KijiTableLayout layout = KijiTableLayouts
        .getTableLayout("org/kiji/rest/layouts/sample_table.json");
    CellSpec spec = layout.getCellSpec(new KijiColumnName("group_family:inline_record"));
    GenericData.Record genericRecord = new GenericData.Record(spec.getAvroSchema());
    genericRecord.put("username", "gumshoe");
    genericRecord.put("num_purchases", 5647382910L);
    KijiCell<JsonNode> postCell = fromInputs("group_family", "inline_record", 3141592L,
        AvroToJsonStringSerializer.getJsonNode(genericRecord), spec.getAvroSchema());
    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    addCellToRow(postRow, postCell);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .build("default", "sample_table");

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.valueToTree(returnRow.getCells().get("group_family")
        .get("inline_record").get(0).getValue());
    assertEquals("gumshoe", node.get("username").asText());
    assertEquals(5647382910L, node.get("num_purchases").asLong());
  }

  @Test
  public void testMapColumnPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 54325L);
    String encodedRowKey = URLEncoder.encode(stringRowKey, "UTF-8");

    KijiCell<String> postCell1 = fromInputs("strings", EXTENSIVE_COLUMN_TEST,
        3141592L, "helloworld", Schema.create(Type.STRING));
    String longsColumnDec = " ";

    KijiCell<Long> postCell2 = fromInputs("longs", longsColumnDec, 3141592L, 987654567890L,
        Schema.create(Type.LONG));
    String bansColumnDec = "harkonnen:.:.?&;& ";

    PickBan postBan = new PickBan();
    postBan.setHeroId(1029384756L);
    postBan.setIsPick(true);
    postBan.setOrder(6L);
    postBan.setTeam(7654L);

    KijiCell<JsonNode> postCell3 = fromInputs("pick_bans", bansColumnDec, 3141592L,
        AvroToJsonStringSerializer.getJsonNode(postBan), postBan.getSchema());

    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));

    addCellToRow(postRow, postCell1);
    addCellToRow(postRow, postCell2);
    addCellToRow(postRow, postCell3);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class).build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("longs").containsKey(longsColumnDec));
    assertEquals(987654567890L, returnRow.getCells().get("longs").get(longsColumnDec).get(0)
        .getValue());
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(EXTENSIVE_COLUMN_TEST));
    assertEquals("helloworld",
        returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST).get(0)
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
    String stringRowKey = getUrlEncodedEntityIdString("sample_table", 54322L);
    KijiCell<String> postCell = fromInputs("nonfamily", "noncolumn", 314592L, "hagar",
        Schema.create(Type.STRING));
    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    addCellToRow(postRow, postCell);

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
    String stringRowKey = getEntityIdString("sample_table", 55025L);
    String encodedRowKey = URLEncoder.encode(stringRowKey, "UTF-8");

    KijiCell<String> postCell1 = fromInputs("strings", EXTENSIVE_COLUMN_TEST, 2L,
        "sample_string", Schema.create(Type.STRING));
    KijiCell<Long> postCell2 = fromInputs("group_family", "long_qualifier", 3141591L, 123L,
        Schema.create(Type.LONG));

    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));

    addCellToRow(postRow, postCell1);
    addCellToRow(postRow, postCell2);

    postRow.addCell("group_family", "string_qualifier", null, "helloworld", mStringOption);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .queryParam("timerange", "3141592..")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertTrue(returnRow.getCells().get("group_family").get("string_qualifier").get(0)
        .getTimestamp() > System.currentTimeMillis() - 3000);
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
    assertNull(returnRow.getCells().get("strings"));

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .queryParam("timerange", "3141591..3141592")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
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
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(EXTENSIVE_COLUMN_TEST));
    assertEquals(2L, returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST)
        .get(0).getTimestamp().longValue());
    assertEquals("sample_string",
        returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST).get(0)
            .getValue());
    assertEquals(1, returnRow.getCells().size());
  }

  private static <T> KijiCell<T> fromInputs(String family, String qualifier,
      Long timestamp, T value, Schema writerSchema) {
    return new KijiCell<T>(family, qualifier, timestamp, new DecodedCell<T>(writerSchema, value));
  }

  @Test
  public void testBatchPost() throws Exception {
    // Set up.
    String stringRowKey = getEntityIdString("sample_table", 55025L);
    String encodedRowKey = URLEncoder.encode(stringRowKey, "UTF-8");

    String stringsColumn = EXTENSIVE_COLUMN_TEST;
    KijiCell<String> postCell1 = fromInputs("strings", stringsColumn, 2L, "sample_string",
        Schema.create(Type.STRING));
    KijiCell<Integer> postCell2 = fromInputs("group_family", "long_qualifier", 3141591L, 123,
        Schema.create(Type.LONG));
    KijiRestRow postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    addCellToRow(postRow, postCell1);

    List<KijiRestRow> postRows = Lists.newLinkedList();
    postRows.add(postRow);

    postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    addCellToRow(postRow, postCell2);
    postRows.add(postRow);

    postRow = new KijiRestRow(
        KijiRestEntityId.create(stringToJsonNode(
        URLDecoder.decode(stringRowKey, "UTF-8"))));
    postRow.addCell("group_family", "string_qualifier", null, "helloworld",
        mStringOption);
    postRows.add(postRow);

    // Post.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .build("default", "sample_table");
    Object target = client().resource(resourceURI).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRows);

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .queryParam("timerange", "3141592..")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("group_family").containsKey("string_qualifier"));
    assertTrue(returnRow.getCells().get("group_family").get("string_qualifier").get(0)
        .getTimestamp() > System.currentTimeMillis() - 3000);
    assertEquals("helloworld", returnRow.getCells().get("group_family").get("string_qualifier")
        .get(0).getValue());
    assertEquals(1, returnRow.getCells().size());
    assertNull(returnRow.getCells().get("strings"));

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .queryParam("timerange", "3141591..3141592")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("group_family").containsKey("long_qualifier"));
    assertEquals(3141591L, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getTimestamp().longValue());
    assertEquals(123, returnRow.getCells().get("group_family").get("long_qualifier").get(0)
        .getValue());
    assertEquals(1, returnRow.getCells().size());

    // Retrieve.
    resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", encodedRowKey)
        .queryParam("timerange", "0..3")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/sample_table/rows?eid="
            + URLEncoder.encode(stringRowKey, UTF_8)));
    assertTrue(returnRow.getCells().get("strings")
        .containsKey(EXTENSIVE_COLUMN_TEST));
    assertEquals(2L, returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST)
        .get(0).getTimestamp().longValue());
    assertEquals("sample_string",
        returnRow.getCells().get("strings").get(EXTENSIVE_COLUMN_TEST).get(0)
            .getValue());
    assertEquals(1, returnRow.getCells().size());
  }

  @Test
  public void testShouldPostAndGetCounterCell() throws Exception {
    KijiCell<Long> postCell = fromInputs("info",
        "logins",
        0L,
        1234567890987L,
        Schema.create(Type.LONG));
    String eid = "[\"menandros\",\"asia.minor\"]";
    KijiRestRow postRow = new KijiRestRow(KijiRestEntityId.create(stringToJsonNode(eid)));
    addCellToRow(postRow, postCell);

    // Post.
    Object target = client().resource(PLAYERS_ROWS_RESOURCE).type(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON).post(Object.class, postRow);

    // Retrieve.
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "players");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);

    // Check.
    assertTrue(target.toString().contains(
        "/v1/instances/default/tables/players/rows?eid=" + URLEncoder.encode(eid, UTF_8)));
    assertEquals(1234567890987L, returnRow.getCells().get("info").get("logins").get(0).getValue());
  }

  @Test
  public void testShouldDeleteRow() throws Exception {

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    assertTrue(client().resource(resourceURI).delete(Boolean.class));

    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // make sure that row has been deleted
    assertEquals(null, returnRow.getCells().get("group_family"));
  }

  @Test
  public void testShouldDeleteRows() throws Exception {

    String eid1 = getEntityIdString("sample_table", 12345L);
    String eid2 = getEntityIdString("sample_table", 2345L);
    String eids = createJsonArray(eid1, eid2);
    eids = URLEncoder.encode(eids, "UTF-8");
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eids", eids)
        .build("default", "sample_table");
    assertTrue(client().resource(resourceURI).delete(Boolean.class));

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);
    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // make sure that row has been deleted
    assertEquals(null, returnRow.getCells().get("group_family"));

    eid = getUrlEncodedEntityIdString("sample_table", 2345L);
    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // make sure that row has been deleted
    assertEquals(null, returnRow.getCells().get("group_family"));

    eid = getUrlEncodedEntityIdString("sample_table", 56789L);
    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // make sure that other row has not been affected
    returnRow.getCells().get("group_family").get("string_qualifier").get(0);
  }

  @Test
  public void testShouldDeleteColumns() throws Exception {

    String eid = getUrlEncodedEntityIdString("sample_table", 12345L);
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:team_qualifier,group_family:long_qualifier")
        .build("default", "sample_table");
    assertTrue(client().resource(resourceURI).delete(Boolean.class));

    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:long_qualifier,group_family:string_qualifier")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    // make sure that column has been deleted
    assertEquals(null, returnRow.getCells().get("group_family").get("long_qualifier"));
    // make sure that other column remains unaffected
    assertEquals(
        "some_value",
        returnRow.
            getCells().
            get("group_family").
            get("string_qualifier").
            get(0).
            getValue().
            toString());

  }

  @Test
  public void testShouldDeleteColumnsInRows() throws Exception {

    String eid1 = getEntityIdString("sample_table", 12345L);
    String eid2 = getEntityIdString("sample_table", 2345L);
    String eids = createJsonArray(eid1, eid2);
    eids = URLEncoder.encode(eids, "UTF-8");
    URI resourceURI = UriBuilder.fromResource(RowsResource.class)
        .queryParam("eids", eids)
        .queryParam("cols", "group_family:string_qualifier")
        .build("default", "sample_table");
    assertTrue(client().resource(resourceURI).delete(Boolean.class));

    // make sure that string_qualifier has been deleted in row 2345L
    String eid = getUrlEncodedEntityIdString("sample_table", 2345L);
    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:string_qualifier")
        .build("default", "sample_table");
    KijiRestRow returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(null, returnRow.getCells().get("group_family"));

    // make sure that the other columns in row are not affected
    eid = getUrlEncodedEntityIdString("sample_table", 12345L);
    resourceURI = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eid)
        .queryParam("cols", "group_family:long_qualifier")
        .build("default", "sample_table");
    returnRow = client().resource(resourceURI).get(KijiRestRow.class);
    assertEquals(
        1000,
        returnRow.getCells().get("group_family").get("long_qualifier").get(0).getValue());
  }

  @Test
  public void testShouldFailDeleteRows() throws Exception {

    String eid1 = getEntityIdString("sample_table", 12345L);
    String eid2 = getEntityIdString("sample_table", 2345L);
    String eids = createJsonArray(eid1, eid2);
    eids = URLEncoder.encode(eids, "UTF-8");

    // Delete.
    try {
      URI resourceURI = UriBuilder.fromResource(RowsResource.class)
          .queryParam("eid", eid1)
          .queryParam("eids", eids)
          .build("default", "sample_table");
      client().resource(resourceURI).delete(Boolean.class);
      fail("DELETE succeeded when it should have failed because of an improperly formatted "
           + "request.");
    } catch (UniformInterfaceException e) {
      assertEquals(400, e.getResponse().getStatus());
    }
  }

  private void addCellToRow(KijiRestRow rowToModify, KijiCell<?> cellToPost) throws IOException {
    long schemaId = mSchemaTable.getOrCreateSchemaId(cellToPost.getWriterSchema());
    SchemaOption option = new SchemaOption(schemaId);
    rowToModify.addCell(cellToPost, option);
  }
}
