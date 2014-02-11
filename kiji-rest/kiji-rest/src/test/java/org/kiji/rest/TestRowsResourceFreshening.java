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

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

import javax.ws.rs.core.UriBuilder;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.yammer.dropwizard.testing.ResourceTest;

import org.junit.After;
import org.junit.Test;

import org.kiji.rest.config.FresheningConfiguration;
import org.kiji.rest.plugins.StandardKijiRestPlugin;
import org.kiji.rest.representations.KijiRestRow;
import org.kiji.rest.resources.RowsResource;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.lib.AlwaysFreshen;

/**
 * Tests freshening with kiji-rest.  Uses a test table with three columns (a, b, and c, all of type
 * long) and one freshness policy.  The table is meant to simulates the pythagorean theorem
 * --that is, the freshness policy uses the values of columns a and b to update the value of c
 * according to the theorem.
 */
public class TestRowsResourceFreshening extends ResourceTest {
  private static final String UTF_8 = Charsets.UTF_8.name();
  private static final String INSTANCE = "fresh";
  private static final String TABLE = "py_table";
  private static final String FAMILY = "trifam";
  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String NORM = "norm";

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private ManagedKijiClient mKijiClient;

  /** {@inheritDoc} */
  @Override
  protected void setUpResources() throws Exception {
    mKiji = new InstanceBuilder(INSTANCE)
        .withTable(TABLE, KijiTableLayouts.getTableLayout("org/kiji/rest/layouts/py_table.json"))
        .build();
    mTable = mKiji.openTable(TABLE);

    KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.registerFreshener(
        TABLE,
        new KijiColumnName(FAMILY, C),
        new AlwaysFreshen(),
        new PythagoreanFunction(),
        ImmutableMap.<String, String>of(),
        true,
        false
    );
    manager.registerFreshener(
        TABLE,
        new KijiColumnName(FAMILY, NORM),
        new AlwaysFreshen(),
        new LPNorm(),
        ImmutableMap.<String, String>of(),
        true,
        false
    );

    mWriter = mTable.getWriterFactory().openTableWriter();

    StandardKijiRestPlugin.registerSerializers(this.getObjectMapperFactory());
    mKijiClient = new ManagedKijiClient(ImmutableSet.of(mKiji.getURI()));
    mKijiClient.start();

    RowsResource resource = new RowsResource(mKijiClient,
        this.getObjectMapperFactory().build(),
        new FresheningConfiguration(true, 1000));

    addResource(resource);
  }

  @After
  public void cleanUpResources() throws Exception {
    mWriter.close();
    mTable.release();
    mKiji.release();
    mKijiClient.stop();
  }

  @Test
  public void testFreshReadWithDefaultFreshen() throws Exception {
    EntityId eid = mTable.getEntityId("testFreshReadWithDefaultFreshen");
    String eidString = URLEncoder.encode(eid.toShellString(), "UTF-8");

    mWriter.put(eid, FAMILY, A, 3L);
    mWriter.put(eid, FAMILY, B, 4L);

    URI uri = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eidString)
        .queryParam("cols", FAMILY + ":" + C)
        .build(INSTANCE, TABLE);

    KijiRestRow row = client().resource(uri).get(KijiRestRow.class);
    assertEquals(1, row.getCells().size());
    assertEquals(5, row.getCells().get(FAMILY).get(C).get(0).getValue());
  }

  @Test
  public void testFreshReadWithExplicitFreshen() throws Exception {
    EntityId eid = mTable.getEntityId("testFreshReadWithDefaultFreshen");
    String eidString = URLEncoder.encode(eid.toShellString(), "UTF-8");

    mWriter.put(eid, FAMILY, A, 6L);
    mWriter.put(eid, FAMILY, B, 8L);

    URI uri = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eidString)
        .queryParam("cols", FAMILY + ":" + C)
        .queryParam("freshen", true)
        .queryParam("timeout", 1000)
        .build(INSTANCE, TABLE);

    KijiRestRow row = client().resource(uri).get(KijiRestRow.class);
    assertEquals(1, row.getCells().size());
    assertEquals(10, row.getCells().get(FAMILY).get(C).get(0).getValue());
  }

  @Test
  public void testFreshReadWithoutFreshen() throws Exception {
    EntityId eid = mTable.getEntityId("testFreshReadWithoutFreshen");
    String eidString = URLEncoder.encode(eid.toShellString(), "UTF-8");

    mWriter.put(eid, FAMILY, A, 6L);
    mWriter.put(eid, FAMILY, B, 8L);

    URI uri = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eidString)
        .queryParam("cols", FAMILY + ":" + C)
        .queryParam("freshen", "false")
        .build(INSTANCE, TABLE);

    KijiRestRow row = client().resource(uri).get(KijiRestRow.class);
    assertEquals(0, row.getCells().size());
  }

  @Test
  public void testFreshReadWithParameter() throws Exception {
    EntityId eid = mTable.getEntityId("testFreshReadWithParameter");
    String eidString = URLEncoder.encode(eid.toShellString(), "UTF-8");

    mWriter.put(eid, FAMILY, A, 3L);
    mWriter.put(eid, FAMILY, B, 4L);

    URI uri = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eidString)
        .queryParam("cols", FAMILY + ":" + NORM)
        .queryParam("fresh.power", "200")
        .build(INSTANCE, TABLE);

    KijiRestRow row = client().resource(uri).get(KijiRestRow.class);
    assertEquals(1, row.getCells().size());
    // L_Infinity.
    assertEquals(4.0, row.getCells().get(FAMILY).get(NORM).get(0).getValue());
  }

  @Test
  public void testFreshReadWithURLEncodedParameter() throws Exception {
    EntityId eid = mTable.getEntityId("testFreshReadWithURLEncodedParameter");
    String eidString = URLEncoder.encode(eid.toShellString(), "UTF-8");

    mWriter.put(eid, FAMILY, A, 3L);
    mWriter.put(eid, FAMILY, B, 4L);

    // Use URLEncoded strings.
    final String freshPower = "%66%72%65%73%68%2E%70%6F%77%65%72";
    final String twoHundred = "%32%30%30";
    assertEquals("fresh.power", URLDecoder.decode(freshPower, UTF_8));
    assertEquals("200", URLDecoder.decode(twoHundred, UTF_8));

    URI uri = UriBuilder
        .fromResource(RowsResource.class)
        .queryParam("eid", eidString)
        .queryParam("cols", FAMILY + ":" + NORM)
        .queryParam(freshPower, twoHundred)
        .build(INSTANCE, TABLE);

    KijiRestRow row = client().resource(uri).get(KijiRestRow.class);
    assertEquals(1, row.getCells().size());
    // L_Infinity.
    assertEquals(4.0, row.getCells().get(FAMILY).get(NORM).get(0).getValue());
  }

  private static final class PythagoreanFunction extends ScoreFunction<Long> {
    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) throws IOException {
      return KijiDataRequest.create(FAMILY);
    }

    @Override
    public TimestampedValue<Long> score(KijiRowData dataToScore, FreshenerContext context)
        throws IOException {
      KijiCell<Long> cellA = dataToScore.getMostRecentCell(FAMILY, A);
      KijiCell<Long> cellB = dataToScore.getMostRecentCell(FAMILY, B);
      return TimestampedValue.create(Math.max(cellA.getTimestamp(), cellB.getTimestamp()),
          Math.round(Math.sqrt(Math.pow(cellA.getData(), 2) + Math.pow(cellB.getData(), 2))));
    }
  }

  private static final class LPNorm extends ScoreFunction<Double> {
    @Override
    public KijiDataRequest getDataRequest(FreshenerContext context) throws IOException {
      return KijiDataRequest.create(FAMILY);
    }

    @Override
    public TimestampedValue<Double> score(KijiRowData dataToScore, FreshenerContext context)
        throws IOException {
      double power = Double.parseDouble(context.getParameter("power"));
      KijiCell<Long> cellA = dataToScore.getMostRecentCell(FAMILY, A);
      KijiCell<Long> cellB = dataToScore.getMostRecentCell(FAMILY, B);
      return TimestampedValue.create(Math.max(cellA.getTimestamp(), cellB.getTimestamp()),
          Math.pow(Math.pow(cellA.getData(), power) + Math.pow(cellB.getData(), power),
              1.0 / power));
    }
  }
}
