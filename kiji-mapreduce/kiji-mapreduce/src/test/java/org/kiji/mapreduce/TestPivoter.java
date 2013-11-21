/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.pivot.KijiPivotJobBuilder;
import org.kiji.mapreduce.pivot.KijiPivoter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a {@link KijiPivoter} job in-process against a fake HBase instance. */
public class TestPivoter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestPivoter.class);

  // -----------------------------------------------------------------------------------------------

  /** {@link KijiPivoter} intended to run on the generic KijiMR test layout. */
  public static class TestingPivoter extends KijiPivoter {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData row, KijiTableContext context)
        throws IOException {
      final Integer zipCode = row.getMostRecentValue("info", "zip_code");
      final String userId = Bytes.toString((byte[]) row.getEntityId().getComponentByIndex(0));

      final EntityId eid = context.getEntityId(zipCode.toString());
      context.put(eid, "primitives", "string", userId);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Test table, owned by this test. */
  private KijiTable mTable;

  @Before
  public final void setupTest() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    new InstanceBuilder(getKiji())
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
                    .withQualifier("zip_code").withValue(94110)
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
                    .withQualifier("zip_code").withValue(94111)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable("test");
  }

  @After
  public final void teardownTest() throws Exception {
    mTable.release();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testPivoter() throws Exception {
    final KijiMapReduceJob job = KijiPivotJobBuilder.create()
        .withConf(getConf())
        .withPivoter(TestingPivoter.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());
    assertEquals(2,
        job.getHadoopJob().getCounters()
            .findCounter(JobHistoryCounters.PIVOTER_ROWS_PROCESSED).getValue());

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiDataRequest dataRequest = KijiDataRequest.create("primitives");
      final KijiRowData row1 = reader.get(mTable.getEntityId("94110"), dataRequest);
      assertEquals("Marsellus Wallace", row1.getMostRecentValue("primitives", "string").toString());

      final KijiRowData row2 = reader.get(mTable.getEntityId("94111"), dataRequest);
      assertEquals("Vincent Vega", row2.getMostRecentValue("primitives", "string").toString());

    } finally {
      reader.close();
    }
  }
}
