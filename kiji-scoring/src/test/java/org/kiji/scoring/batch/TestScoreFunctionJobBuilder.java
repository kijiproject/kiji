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
package org.kiji.scoring.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.impl.TestInternalFreshKijiTableReader;

public class TestScoreFunctionJobBuilder extends KijiClientTest {

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupTestScoreFunctionJobBuilder() throws IOException {
    mKiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("foo")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(5L, "foo-val")
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(5L, "bar-val")
        .build();
    mTable = mKiji.openTable("row_data_test_table");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupTestScoreFunctionJobBuilder() throws IOException {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testSimpleJob() throws IOException, InterruptedException, ClassNotFoundException {
    final EntityId fooId = mTable.getEntityId("foo");
    final EntityId barId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");

    final KijiMapReduceJob sfJob = ScoreFunctionJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mTable.getURI())
        .withAttachedColumn(KijiColumnName.create("family:qual0"))
        .withScoreFunctionClass(TestInternalFreshKijiTableReader.TestScoreFunction.class)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();

    assertTrue(sfJob.run());

    assertEquals("new-val",
        mReader.get(fooId, request).getMostRecentValue("family", "qual0").toString());
    assertEquals("new-val",
        mReader.get(barId, request).getMostRecentValue("family", "qual0").toString());
  }

  @Test
  public void testInputOutputMismatch() throws IOException {
    final KijiURI inURI = mTable.getURI();
    final KijiURI outURI = KijiURI.newBuilder(mTable.getURI()).withTableName("wrong").build();

    try {
      final KijiMapReduceJob sfJob = ScoreFunctionJobBuilder.create()
          .withConf(getConf())
          .withInputTable(inURI)
          .withAttachedColumn(KijiColumnName.create("family:qual0"))
          .withScoreFunctionClass(TestInternalFreshKijiTableReader.TestScoreFunction.class)
          .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(outURI))
          .build();
      fail("should have thrown JobConfigurationException.");
    } catch (JobConfigurationException jce) {
      assertEquals(String.format("Output table must be the same as the input"
          + "table. Got input: %s output: %s", inURI, outURI), jce.getMessage());
    }
  }
}
