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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a producer job in-process against a fake HBase instance. */
public class TestGatherer extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGatherer.class);

  /**
   * Producer intended to run on the generic KijiMR test layout. Uses resource
   * org/kiji/mapreduce/layout/test.json.
   */
  public static class TestingGatherer extends KijiGatherer<LongWritable, Text> {

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return LongWritable.class;
   }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info");
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData row, MapReduceContext<LongWritable, Text> context)
        throws IOException {
      final Integer zipCode = row.getMostRecentValue("info", "zip_code");
      final String userId = Bytes.toString(row.getEntityId().getKijiRowKey());
      context.write(new LongWritable(zipCode), new Text(userId));
    }
  }

  /** Test table, owned by this test. */
  private KijiTable mTable;

  @Before
  public final void setupTestGatherer() throws Exception {
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
                    .withQualifier("zip_code").withValue(94110)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable("test");
  }

  @After
  public final void teardownTestGatherer() throws Exception {
    mTable.close();
  }

  @Test
  public void testGatherer() throws Exception {
    final File outputDir = File.createTempFile("gatherer-output", ".dir", getLocalTempDir());
    Preconditions.checkState(outputDir.delete());
    final int numSplits = 1;

    // Run gatherer:
    final MapReduceJob job = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(TestingGatherer.class)
        .withInputTable(mTable)
        .withOutput(new TextMapReduceJobOutput(new Path(outputDir.toString()), numSplits))
        .build();
    assertTrue(job.run());

    // Validate output:
    final File outputPartFile = new File(outputDir, "part-m-00000");
    final String gatheredText = FileUtils.readFileToString(outputPartFile);
    final String[] lines = gatheredText.split("\n");
    assertEquals(2, lines.length);
    final Set<String> userIds = Sets.newHashSet();
    for (String line : lines) {
      final String[] split = line.split("\t");
      assertEquals(2, split.length);
      assertEquals("94110", split[0]);
      userIds.add(split[1]);
    }
    assertTrue(userIds.contains("Marsellus Wallace"));
    assertTrue(userIds.contains("Vincent Vega"));
  }
}
