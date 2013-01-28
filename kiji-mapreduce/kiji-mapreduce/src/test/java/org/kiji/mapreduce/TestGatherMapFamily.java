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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Tests a gatherer on a map-type family. */
public class TestGatherMapFamily {
  private static final Logger LOG = LoggerFactory.getLogger(TestGatherMapFamily.class);

  /** Testing gatherer implementation. */
  public static class MapFamilyGatherer extends KijiGatherer<Text, Text> {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("map_family"));
    }

    /** {@inheritDoc} */
    @Override
    public void gather(KijiRowData input, MapReduceContext<Text, Text> context)
        throws IOException {
      final NavigableMap<String, Utf8> qmap = input.getMostRecentValues("map_family");
      Preconditions.checkState(qmap.size() == 1);
      for (Map.Entry<String, Utf8> entry : qmap.entrySet()) {
        final String qualifier = entry.getKey().toString();
        final String value = entry.getValue().toString();
        context.write(new Text(qualifier), new Text(value));
      }
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return Text.class;
    }
  }

  private Kiji mKiji;
  private KijiTable mTable;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
                .withFamily("map_family")
                    .withQualifier("key-mw").withValue("MW")
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
                .withFamily("map_family")
                    .withQualifier("key-vv").withValue("VV")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("test");
  }

  @After
  public void cleanupEnvironment() throws IOException {
    IOUtils.closeQuietly(mTable);
    mKiji.release();
  }

  @Test
  public void testGather() throws Exception {
    final File systemTmpDir = new File(System.getProperty("java.io.tmpdir"));
    Preconditions.checkState(systemTmpDir.exists());

    final File testTmpDir = File.createTempFile("kiji-mr", ".test", systemTmpDir);
    testTmpDir.delete();
    Preconditions.checkState(testTmpDir.mkdirs());

    final File outputDir = File.createTempFile("gatherer-output", ".dir", testTmpDir);
    Preconditions.checkState(outputDir.delete());
    final int numSplits = 1;

    // Run gatherer:
    final MapReduceJob job = KijiGatherJobBuilder.create()
        .withGatherer(MapFamilyGatherer.class)
        .withInputTable(mTable)
        .withOutput(new TextMapReduceJobOutput(new Path(outputDir.toString()), numSplits))
        .build();
    assertTrue(job.run());

    // Validate output:
    final File outputPartFile = new File(outputDir, "part-m-00000");
    final String gatheredText = FileUtils.readFileToString(outputPartFile);
    final Set<String> outputLines = Sets.newHashSet();
    for (String line : gatheredText.split("\n")) {
      outputLines.add(line);
    }
    assertTrue(outputLines.contains("key-vv\tVV"));
    assertTrue(outputLines.contains("key-mw\tMW"));

    // Cleanup:
    FileUtils.deleteQuietly(testTmpDir);
  }
}
