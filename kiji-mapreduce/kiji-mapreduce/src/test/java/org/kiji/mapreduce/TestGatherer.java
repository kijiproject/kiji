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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.HBaseFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.TestingHBaseFactory;
import org.kiji.schema.impl.DefaultHBaseFactory;
import org.kiji.schema.layout.KijiTableLayout;

/** Runs a producer job in-process against a fake HBase instance. */
public class TestGatherer {
  private static final Logger LOG = LoggerFactory.getLogger(TestGatherer.class);

  /**
   * Producer intended to run on the generic KijiMR test layout.
   *
   * @see testing resource org/kiji/mapreduce/layout/test.json
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
      return new KijiDataRequest()
          .addColumn(new Column("info"));
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

  @Before
  public void setUp() throws Exception {
    // TODO(KIJI-358): This is quite dangerous, actually, if Maven runs tests in parallel.
    // Instead we should pick separate fake HBase instance IDs for each test.
    HBaseFactory factory = HBaseFactory.Provider.get();
    if (factory instanceof TestingHBaseFactory) {
      ((TestingHBaseFactory) factory).reset();
    }
  }

  @Test
  public void testGatherer() throws Exception {
    // Setup configuration:
    final KijiURI kijiInstanceURI = KijiURI.parse("kiji://.fake.1/test_instance");
    final Configuration conf = HBaseConfiguration.create();

    // In-process MapReduce execution:
    conf.set("mapred.job.tracker", "local");

    final File systemTmpDir = new File(System.getProperty("java.io.tmpdir"));
    Preconditions.checkState(systemTmpDir.exists());

    final File testTmpDir = File.createTempFile("kiji-mr", ".test", systemTmpDir);
    testTmpDir.delete();
    Preconditions.checkState(testTmpDir.mkdirs());

    conf.set("fs.defaultFS", "file://" + testTmpDir);

    KijiInstaller.install(kijiInstanceURI, conf);
    final Kiji kiji = Kiji.Factory.open(kijiInstanceURI, conf);
    LOG.info(String.format("Opened Kiji instance '%s'.", kijiInstanceURI.getInstance()));

    // Create input Kiji table:
    final KijiAdmin admin =
        new KijiAdmin(
            DefaultHBaseFactory.Provider.get().getHBaseAdminFactory(kijiInstanceURI).create(conf),
            kiji);

    final KijiTableLayout tableLayout =
        new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);
    admin.createTable("test", tableLayout, false);

    final KijiTable table = kiji.openTable("test");

    // Populate input Kiji table:
    {
      final KijiTableWriter writer = table.openTableWriter();
      writer.put(table.getEntityId("Marsellus Wallace"), "info", "first_name", "Marsellus");
      writer.put(table.getEntityId("Marsellus Wallace"), "info", "last_name", "Wallace");
      writer.put(table.getEntityId("Marsellus Wallace"), "info", "zip_code", 94110);

      writer.put(table.getEntityId("Vincent Vega"), "info", "first_name", "Vincent");
      writer.put(table.getEntityId("Vincent Vega"), "info", "last_name", "Vega");
      writer.put(table.getEntityId("Vincent Vega"), "info", "zip_code", 94110);
      writer.close();
    }

    final File outputDir = File.createTempFile("gatherer-output", ".dir", testTmpDir);
    Preconditions.checkState(outputDir.delete());
    final int numSplits = 1;

    // Run gatherer:
    final MapReduceJob job = new KijiGatherJobBuilder()
        .withGatherer(TestingGatherer.class)
        .withInputTable(table)
        .withOutput(new TextMapReduceJobOutput(new Path(outputDir.toString()), numSplits))
        .build();
    assertTrue(job.run());

    // Validate output:
    {
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

    // Cleanup:
    FileUtils.deleteQuietly(testTmpDir);
  }
}
