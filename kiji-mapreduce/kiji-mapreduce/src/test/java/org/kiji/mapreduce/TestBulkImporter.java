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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a bulk-importer job in-process against a fake HBase instance. */
public class TestBulkImporter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestBulkImporter.class);

  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public final void setupTestBulkImporter() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());

    // Populate the environment.
    new InstanceBuilder(getKiji())
        .withTable("test", layout)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable("test");
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestBulkImporter() throws Exception {
    mReader.close();
    mTable.close();
  }

  /**
   * Producer intended to run on the generic KijiMR test layout. Uses the resource
   * org/kiji/mapreduce/layout/test.json
   */
  public static class SimpleBulkImporter extends KijiBulkImporter<LongWritable, Text> {
    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable inputKey, Text value, KijiTableContext context)
        throws IOException {
      final String line = value.toString();
      final String[] split = line.split(":");
      Preconditions.checkState(split.length == 2,
          String.format("Unable to parse bulk-import test input line: '%s'.", line));
      final String rowKey = split[0];
      final String name = split[1];

      final EntityId eid = context.getEntityId(rowKey);
      context.put(eid, "primitives", "string", name);
      context.put(eid, "primitives", "long", inputKey.get());
    }
  }

  @Test
  public void testSimpleBulkImporter() throws Exception {
    // Prepare input file:
    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get("org/kiji/mapreduce/TestBulkImportInput.txt"));

    // Run the bulk-import:
    final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withBulkImporter(SimpleBulkImporter.class)
        .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("primitives"));
    for (KijiRowData row : scanner) {
      final EntityId eid = row.getEntityId();
      final String rowId = Bytes.toString((byte[]) eid.getComponentByIndex(0));
      final String cellContent = row.getMostRecentValue("primitives", "string").toString();
      LOG.info("Row: {}, primitives.string: {}, primitives.long: {}",
          rowId, cellContent, row.getMostRecentValue("primitives", "long"));
      if (rowId.equals("row1")) {
        assertEquals("Marsellus Wallace", cellContent);
      } else if (rowId.equals("row2")) {
        assertEquals("Vincent Vega", cellContent);
      } else {
        fail();
      }
    }
    scanner.close();
  }

  /**
   * Producer intended to run on the generic KijiMR test layout.
   *
   * @see testing resource org/kiji/mapreduce/layout/test.json
   */
  public static class BulkImporterWorkflow extends KijiBulkImporter<LongWritable, Text> {
    private boolean mSetupFlag = false;
    private int mProduceCounter = 0;
    private boolean mCleanupFlag = false;

    /** {@inheritDoc} */
    @Override
    public void setup(KijiTableContext context) throws IOException {
      super.setup(context);
      assertFalse(mSetupFlag);
      assertEquals(0, mProduceCounter);
      mSetupFlag = true;
    }

    /** {@inheritDoc} */
    @Override
    public void produce(LongWritable inputKey, Text value, KijiTableContext context)
        throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      mProduceCounter += 1;

      final String line = value.toString();
      final String[] split = line.split(":");
      Preconditions.checkState(split.length == 2,
          String.format("Unable to parse bulk-import test input line: '%s'.", line));
      final String rowKey = split[0];
      final String name = split[1];

      final EntityId eid = context.getEntityId(rowKey);
      context.put(eid, "primitives", "string", name);
    }

    /** {@inheritDoc} */
    @Override
    public void cleanup(KijiTableContext context) throws IOException {
      assertTrue(mSetupFlag);
      assertFalse(mCleanupFlag);
      assertEquals(2, mProduceCounter);  // input file has 2 entries
      mCleanupFlag = true;
      super.cleanup(context);
    }
  }

  /** Tests the bulk-importer workflow (setup/produce/cleanup) and counters. */
  @Test
  public void testBulkImporterWorkflow() throws Exception {
    // Prepare input file:
    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get("org/kiji/mapreduce/TestBulkImportInput.txt"));

    // Run the bulk-import:
    final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withBulkImporter(BulkImporterWorkflow.class)
        .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    // Validate output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("primitives"));
    final Set<String> produced = Sets.newHashSet();
    for (KijiRowData row : scanner) {
      final String string = row.getMostRecentValue("primitives", "string").toString();
      produced.add(string);
    }
    scanner.close();

    assertTrue(produced.contains("Marsellus Wallace"));
    assertTrue(produced.contains("Vincent Vega"));

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(2,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
  }

  // TODO(KIJI-359): Implement missing tests
  //  - bulk-importing to HFiles
  //  - bulk-importing multiple files
}
