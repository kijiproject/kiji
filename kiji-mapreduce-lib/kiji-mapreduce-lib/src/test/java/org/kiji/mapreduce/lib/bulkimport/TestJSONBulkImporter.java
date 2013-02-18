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

package org.kiji.mapreduce.lib.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.TestingResources;
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Unit tests. */
public class TestJSONBulkImporter extends KijiClientTest {
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

  @Test
  public void testJSONBulkImporter() throws Exception {
    // Prepare input file:
    File inputFile = File.createTempFile("TestJSONImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get(BulkImporterTestUtils.JSON_IMPORT_DATA));

    Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE,
        BulkImporterTestUtils.localResource(BulkImporterTestUtils.FOO_IMPORT_DESCRIPTOR));

    // Run the bulk-import:
    final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(JSONBulkImporter.class)
        .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(1,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());

    // Validate output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("info"));
    BulkImporterTestUtils.validateImportedRows(scanner, false);
    scanner.close();
  }

  @Test
  public void testTimestampJSONBulkImporter() throws Exception {
    // Prepare input file:
    File inputFile = File.createTempFile("TestJSONImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get(BulkImporterTestUtils.JSON_IMPORT_DATA));

    Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE,
        BulkImporterTestUtils.localResource(BulkImporterTestUtils.FOO_TIMESTAMP_IMPORT_DESCRIPTOR));

    // Run the bulk-import:
    final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(JSONBulkImporter.class)
        .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(1,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());

    // Validate output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("info"));
    BulkImporterTestUtils.validateImportedRows(scanner, true);
    scanner.close();
  }

  @Test
  public void testJSONPathBulkImporter() throws Exception {
    // Prepare input file:
    File inputFile = File.createTempFile("TestComplexJSONImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get(BulkImporterTestUtils.COMPLEX_JSON_IMPORT_DATA));

    Configuration conf = getConf();
    conf.set(DescribedInputTextBulkImporter.CONF_FILE,
        BulkImporterTestUtils.localResource(BulkImporterTestUtils.FOO_JSONPATH_IMPORT_DESCRIPTOR));

    // Run the bulk-import:
    final MapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withBulkImporter(JSONBulkImporter.class)
        .withInput(new TextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    assertTrue(job.run());

    final Counters counters = job.getHadoopJob().getCounters();
    assertEquals(3,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).getValue());
    assertEquals(1,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE).getValue());
    assertEquals(0,
        counters.findCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED).getValue());

    // Validate output:
    final KijiRowScanner scanner = mReader.getScanner(KijiDataRequest.create("info"));
    BulkImporterTestUtils.validateImportedRows(scanner, false);
    scanner.close();
  }
}
