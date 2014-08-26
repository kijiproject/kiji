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

package org.kiji.mapreduce;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

public class IntegrationTestKijiBulkLoad
    extends AbstractKijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestKijiBulkLoad.class);

  private Configuration mConf = null;
  private FileSystem mFS = null;
  private Path mBulkImportInputPath = null;
  private Kiji mKiji = null;
  private KijiTable mOutputTable = null;

  /**
   * Generates a random HDFS path.
   *
   * @param prefix Prefix for the random file name.
   * @return a random HDFS path.
   * @throws Exception on error.
   */
  private Path makeRandomPath(String prefix) throws Exception {
    Preconditions.checkNotNull(mFS);
    final Path base = new Path(FileSystem.getDefaultUri(mConf));
    final Random random = new Random(System.nanoTime());
    return new Path(base, String.format("/%s-%s", prefix, random.nextLong()));
  }

  private void writeTestResource(Path path, String testResource) throws Exception {
    final OutputStream ostream = mFS.create(path);
    IOUtils.write(TestingResources.get(testResource), ostream);
    ostream.close();
  }

  @Before
  public void setUp() throws Exception {
    mConf = createConfiguration();
    mFS = FileSystem.get(mConf);
    mBulkImportInputPath = makeRandomPath("bulk-import-input");

    // Prepare input file:
    writeTestResource(mBulkImportInputPath, "org/kiji/mapreduce/TestBulkImportInput.txt");

    mKiji = Kiji.Factory.open(getKijiURI(), mConf);
    final KijiTableLayout layout = KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    mKiji.createTable("test", layout);
    mOutputTable = mKiji.openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    mOutputTable.release();
    mKiji.release();
    mFS.delete(mBulkImportInputPath, false);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973

    mOutputTable = null;
    mKiji = null;
    mBulkImportInputPath = null;
    mFS = null;
    mConf = null;
  }

  @Test
  public void testBulkLoadHFile() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withBulkImporter(TestBulkImporter.SimpleBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mOutputTable.getURI(), hfileDirPath))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    // There is only one reducer, hence one HFile shard:
    final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
    loader.load(hfilePath, mOutputTable);
  }

  /**
   * Bulk importer intended to run on the generic KijiMR test layout. Uses the resource
   * org/kiji/mapreduce/layout/test.json.
   *
   * This importer emits cells, but immediately deletes some of them as well: we should
   * not see the deleted cells after bulk load.
   */
  public static class TombstoningBulkImporter extends KijiBulkImporter<LongWritable, Text> {
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
      context.put(eid, "primitives", "string", 1L, name);
      context.put(eid, "primitives", "long", 1L, inputKey.get());

      // Now delete the long
      context.deleteCell(eid, "primitives", "long", 1L);
    }
  }

  @Test
  public void testBulkLoadHFilesWithTombstones() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withBulkImporter(TombstoningBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mOutputTable.getURI(), hfileDirPath))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    loader.load(hfileDirPath, mOutputTable);

    final KijiDataRequestBuilder dataRequestBuilder = KijiDataRequest.builder();
    dataRequestBuilder
        .newColumnsDef()
        .add("primitives", "string")
        .add("primitives", "long");
    final KijiDataRequest dataRequest = dataRequestBuilder.build();

    final KijiTableReader tableReader = mOutputTable.openTableReader();
    try {
      final KijiRowScanner scanner = tableReader.getScanner(dataRequest);
      try {
        int rows = 0;
        for (KijiRowData row : scanner) {
          rows++;
          assertTrue(row.containsColumn("primitives", "string"));
          assertFalse(row.containsColumn("primitives", "long"));
        }
        assertTrue("At least one row should have been found.", rows > 0);
      } finally {
        scanner.close();
      }
    } finally {
      tableReader.close();
    }
  }

  @Test
  public void testBulkLoadDirectory() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withBulkImporter(TestBulkImporter.SimpleBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mOutputTable.getURI(), hfileDirPath))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    loader.load(hfileDirPath, mOutputTable);
  }

  @Test
  public void testBulkLoadMultipleSplits() throws Exception {
    final int nSplits = 3;
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withBulkImporter(TestBulkImporter.SimpleBulkImporter.class)
        .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
            mOutputTable.getURI(), hfileDirPath, nSplits))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    loader.load(hfileDirPath, mOutputTable);
  }
}
