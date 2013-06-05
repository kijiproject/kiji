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

import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Random;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
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
        .withOutput(new HFileMapReduceJobOutput(mOutputTable.getURI(), hfileDirPath))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    // There is only one reducer, hence one HFile shard:
    final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
    loader.load(hfilePath, mOutputTable);
  }

  @Test
  public void testBulkLoadDirectory() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    final KijiMapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(mBulkImportInputPath))
        .withBulkImporter(TestBulkImporter.SimpleBulkImporter.class)
        .withOutput(new HFileMapReduceJobOutput(mOutputTable.getURI(), hfileDirPath))
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
        .withOutput(new HFileMapReduceJobOutput(mOutputTable.getURI(), hfileDirPath, nSplits))
        .build();
    assertTrue(mrjob.run());

    final HFileLoader loader = HFileLoader.create(mConf);
    loader.load(hfileDirPath, mOutputTable);
  }
}
