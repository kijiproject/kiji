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

package org.kiji.mapreduce.testlib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.HFileLoader;
import org.kiji.mapreduce.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** Tests bulk-importers. */
public class IntegrationTestSimpleBulkImporter extends AbstractKijiIntegrationTest {
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

  private void writeBulkImportInput(Path path) throws Exception {
    final String[] inputLines = {
        "row1:1",
        "row2:2",
        "row3:2",
        "row4:2",
        "row5:5",
        "row6:1",
        "row7:2",
        "row8:1",
        "row9:2",
        "row10:2",
    };

    final OutputStream ostream = mFS.create(path);
    for (String line : inputLines) {
      IOUtils.write(line, ostream);
      ostream.write('\n');
    }
    ostream.close();
  }

  /**
   * Reads a table into a map from Kiji row keys to KijiRowData.
   *
   * @param table Kiji table to read from.
   * @param kdr Kiji data request.
   * @return a map of the rows.
   * @throws Exception on error.
   */
  private static Map<String, KijiRowData> toRowMap(KijiTable table, KijiDataRequest kdr)
      throws Exception {
    final KijiTableReader reader = table.openTableReader();
    try {
      final KijiRowScanner scanner = reader.getScanner(kdr);
      try {
        final Map<String, KijiRowData> rows = Maps.newHashMap();
        for (KijiRowData row : scanner) {
          rows.put(Bytes.toString(row.getEntityId().getKijiRowKey()), row);
        }
        return rows;
      } finally {
        scanner.close();
      }
    } finally {
      reader.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    mConf = createConfiguration();
    mFS = FileSystem.get(mConf);
    mBulkImportInputPath = makeRandomPath("bulk-import-input");
    writeBulkImportInput(mBulkImportInputPath);
    mKiji = Kiji.Factory.open(getKijiURI(), mConf);
    final KijiTableLayout layout = new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);
    mKiji.getAdmin().createTable("test", layout, false);
    mOutputTable = mKiji.openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    mOutputTable.close();
    mKiji.release();
    mFS.delete(mBulkImportInputPath, false);

    mOutputTable = null;
    mKiji = null;
    mBulkImportInputPath = null;
    mFS = null;
    mConf = null;
  }

  @Test
  public void testSimpleBulkImporterDirect() throws Exception {
    final MapReduceJob mrjob = KijiBulkImportJobBuilder.create()
        .withBulkImporter(SimpleBulkImporter.class)
        .withInput(new TextMapReduceJobInput(mBulkImportInputPath))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mOutputTable))
        .build();
    assertTrue(mrjob.run());

    validateOutputTable();
  }

  @Test
  public void testSimpleBulkImporterHFile() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    try {
      final MapReduceJob mrjob = KijiBulkImportJobBuilder.create()
          .withBulkImporter(SimpleBulkImporter.class)
          .withInput(new TextMapReduceJobInput(mBulkImportInputPath))
          .withOutput(new HFileMapReduceJobOutput(mOutputTable, hfileDirPath))
          .build();
      assertTrue(mrjob.run());

      final HFileLoader loader = HFileLoader.create(mConf);
      // There is only one reducer, hence one HFile shard:
      final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
      loader.load(hfilePath, mOutputTable);

      validateOutputTable();

    } finally {
      mFS.delete(hfileDirPath, true);
    }
  }

  private void validateOutputTable() throws Exception {
    final KijiDataRequest kdr = new KijiDataRequest().addColumn(new Column("primitives"));
    final Map<String, KijiRowData> rows = toRowMap(mOutputTable, kdr);
    assertEquals(10, rows.size());
    for (int i = 1; i <= 10; ++i) {
      final String rowId = String.format("row%d", i);
      assertTrue(rows.containsKey(rowId));
      assertTrue(
          rows.get(rowId).getMostRecentValue("primitives", "string").toString()
              .startsWith(rowId + "-"));
    }
  }

  // TODO: tests with # of splits > 1.

}
