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

package org.kiji.mapreduce.input;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ScanEquals;
import org.kiji.schema.util.TestFileUtils;

public class TestKijiTableMapReduceJobInput extends KijiClientTest {
  private File mTempDir;
  private Path mTempPath;
  private KijiTable mTable;

  @Before
  public void setUp() throws Exception {
    mTempDir = TestFileUtils.createTempDir("test", "dir");
    mTempPath = new Path("file://" + mTempDir);

    getConf().set("fs.defaultFS", mTempPath.toString());
    getConf().set("fs.default.name", mTempPath.toString());
    final KijiTableLayout layout = KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    getKiji().createTable("test", layout);

    // Set the working directory so that it gets cleaned up after the test:
    getConf().set("mapred.working.dir", new Path(mTempPath, "workdir").toString());

    mTable = getKiji().openTable("test");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
    mTable.close();
    mTempDir = null;
    mTempPath = null;
    mTable = null;
  }

  @Test
  public void testConfigure() throws IOException {
    final Job job = new Job();

    // Request the latest 3 versions of column 'info:email':
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.addColumns().withMaxVersions(3).add("info", "email");
    KijiDataRequest dataRequest = builder.build();

    // Read from 'here' to 'there':
    final EntityId startRow = RawEntityId.fromKijiRowKey(Bytes.toBytes("here"));
    final EntityId limitRow = RawEntityId.fromKijiRowKey(Bytes.toBytes("there"));
    final KijiTableMapReduceJobInput.RowOptions rowOptions =
        new KijiTableMapReduceJobInput.RowOptions(startRow, limitRow, null);
    final MapReduceJobInput kijiTableJobInput =
        new KijiTableMapReduceJobInput(mTable, dataRequest, rowOptions);
    kijiTableJobInput.configure(job);

    // Check that the job was configured correctly.
    final Configuration conf = job.getConfiguration();
    assertEquals(
        KijiManagedHBaseTableName.getKijiTableName(
            getKiji().getURI().getInstance(), mTable.getURI().getTable()).toString(),
        conf.get(TableInputFormat.INPUT_TABLE));

    // Check the scan range.
    final Scan scan =
        GenericTableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
    assertArrayEquals(Bytes.toBytes("here"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("there"), scan.getStopRow());

    // Check the scan columns.
    final HBaseDataRequestAdapter dataRequestAdapter = new HBaseDataRequestAdapter(dataRequest);
    final Scan expectedScan = dataRequestAdapter.toScan(mTable.getLayout());
    expectedScan.setStartRow(Bytes.toBytes("here"));
    expectedScan.setStopRow(Bytes.toBytes("there"));
    expectedScan.setCacheBlocks(false);
    assertTrue(new ScanEquals(expectedScan).matches(scan));
  }
}
