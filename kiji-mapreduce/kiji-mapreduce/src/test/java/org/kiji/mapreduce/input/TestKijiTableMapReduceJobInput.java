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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiManagedHBaseTableName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ScanEquals;

public class TestKijiTableMapReduceJobInput extends KijiClientTest {
  @Before
  public void setupLayout() throws Exception {
    getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));
  }

  @Test
  public void testConfigure() throws IOException {
    Job job = new Job();
    // Read from mykiji.mytable.
    Kiji kiji = createMock(Kiji.class);
    HBaseKijiTable inputTable = createMock(HBaseKijiTable.class);
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");

    expect(kiji.getURI()).andReturn(KijiURI.parse("kiji://.env/mykiji")).anyTimes();
    expect(kiji.getConf()).andReturn(job.getConfiguration()).anyTimes();
    expect(inputTable.getKiji()).andReturn(kiji).anyTimes();
    expect(inputTable.getName()).andReturn("table").anyTimes();
    expect(inputTable.getLayout()).andReturn(tableLayout);

    // Request the latest 3 versions of the foo:bar column.
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "column").withMaxVersions(3));

    // Read from here to there.
    EntityId startRow = RawEntityId.fromKijiRowKey(Bytes.toBytes("here"));
    EntityId limitRow = RawEntityId.fromKijiRowKey(Bytes.toBytes("there"));

    replay(kiji);
    replay(inputTable);

    // Configure the job.
    KijiTableMapReduceJobInput.RowOptions rowOptions = new KijiTableMapReduceJobInput.RowOptions(
        startRow, limitRow, null);
    MapReduceJobInput kijiTableJobInput = new KijiTableMapReduceJobInput(
        inputTable, dataRequest, rowOptions);
    kijiTableJobInput.configure(job);

    verify(kiji);
    verify(inputTable);

    // Check that the job was configured correctly.
    Configuration conf = job.getConfiguration();
    assertEquals(KijiManagedHBaseTableName.getKijiTableName("mykiji", "table").toString(),
        conf.get(TableInputFormat.INPUT_TABLE));

    // Check the scan range.
    Scan scan = GenericTableMapReduceUtil.convertStringToScan(conf.get(TableInputFormat.SCAN));
    assertArrayEquals(Bytes.toBytes("here"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("there"), scan.getStopRow());

    // Check the scan columns.
    HBaseDataRequestAdapter dataRequestAdapter = new HBaseDataRequestAdapter(dataRequest);
    Scan expectedScan = dataRequestAdapter.toScan(tableLayout);
    expectedScan.setStartRow(Bytes.toBytes("here"));
    expectedScan.setStopRow(Bytes.toBytes("there"));
    expectedScan.setCacheBlocks(false);
    assertTrue(new ScanEquals(expectedScan).matches(scan));
  }
}
