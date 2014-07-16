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

package org.kiji.mapreduce.framework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiBufferedWriter;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraKijiInstaller;
import org.kiji.schema.layout.KijiTableLayouts;

/**
 * Test the Cassandra InputFormat.  Make sure that it does not drop / duplicate any rows.
 */
public class IntegrationTestCassandraKijiTableInputFormat {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestCassandraKijiTableInputFormat.class);

  @Rule
  public TestName mTestName = new TestName();

  private static final String BASE_TEST_URI_PROPERTY = "kiji.test.cluster.uri";

  private static KijiURI mUri;
  private static KijiURI mTableUri;

  private static final int NUM_ROWS = 100000;

  @BeforeClass
  public static void populateTable() throws Exception {
    final Configuration conf = HBaseConfiguration.create();

    if (System.getProperty(BASE_TEST_URI_PROPERTY) != null) {
      mUri = KijiURI.newBuilder(System.getProperty(BASE_TEST_URI_PROPERTY)).build();
    } else {
      // Create a Kiji instance.
      mUri = KijiURI.newBuilder(String.format(
          "kiji-cassandra://%s:%s/127.0.0.10:9042/testinputformat",
          conf.get(HConstants.ZOOKEEPER_QUORUM),
          conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT)
      )).build();
    }

    LOG.info("Installing to URI " + mUri);
    try {
      CassandraKijiInstaller.get().install(mUri, conf);
      LOG.info("Created Kiji instance at " + mUri);
    } catch (IOException ioe) {
      LOG.warn("Could not create Kiji instance.");
      assertTrue("Did not start.", false);
    }

    // Create a table with a simple table layout.
    final Kiji kiji = Kiji.Factory.open(mUri);
    final long timestamp = System.currentTimeMillis();
    kiji.createTable(KijiTableLayouts.getLayout("org/kiji/mapreduce/layout/foo-test-rkf2.json"));

    mTableUri = KijiURI.newBuilder(mUri.toString()).withTableName("foo").build();
    LOG.info("Table URI = " + mTableUri);
    KijiTable table = kiji.openTable("foo");
    KijiBufferedWriter writer = table.getWriterFactory().openBufferedWriter();

    for (int rowNum = 0; rowNum < NUM_ROWS; rowNum++) {
      EntityId eid = table.getEntityId("user" + rowNum + "@kiji.org");
      writer.put(eid, "info", "name", "Mr. Bonkers");
    }
    writer.flush();
    writer.close();
    table.release();
    kiji.release();
  }


  private Configuration createConfiguration() {
    return HBaseConfiguration.create();
  }

  @Test
  public void testGetAllRows() throws Exception {
    Job job = new Job(createConfiguration());

    CassandraKijiTableInputFormat inputFormat = new CassandraKijiTableInputFormat();
    inputFormat.setConf(job.getConfiguration());

    KijiTableInputFormat.configureJob(
        job,
        mTableUri,
        KijiDataRequest.create("info", "name"),
        null,
        null,
        null
    );

    List<InputSplit> inputSplits = inputFormat.getSplits(job);

    // Go through all of the KijiRowData that you get back from all of the different input splits.
    int rowCount = 0;
    for (InputSplit inputSplit: inputSplits) {
      assert inputSplit instanceof CassandraInputSplit;
      CassandraInputSplit split = (CassandraInputSplit) inputSplit;
      CassandraKijiTableInputFormat.CassandraKijiTableRecordReader recordReader =
          new CassandraKijiTableInputFormat.CassandraKijiTableRecordReader(job.getConfiguration());
      recordReader.initializeWithConf(split, job.getConfiguration());
      while (recordReader.nextKeyValue()) {
        assertNotNull(recordReader.getCurrentValue());
        KijiRowData data = recordReader.getCurrentValue();
        rowCount += 1;
      }
      recordReader.close();
    }
    assertEquals(NUM_ROWS, rowCount);
  }

}
