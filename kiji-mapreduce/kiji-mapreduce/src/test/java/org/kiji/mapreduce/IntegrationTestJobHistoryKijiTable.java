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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.ResourceUtils;

/**
 * Integration test for the job history table.
 */
public class IntegrationTestJobHistoryKijiTable extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestJobHistoryKijiTable.class);

  /**
   * Installs the job history table.
   */
  @Before
  public void setup() throws Exception {
    Kiji kiji = null;
    try {
      kiji = Kiji.Factory.open(getKijiConfiguration());
      KijiAdmin kijiAdmin = kiji.getAdmin();
      JobHistoryKijiTable.install(kijiAdmin);
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Test that makes sure the job history table is installed correctly and can be opened.
   */
  @Test
  public void testInstallAndOpen() throws Exception {
    Kiji kiji = Kiji.Factory.open(getKijiConfiguration());
    // This will throw an IOException if there's difficulty opening the table
    JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);
    jobHistory.close();
    kiji.release();
  }

  /** A private inner producer to test job recording. */
  public static class EmailDomainProducer extends KijiProducer {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      // We only need to read the most recent email address field from the user's row.
      return KijiDataRequest.create("info", "email");
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "derived:domain";
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      if (!input.containsColumn("info", "email")) {
        // This user doesn't have an email address.
        return;
      }
      String email = input.getMostRecentValue("info", "email").toString();
      int atSymbol = email.indexOf("@");
      if (atSymbol < 0) {
        // Couldn't find the '@' in the email address. Give up.
        return;
      }
      String domain = email.substring(atSymbol + 1);
      context.put(domain);
    }
  }

  /**
   * Test of all the basic information recorded by a mapper.
   */
  @Test
  public void testMappers() throws Exception {
    createAndPopulateFooTable();
    KijiConfiguration kijiConf = getKijiConfiguration();
    // Set a value in the configuration. We'll check to be sure we can retrieve it later.
    kijiConf.getConf().set("CONF_TEST_ANIMAL_STRING", "squirrel");
    LOG.info("Kiji configuration has " + new Integer(kijiConf.getConf().size()).toString()
        + " keys.");
    Kiji kiji = Kiji.Factory.open(kijiConf);
    KijiTable fooTable = kiji.openTable("foo");
    JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);

    // Construct a Producer for this table.
    KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
        .withInputTable(fooTable)
        .withProducer(EmailDomainProducer.class)
        .withOutput(new DirectKijiTableMapReduceJobOutput(fooTable));
    MapReduceJob mrJob = builder.build();

    // Record the jobId and run the job.
    String jobName = mrJob.getHadoopJob().getJobName();
    LOG.info("About to run job: " + jobName);
    assertTrue(mrJob.run());
    String jobId = mrJob.getHadoopJob().getJobID().toString();
    LOG.info("Job was run with id: " + jobId);

    // Retrieve the recorded values and sanity test them.
    KijiRowData jobRecord = jobHistory.getJobDetails(jobId);
    assertTrue(jobRecord.containsColumn("info", "jobName"));
    assertEquals(jobRecord.getMostRecentValue("info", "jobName").toString(), jobName);
    assertTrue(jobRecord.containsColumn("info", "jobId"));
    assertEquals(jobRecord.getMostRecentValue("info", "jobId").toString(), jobId);

    assertTrue(jobRecord.containsColumn("info", "startTime"));
    assertTrue(jobRecord.containsColumn("info", "endTime"));
    assertTrue(jobRecord.<Long>getMostRecentValue("info", "startTime")
        < jobRecord.<Long>getMostRecentValue("info", "endTime"));

    // Check counters. We don't know the exact number of rows in the foo table, so just check if
    // it's greater than 0.
    assertTrue(jobRecord.containsColumn("info", "counters"));
    Counters counters = new Counters();
    ByteBuffer countersByteBuffer = jobRecord.getMostRecentValue("info", "counters");
    counters.readFields(new DataInputStream(new ByteArrayInputStream(countersByteBuffer.array())));
    long processed = counters.findCounter(JobHistoryCounters.PRODUCER_ROWS_PROCESSED).getValue();
    LOG.info("Processed " + processed + " rows.");
    assertTrue(processed > 0);

    // Test to make sure the Configuration contains the value we set before.
    assertTrue(jobRecord.containsColumn("info", "configuration"));
    LOG.info(jobRecord.<ByteBuffer>getMostRecentValue("info", "configuration").array().toString());
    Configuration config = new Configuration();
    ByteBuffer configByteBuffer = jobRecord.getMostRecentValue("info", "configuration");
    ByteArrayInputStream configInputStream = new ByteArrayInputStream(configByteBuffer.array());
    config.readFields(new DataInputStream(configInputStream));
    LOG.info("Deserialized configuration has " + new Integer(config.size()).toString() + " keys.");
    Configuration.dumpConfiguration(config, new OutputStreamWriter(System.out));
    assertTrue(EmailDomainProducer.class
        == config.getClass(KijiConfKeys.KIJI_PRODUCER_CLASS, null));
    assertEquals("Couldn't retrieve configuration field from deserialized configuration.",
        "squirrel", config.get("CONF_TEST_ANIMAL_STRING"));

    fooTable.close();
    jobHistory.close();
    kiji.release();
  }

  /**
   * Test that makes sure information is recorded correctly for a job run with .submit() instead
   * of .run(). Only checks timing info.
   */
  @Test
  public void testSubmit() throws Exception {
    createAndPopulateFooTable();
    Kiji kiji = Kiji.Factory.open(getKijiConfiguration());
    KijiTable fooTable = kiji.openTable("foo");
    JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);

    // Construct a Producer for this table.
    KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
        .withInputTable(fooTable)
        .withProducer(EmailDomainProducer.class)
        .withOutput(new DirectKijiTableMapReduceJobOutput(fooTable));
    MapReduceJob mrJob = builder.build();

    LOG.info("About to submit job: " + mrJob.getHadoopJob().getJobName());
    MapReduceJob.Status status = mrJob.submit();
    while (!status.isComplete()) {
      Thread.sleep(1000L);
    }
    assertTrue(status.isSuccessful());
    String jobId = mrJob.getHadoopJob().getJobID().toString();
    LOG.info("Job successfully submitted and run. Id: " + jobId);

    // The job recording takes place in a separate thread, so sleep a bit to give it time to write
    // out.
    Thread.sleep(5000L);

    KijiRowData jobRecord = jobHistory.getJobDetails(jobId);
    assertTrue(jobRecord.containsColumn("info", "startTime"));
    assertTrue(jobRecord.containsColumn("info", "endTime"));
    assertTrue(jobRecord.<Long>getMostRecentValue("info", "startTime")
        < jobRecord.<Long>getMostRecentValue("info", "endTime"));

    fooTable.close();
    jobHistory.close();
    kiji.release();
  }

  /**
   * Tests that a job will still run to completion even without an installed job history table.
   */
   @Test
   public void testMissingHistoryTableNonfatal() throws Exception {
     createAndPopulateFooTable();
     Kiji kiji = Kiji.Factory.open(getKijiConfiguration());
     KijiTable fooTable = kiji.openTable("foo");
     KijiAdmin kijiAdmin = kiji.getAdmin();
     kijiAdmin.deleteTable(JobHistoryKijiTable.getInstallName());

     KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
         .withInputTable(fooTable)
         .withProducer(EmailDomainProducer.class)
         .withOutput(new DirectKijiTableMapReduceJobOutput(fooTable));
     MapReduceJob mrJob = builder.build();
     assertTrue(mrJob.run());

     fooTable.close();
     kiji.release();
   }
}
