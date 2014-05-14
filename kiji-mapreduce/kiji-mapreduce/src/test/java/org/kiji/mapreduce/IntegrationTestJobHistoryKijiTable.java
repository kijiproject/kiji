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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.avro.generated.JobHistoryEntry;
import org.kiji.mapreduce.framework.JobHistoryKijiTable;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.mapreduce.tools.KijiJobHistory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * Integration test for the job history table.
 */
public class IntegrationTestJobHistoryKijiTable extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestJobHistoryKijiTable.class);

  /**
   * Test that makes sure the job history table is installed correctly and can be opened.
   */
  @Test
  public void testInstallAndOpen() throws Exception {
    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // This will throw an IOException if there's difficulty opening the table
      final JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);
      jobHistory.close();
    } finally {
      kiji.release();
    }
  }

  /**
   * This test sets up an older version of the job history table and ensures that
   * it gets updated upon install.
   *
   * @throws Exception Upon failure to install or upgrade the job history table.
   */
  @Test
  public void testUpgradeJobHistoryTable() throws Exception {
    final String tableName = "job_history";

    // old table layout.
    final String tableLayoutResource = "/org/kiji/mapreduce/job-history-layout.json";

    // all job history tables have at least this version.
    final String prevTableLayoutVersion = "1";

    // latest job history layout version
    final String jhTableLayoutVersion = "2";

    Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      // If the job history table exists, delete it.
      if (kiji.getTableNames().contains(tableName)) {
        kiji.deleteTable(tableName);
      }
      // Create a job history table with the older layout
      kiji.createTable(
          KijiTableLayout.createFromEffectiveJsonResource(tableLayoutResource).getDesc());
      assertEquals(kiji.getMetaTable().getTableLayout(tableName).getDesc().getLayoutId(),
          prevTableLayoutVersion);

      // Now install job history table
      final JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);
      try {
        assertEquals(kiji.getMetaTable().getTableLayout(tableName).getDesc().getLayoutId(),
            jhTableLayoutVersion);
      } finally {
        jobHistory.close();
      }
    } finally {
      kiji.release();
    }
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

  /** A private inner producer to test job recording of failed jobs. */
  public static class BrokenEmailDomainProducer extends KijiProducer {
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
      throw new RuntimeException("This producer always fails.");
    }
  }

  /**
   * Test of all the basic information recorded by a mapper.
   */
  @Test
  public void testMappers() throws Exception {
    createAndPopulateFooTable();
    final Configuration jobConf = getConf();
    // Set a value in the configuration. We'll check to be sure we can retrieve it later.
    jobConf.set("conf.test.animal.string", "squirrel");
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();
      final JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);

      try {
        // Construct a Producer for this table.
        final KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
            .withConf(jobConf)
            .withInputTable(fooTableURI)
            .withProducer(EmailDomainProducer.class)
            .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(fooTableURI));
        KijiMapReduceJob mrJob = builder.build();

        // Record the jobId and run the job.
        String jobName = mrJob.getHadoopJob().getJobName();
        LOG.info("About to run job: " + jobName);
        assertTrue(mrJob.run());
        String jobId = mrJob.getHadoopJob().getJobID().toString();
        LOG.info("Job was run with id: " + jobId);

        // Retrieve the recorded values and sanity test them.
        JobHistoryEntry jobEntry = jobHistory.getJobDetails(jobId);
        assertEquals(jobEntry.getJobName(), jobName);
        assertEquals(jobEntry.getJobId(), jobId);
        assertTrue(jobEntry.getJobStartTime() < jobEntry.getJobEndTime());
        assertEquals("SUCCEEDED", jobEntry.getJobEndStatus());

        // Check counters. We don't know the exact number of rows in the foo table, so just check if
        // it's greater than 0.
        final String countersString = jobEntry.getJobCounters();
        final Pattern countersPattern = Pattern.compile("PRODUCER_ROWS_PROCESSED=(\\d+)");
        final Matcher countersMatcher = countersPattern.matcher(countersString);
        assertTrue(countersMatcher.find());
        assertTrue(Integer.parseInt(countersMatcher.group(1)) > 0);

        // Test to make sure the Configuration has the correct producer class, and records the value
        // we set previously.
        final String configString = jobEntry.getJobConfiguration();
        final Configuration config = new Configuration();
        config.addResource(new ByteArrayInputStream(configString.getBytes()));
        assertTrue(EmailDomainProducer.class
            == config.getClass(KijiConfKeys.KIJI_PRODUCER_CLASS, null));
        assertEquals("Couldn't retrieve configuration field from deserialized configuration.",
            "squirrel", config.get("conf.test.animal.string"));
      } finally {
        jobHistory.close();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Test that runs a producer that always fails and checks to be sure that it's recorded in the
   * history table with a failure.
   */
  @Test
  public void testFailingJob() throws Exception {
    createAndPopulateFooTable();
    final Configuration jobConf = getConf();
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();
      final JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);
      try {
        // Construct a Producer for this table.
        final KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
            .withConf(jobConf)
            .withInputTable(fooTableURI)
            .withProducer(BrokenEmailDomainProducer.class)
            .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(fooTableURI));
        KijiMapReduceJob mrJob = builder.build();

        // Record the jobId and run the job. Make sure it completes and failed.
        String jobName = mrJob.getHadoopJob().getJobName();
        LOG.info("About to run failing job: " + jobName);
        assertFalse(mrJob.run());
        String jobId = mrJob.getHadoopJob().getJobID().toString();
        LOG.info("Job was run with id: " + jobId);
        assertTrue(mrJob.getHadoopJob().isComplete());
        assertFalse(mrJob.getHadoopJob().isSuccessful());

        // Retrieve the job status from the history table and make sure it failed.
        JobHistoryEntry jobEntry = jobHistory.getJobDetails(jobId);
        assertEquals("FAILED", jobEntry.getJobEndStatus());
      } finally {
        jobHistory.close();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Test that makes sure information is recorded correctly for a job run with .submit() instead
   * of .run(). Only checks timing info.
   */
  @Test
  public void testSubmit() throws Exception {
    createAndPopulateFooTable();
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();
      JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);

      try {
        // Construct a Producer for this table.
        KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
            .withConf(getConf())
            .withInputTable(fooTableURI)
            .withProducer(EmailDomainProducer.class)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(fooTableURI));
        KijiMapReduceJob mrJob = builder.build();

        LOG.info("About to submit job: " + mrJob.getHadoopJob().getJobName());
        KijiMapReduceJob.Status status = mrJob.submit();
        while (!status.isComplete()) {
          Thread.sleep(1000L);
        }
        assertTrue(status.isSuccessful());
        String jobId = mrJob.getHadoopJob().getJobID().toString();
        LOG.info("Job successfully submitted and run. Id: " + jobId);

        // The job recording takes place in a separate thread, so sleep a bit to give it time to
        // write out.
        Thread.sleep(5000L);

        JobHistoryEntry jobEntry = jobHistory.getJobDetails(jobId);
        assertTrue(jobEntry.getJobStartTime() < jobEntry.getJobEndTime());
      } finally {
        jobHistory.close();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Tests that a job will still run to completion even without an installed job history table.
   */
  @Test
  public void testMissingHistoryTableNonfatal() throws Exception {
    createAndPopulateFooTable();
    // Do not create a job history table.
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final KijiTable fooTable = kiji.openTable("foo");
      try {
        final KijiProduceJobBuilder builder = KijiProduceJobBuilder.create()
            .withConf(getConf())
            .withInputTable(fooTable.getURI())
            .withProducer(EmailDomainProducer.class)
            .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(
                fooTable.getURI()));
        final KijiMapReduceJob mrJob = builder.build();
        assertTrue(mrJob.run());
      } finally {
        fooTable.release();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Tests the output of the job-history tool.
   */
  @Test
  public void testJobHistoryTool() throws Exception {
    createAndPopulateFooTable();
    final Configuration jobConf = getConf();
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    try {
      final KijiURI fooTableURI = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build();

      // Construct two producers for this table.
      final KijiProduceJobBuilder builderEmailDomain = KijiProduceJobBuilder.create()
          .withConf(jobConf)
          .withInputTable(fooTableURI)
          .withProducer(EmailDomainProducer.class)
          .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(fooTableURI));
      KijiMapReduceJob mrJobOne = builderEmailDomain.build();
      KijiMapReduceJob mrJobTwo = builderEmailDomain.build();

      // Run the first produce job.
      String jobOneName = mrJobOne.getHadoopJob().getJobName();
      LOG.info("About to run job: " + jobOneName);
      assertTrue(mrJobOne.run());
      String jobOneId = mrJobOne.getHadoopJob().getJobID().toString();
      LOG.info("Job was run with id: " + jobOneId);

      // Get the StdOut from the job-history tool.
      String jobHistoryStdOut = runTool(new KijiJobHistory(),
          new String[]{"--kiji=" + kiji.getURI(), "--job-id=" + jobOneId,
              "--verbose", "--counter-names", }).getStdout("Utf-8");

      JobHistoryKijiTable jobHistory = JobHistoryKijiTable.open(kiji);

      // Check if the StdOut contains the job history for the first job
      try {
        JobHistoryEntry jobOneEntry = jobHistory.getJobDetails(jobOneId);
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobName()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobId()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobOneEntry.getJobStartTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobOneEntry.getJobEndTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobEndStatus()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobCounters()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobConfiguration()));
        assertTrue(jobHistoryStdOut.contains(
            Arrays.toString(jobOneEntry.getCountersFamily().keySet().toArray())));
        // check for a specific counter to guard against an empty array in the check above
        assertTrue(jobHistoryStdOut.contains(
            "org.kiji.mapreduce.framework.JobHistoryCounters:PRODUCER_ROWS_PROCESSED"));

        // Run the second produce job.
        String jobTwoName = mrJobTwo.getHadoopJob().getJobName();
        LOG.info("About to run job: " + jobTwoName);
        assertTrue(mrJobTwo.run());
        String jobTwoId = mrJobTwo.getHadoopJob().getJobID().toString();
        LOG.info("Job was run with id: " + jobTwoId);

        // Get the StdOut from the job-history tool, again.
        jobHistoryStdOut = runTool(new KijiJobHistory(),
            new String[]{"--kiji=" + kiji.getURI()}).getStdout("Utf-8");

        // Check if the StdOut contains the relevant job histories for each job.
        // Check the first produce job again.
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobName()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobId()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobOneEntry.getJobStartTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobOneEntry.getJobEndTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(jobOneEntry.getJobEndStatus()));

        // Check the second produce job.
        JobHistoryEntry jobTwoEntry = jobHistory.getJobDetails(jobTwoId);
        assertTrue(jobHistoryStdOut.contains(jobTwoEntry.getJobName()));
        assertTrue(jobHistoryStdOut.contains(jobTwoEntry.getJobId()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobTwoEntry.getJobStartTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(new Date(jobTwoEntry.getJobEndTime()).toString()));
        assertTrue(jobHistoryStdOut.contains(jobTwoEntry.getJobEndStatus()));
      } finally {
        jobHistory.close();
      }
    } finally {
      kiji.release();
    }
  }
}
