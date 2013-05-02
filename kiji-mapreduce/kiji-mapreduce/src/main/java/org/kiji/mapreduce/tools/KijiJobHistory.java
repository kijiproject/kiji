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

package org.kiji.mapreduce.tools;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.avro.generated.JobHistoryEntry;
import org.kiji.mapreduce.framework.JobHistoryKijiTable;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;

/** A tool that installs a job history table and lets you query individual jobs from it. */
@ApiAudience.Private
public final class KijiJobHistory extends BaseTool {
  @Flag(name="kiji", usage="URI of the Kiji instance to query.")
  private String mKijiURIFlag = KConstants.DEFAULT_URI;

  @Flag(name="job-id", usage="ID of the job to query.")
  private String mJobId = "";

  @Flag(name="verbose", usage="Include counters and configuration for a given job-id.")
  private boolean mVerbose = false;

  /** URI of the Kiji instance to query. */
  private KijiURI mKijiURI = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "job-history";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Inspect or manipulate the MapReduce job history table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Admin";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mKijiURIFlag != null) && !mKijiURIFlag.isEmpty(),
        "Specify a Kiji instance with --kiji=kiji://hbase-address/kiji-instance");
    mKijiURI = KijiURI.newBuilder(mKijiURIFlag).build();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Kiji kiji = Kiji.Factory.open(mKijiURI);
    try {
      JobHistoryKijiTable jobHistoryTable = JobHistoryKijiTable.open(kiji);
      if (!mJobId.isEmpty()) {
        JobHistoryEntry data = jobHistoryTable.getJobDetails(mJobId);
        printEntry(data);
      } else {
        KijiRowScanner jobScanner = jobHistoryTable.getJobScanner();
        for (KijiRowData data : jobScanner) {
          final Map<String, String> extendedInfo = new HashMap<String, String>();
          for (String qualifier : data.getQualifiers("extendedInfo")) {
            extendedInfo.put(
                qualifier, data.getMostRecentValue("extendedInfo", qualifier).toString());
          }
          printEntry(JobHistoryEntry.newBuilder()
              .setJobId(data.getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_ID_QUALIFIER).toString())
              .setJobName(data.getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_NAME_QUALIFIER).toString())
              .setJobStartTime(data.<Long>getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_START_TIME_QUALIFIER))
              .setJobEndTime(data.<Long>getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_END_TIME_QUALIFIER))
              .setJobEndStatus(data.getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_END_STATUS_QUALIFIER).toString())
              .setJobCounters(data.getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_COUNTERS_QUALIFIER).toString())
              .setJobConfiguration(data.getMostRecentValue(JobHistoryKijiTable.JOB_HISTORY_FAMILY,
                  JobHistoryKijiTable.JOB_HISTORY_CONFIGURATION_QUALIFIER).toString())
              .setExtendedInfo(extendedInfo)
              .build());
          getPrintStream().printf("%n");
        }
        jobScanner.close();
      }
      jobHistoryTable.close();
    } finally {
      kiji.release();
    }

    return SUCCESS;
  }

  /**
   * Prints a job details.
   *
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   * @throws IOException If there is an error retrieving the counters.
   */
  private void printEntry(JobHistoryEntry entry) throws IOException {
    final PrintStream ps = getPrintStream();
    ps.printf("Job:\t\t%s%n", entry.getJobId());
    ps.printf("Name:\t\t%s%n", entry.getJobName());
    ps.printf("Started:\t%s%n", new Date(entry.getJobStartTime()));
    ps.printf("Ended:\t\t%s%n", new Date(entry.getJobEndTime()));
    ps.printf("End Status:\t\t%s%n", entry.getJobEndStatus());
    if (mVerbose) {
      printCounters(entry);
      printConfiguration(entry);
    }
  }

  /**
   * Prints a representation of the Counters for a Job.
   *
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   * @throws IOException If there is an error retrieving the counters.
   */
  private void printCounters(JobHistoryEntry entry) throws IOException {
    PrintStream ps = getPrintStream();
    Counters counters = new Counters();
    ByteBuffer countersByteBuffer = ByteBuffer.wrap(entry.getJobCounters().getBytes("utf-8"));
    counters.readFields(
        new DataInputStream(new ByteArrayInputStream(countersByteBuffer.array())));

    ps.println("Counters:");
    ps.println(counters.toString());
  }

  /**
   * Prints a representation of the Configuration for the Job.
   * @param entry a JobHistoryEntry retrieved from the JobHistoryTable.
   * @throws IOException If there is an error retrieving the configuration.
   */
  private void printConfiguration(JobHistoryEntry entry) throws IOException {
    OutputStreamWriter osw = null;
    try {
      PrintStream ps = getPrintStream();
      osw = new OutputStreamWriter(ps, "UTF-8");
      Configuration config = new Configuration();
      ByteBuffer configByteBuffer = ByteBuffer.wrap(entry.getJobConfiguration().getBytes("utf-8"));
      config.readFields(new DataInputStream(new ByteArrayInputStream(configByteBuffer.array())));

      ps.print("Configuration:\n");
      Configuration.dumpConfiguration(config, osw);
      ps.print("\n");
    } finally {
      IOUtils.closeQuietly(osw);
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiJobHistory(), args));
  }
}
