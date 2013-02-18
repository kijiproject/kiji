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
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.framework.JobHistoryKijiTable;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;

/** A tool that installs a job history table and lets you query individual jobs from it. */
@ApiAudience.Private
public final class KijiJobHistory extends BaseTool {
  @Flag(name="kiji", usage="URI of the Kiji instance to query.")
  private String mKijiURIFlag = KConstants.DEFAULT_URI;

  @Flag(name="job-id", usage="ID of the job to query.")
  private String mJobId = "";

  @Flag(name="verbose", usage="Include counters and configuration for a given job-id.")
  private boolean mVerbose = false;

  @Flag(name="install", usage="Install a job history table.")
  private boolean mInstall = false;

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

    if (mJobId.isEmpty() && !mInstall) {
      throw new RequiredFlagException("job-id=<jobid> or --install");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Kiji kiji = Kiji.Factory.open(mKijiURI);
    try {
      if (mInstall) {
        JobHistoryKijiTable.install(kiji);
      }

      if (!mJobId.isEmpty()) {
        JobHistoryKijiTable jobHistoryTable = JobHistoryKijiTable.open(kiji);
        final KijiRowData data = jobHistoryTable.getJobDetails(mJobId);
        final PrintStream ps = getPrintStream();

        ps.printf("Job:\t\t%s%n", data.getMostRecentValue("info", "jobId"));
        ps.printf("Name:\t\t%s%n", data.getMostRecentValue("info", "jobName"));
        ps.printf("Started:\t%s%n", new Date((Long) data.getMostRecentValue("info", "startTime")));
        ps.printf("Ended:\t\t%s%n", new Date((Long) data.getMostRecentValue("info", "endTime")));
        if (mVerbose) {
          printCounters(data);
          printConfiguration(data);
        }
      }
    } finally {
      kiji.release();
    }

    return SUCCESS;
  }

  /**
   * Prints a representation of the Counters for a Job.
   * @param data A row data containing a serialization of the counters.
   * @throws IOException If there is an error retrieving the counters.
   */
  private void printCounters(KijiRowData data) throws IOException {
    PrintStream ps = getPrintStream();
    Counters counters = new Counters();
    ByteBuffer countersByteBuffer = data.getMostRecentValue("info", "counters");
    counters.readFields(
        new DataInputStream(new ByteArrayInputStream(countersByteBuffer.array())));

    ps.println("Counters:");
    ps.println(counters.toString());
  }

  /**
   * Prints a representation of the Configuration for the Job.
   * @param data A row data containing a serialization of the configuraton.
   * @throws IOException If there is an error retrieving the configuration.
   */
  private void printConfiguration(KijiRowData data) throws IOException {
    OutputStreamWriter osw = null;
    try {
      PrintStream ps = getPrintStream();
      osw = new OutputStreamWriter(ps, "UTF-8");
      Configuration config = new Configuration();
      ByteBuffer configByteBuffer = data.getMostRecentValue("info", "configuration");
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
