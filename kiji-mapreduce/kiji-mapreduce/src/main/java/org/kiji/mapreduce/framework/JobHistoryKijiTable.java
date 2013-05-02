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

package org.kiji.mapreduce.framework;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.avro.generated.JobHistoryEntry;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.ResourceUtils;

/**
 * A class providing an API to install and access the job history kiji table.
 *
 * Used in places like KijiMapReduceJob to record information about jobs run through Kiji.
 */
@ApiAudience.Framework
public final class JobHistoryKijiTable implements Closeable {
  /** The name of the table storing a history of completed jobs. */
  private static final String TABLE_NAME = "job_history";
  /** The path to the layout for the table in our resources. */
  private static final String TABLE_LAYOUT_RESOURCE = "/org/kiji/mapreduce/job-history-layout.json";

  /** Column family where job history information is stored. */
  public static final String JOB_HISTORY_FAMILY = "info";
  /** Qualifier where job IDs are stored. */
  public static final String JOB_HISTORY_ID_QUALIFIER = "jobId";
  /** Qualifier where job names are stored. */
  public static final String JOB_HISTORY_NAME_QUALIFIER = "jobName";
  /** Qualifier where job start times are stored. */
  public static final String JOB_HISTORY_START_TIME_QUALIFIER = "startTime";
  /** Qualifier where job end times are stored. */
  public static final String JOB_HISTORY_END_TIME_QUALIFIER = "endTime";
  /** Qualifier where job end statuses are stored. */
  public static final String JOB_HISTORY_END_STATUS_QUALIFIER = "jobEndStatus";
  /** Qualifier where job counters are stored. */
  public static final String JOB_HISTORY_COUNTERS_QUALIFIER = "counters";
  /** Qualifier where job configurations are stored. */
  public static final String JOB_HISTORY_CONFIGURATION_QUALIFIER = "configuration";

  /** The HBaseKijiTable managed by the JobHistoryKijiTable. */
  private final KijiTable mKijiTable;

  /**
   * Opens a JobHistoryKijiTable for a given kiji, installing it if necessary. This method should
   * be matched with a call to {@link #close}.
   *
   * @param kiji The kiji instance to use.
   * @return An opened JobHistoryKijiTable.
   * @throws IOException If there is an error opening the table.
   */
  public static JobHistoryKijiTable open(Kiji kiji) throws IOException {
    return new JobHistoryKijiTable(kiji);
  }

  /**
   * Returns the default name of the job history table.
   *
   * @return The name of the job history table as used by the installer.
   */
  public static String getInstallName() {
    return TABLE_NAME;
  }

  /**
   * Private constructor that opens a new JobHistoryKijiTable, creating it if necessary.
   *
   * @param kiji The kiji instance to retrieve the job history table from.
   * @throws IOException If there's an error opening the underlying HBaseKijiTable.
   */
  private JobHistoryKijiTable(Kiji kiji) throws IOException {
    if (!kiji.getTableNames().contains(TABLE_NAME)) {
      // Try to install the job history table if necessary.
      install(kiji);
    }
    mKijiTable = kiji.openTable(TABLE_NAME);
  }

  /**
   * Writes a job into the JobHistoryKijiTable.
   *
   * @param job The job to save.
   * @param startTime The time the job began, in milliseconds.
   * @param endTime The time the job ended, in milliseconds
   * @throws IOException If there is an error writing to the table.
   */
  public void recordJob(Job job, long startTime, long endTime) throws IOException {
    KijiTableWriter writer = mKijiTable.openTableWriter();
    EntityId jobEntity = mKijiTable.getEntityId(job.getJobID().toString());
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_ID_QUALIFIER,
          startTime, job.getJobID().toString());
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_NAME_QUALIFIER,
          startTime, job.getJobName());
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_START_TIME_QUALIFIER,
          startTime, startTime);
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_END_TIME_QUALIFIER,
          startTime, endTime);
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_END_STATUS_QUALIFIER,
          startTime, job.isSuccessful() ? "SUCCEEDED" : "FAILED");
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_COUNTERS_QUALIFIER,
          startTime, job.getCounters().toString());
      job.getConfiguration().writeXml(baos);
      writer.put(jobEntity, JOB_HISTORY_FAMILY, JOB_HISTORY_CONFIGURATION_QUALIFIER,
          startTime, baos.toString("UTF-8"));
    } finally {
      ResourceUtils.closeOrLog(writer);
    }
  }

  /**
   * Install the job history table into a Kiji instance.
   *
   * @param kiji The Kiji instance to install this table in.
   * @throws IOException If there is an error.
   */
  public static void install(Kiji kiji) throws IOException {
    kiji.createTable(
        KijiTableLayout.createFromEffectiveJsonResource(TABLE_LAYOUT_RESOURCE).getDesc());
  }

  /**
   * Get the saved information for a particular JobID.
   *
   * @param jobId The id of the job to retrieve.
   * @return A KijiRowData containing all the information for the requested Job.
   * @throws IOException If there is an IO error retrieving the data.
   */
  public JobHistoryEntry getJobDetails(String jobId) throws IOException {
    final KijiTableReader reader = mKijiTable.openTableReader();
    final KijiDataRequest request = KijiDataRequest.create("info");
    final KijiRowData data;
    try {
      data = reader.get(mKijiTable.getEntityId(jobId), request);
    } finally {
      reader.close();
    }

    final Map<String, String> extendedInfo = new HashMap<String, String>();
    for (String qualifier : data.getQualifiers("extendedInfo")) {
      extendedInfo.put(qualifier, data.<String>getMostRecentValue("extendedInfo", qualifier));
    }

    return JobHistoryEntry.newBuilder()
        .setJobId(data.getMostRecentValue("info", "jobId").toString())
        .setJobName(data.getMostRecentValue("info", "jobName").toString())
        .setJobStartTime(data.<Long>getMostRecentValue("info", "startTime"))
        .setJobEndTime(data.<Long>getMostRecentValue("info", "endTime"))
        .setJobEndStatus(data.getMostRecentValue("info", "jobEndStatus").toString())
        .setJobCounters(data.getMostRecentValue("info", "counters").toString())
        .setJobConfiguration(data.getMostRecentValue("info", "configuration").toString())
        .setExtendedInfo(extendedInfo)
        .build();
  }

  /**
   * Get the saved information for all JobIDs.
   *
   * @return A KijiRowScanner containing details for all the JobIDs.
   * @throws IOException If there is an IO error retrieving the data.
   */
  public KijiRowScanner getJobScanner() throws IOException {
    KijiTableReader wtr = mKijiTable.openTableReader();
    KijiDataRequest wdr = KijiDataRequest.create("info");

    try {
      return wtr.getScanner(wdr);
    } finally {
      ResourceUtils.closeOrLog(wtr);
    }
  }

  @Override
  public void close() throws IOException {
    mKijiTable.release();
  }
}
