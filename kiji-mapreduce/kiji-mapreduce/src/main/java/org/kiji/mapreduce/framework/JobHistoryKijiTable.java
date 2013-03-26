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

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
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
      writer.put(jobEntity, "info", "jobId", startTime, job.getJobID().toString());
      writer.put(jobEntity, "info", "jobName", startTime, job.getJobName());
      writer.put(jobEntity, "info", "startTime", startTime, startTime);
      writer.put(jobEntity, "info", "endTime", startTime, endTime);
      writer.put(jobEntity, "info", "jobEndStatus", startTime,
          job.isSuccessful() ? "SUCCEEDED" : "FAILED");
      writer.put(jobEntity, "info", "counters", startTime, job.getCounters().toString());
      job.getConfiguration().writeXml(baos);
      writer.put(jobEntity, "info", "configuration", startTime, baos.toString("UTF-8"));
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
  public KijiRowData getJobDetails(String jobId) throws IOException {
    // TODO: This API is too low level. Eventually we should return a JobHistoryEntry object which
    // has fields for the explicit attributes and a map for any implicit attributes.
    KijiTableReader wtr = mKijiTable.openTableReader();
    KijiDataRequest wdr = KijiDataRequest.create("info");

    try {
      return wtr.get(mKijiTable.getEntityId(jobId), wdr);
    } finally {
      ResourceUtils.closeOrLog(wtr);
    }
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
