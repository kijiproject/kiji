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

package org.kiji.mapreduce;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.tools.KijiJobHistory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;

/** An implementation of a runnable MapReduce job that interacts with Kiji tables. */
@ApiAudience.Framework
public final class KijiMapReduceJob extends InternalMapReduceJob {
  private static final Logger LOG = LoggerFactory.getLogger(KijiMapReduceJob.class);

  // TODO(WIBI-1665): Versions of Hadoop after 20.x add the ability to get start and
  // end times directly from a Job, making these superfluous.
  /** Used to track when the job's execution begins. */
  private long mJobStartTime;
  /** Used to track when the job's execution ends. */
  private long mJobEndTime;

  /**
   * Creates a new <code>KijiMapReduceJob</code> instance.
   *
   * @param job The Hadoop job to run.
   */
  private KijiMapReduceJob(Job job) {
    super(job);
  }

  /**
   * Creates a new <code>KijiMapReduceJob</code>.
   *
   * @param job is a Hadoop {@link Job} that interacts with Kiji and will be wrapped by the new
   *     <code>KijiMapReduceJob</code>.
   * @return A new <code>KijiMapReduceJob</code> backed by a Hadoop {@link Job}.
   */
  public static KijiMapReduceJob create(Job job) {
    return new KijiMapReduceJob(job);
  }

  /**
   * Sets the name of the Kiji instance into the configuration object.
   *
   * @param conf The hadoop configuration object.
   * @param instanceName The name of the kiji instance.
   */
  public static void setInstanceName(Configuration conf, String instanceName) {
    // TODO(WIBI-1666): Validate acceptable kiji name here.
    conf.set(KijiConfiguration.CONF_KIJI_INSTANCE_NAME, instanceName);
  }

  /**
   * Gets the name of the Kiji instance from the configuration object.
   *
   * @param conf The hadoop configuration object.
   * @return The name of the kiji instance.
   */
  public static String getInstanceName(Configuration conf) {
    return conf.get(KijiConfiguration.CONF_KIJI_INSTANCE_NAME,
        KijiConfiguration.DEFAULT_INSTANCE_NAME);
  }

  /**
   * Attempts to record information about a completed job into the KijiJobHistory table.
   * If the attempt fails due to an IOException (a Kiji cannot be made, there is no job history
   * table, its content is corrupted, etc.), we catch the exception and warn the user.
   */
  private void recordJobHistory() {
    Job job = getHadoopJob();
    Kiji kiji = null;
    JobHistoryKijiTable jobHistory = null;
    try {
      kiji = Kiji.Factory.open(new KijiConfiguration(job.getConfiguration(),
          getInstanceName(job.getConfiguration())));
      jobHistory = JobHistoryKijiTable.open(kiji);
      jobHistory.recordJob(job, mJobStartTime, mJobEndTime);
    } catch (IOException e) {
      // Warn about the error, but don't fail the job.
      LOG.warn("Exception while recording job history: " + e
          + " This does not affect the success of the job.");
      LOG.warn("Please run: $KIJI_HOME/bin/kiji util " + KijiJobHistory.class.getCanonicalName()
          + " --install");
    } finally {
      IOUtils.closeQuietly(jobHistory);
      IOUtils.closeQuietly(kiji);
    }
  }

  // Unfortunately, our use of an anonymous inner class in this method confuses checkstyle.
  // We disable it temporarily.
  // CSOFF: VisibilityModifierCheck
  /* {@inheritDoc} */
  @Override
  public Status submit() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStartTime = System.currentTimeMillis();
    final Status jobStatus = super.submit();

    // We use an inline defined thread here to poll jobStatus for completion to update
    // our job history table.
    Thread completionPollingThread = new Thread() {
      private static final long POLL_INTERVAL = 1000L; // One second.
      public void run() {
        try {
          while (!jobStatus.isComplete()) {
            Thread.sleep(POLL_INTERVAL);
          }
          mJobEndTime = System.currentTimeMillis();
          recordJobHistory();
        } catch (IOException e) {
          // If we catch an error while polling, bail out.
          LOG.debug("Error polling jobStatus.");
          return;
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while polling jobStatus.");
          return;
        }
      }
    };

    completionPollingThread.start();
    return jobStatus;
  }
  // CSON: VisibilityModifierCheck

  /* {@inheritDoc} */
  @Override
  public boolean run() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStartTime = System.currentTimeMillis();
    boolean ret = super.run();
    mJobEndTime = System.currentTimeMillis();
    recordJobHistory();
    return ret;
  }
}
