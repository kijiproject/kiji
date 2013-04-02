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
import java.util.Collections;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.framework.JobHistoryKijiTable;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/** A runnable MapReduce job that interacts with Kiji tables. */
@ApiAudience.Public
public final class KijiMapReduceJob {
  private static final Logger LOG = LoggerFactory.getLogger(KijiMapReduceJob.class);

  /** The wrapped Hadoop Job. */
  private final Job mJob;
  // TODO(KIJIMR-92): Versions of Hadoop after 20.x add the ability to get start and
  // end times directly from a Job, making these superfluous.
  /** Used to track when the job's execution begins. */
  private long mJobStartTime;
  /** Used to track when the job's execution ends. */
  private long mJobEndTime;
  /** Completion polling thread. */
  private Thread mCompletionPollingThread;
  /** Whether the job is currently started. */
  private boolean mJobStarted = false;

  /**
   * Creates a new <code>KijiMapReduceJob</code> instance.
   *
   * @param job The Hadoop job to run.
   */
  private KijiMapReduceJob(Job job) {
    mJob = Preconditions.checkNotNull(job);
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
   * Join the completion polling thread and block until it exits.
   *
   * @throws InterruptedException if the completion polling thread is interrupted.
   * @throws IOException in case of an IO error when querying job success.
   * @return Whether the job completed successfully
   */
  public boolean join() throws InterruptedException, IOException {
    Preconditions.checkState(mJobStarted,
        "Cannot join completion polling thread because the job is not running.");
    mCompletionPollingThread.join();
    return mJob.isSuccessful();
  }

  /**
   * The status of a job that was started asynchronously using {@link #submit()}.
   */
  public static final class Status {
    /** The Job whose status is being tracked. */
    private final Job mJob;

    /**
     * Constructs a <code>Status</code> around a Hadoop job.
     *
     * @param job The Hadoop job being run.
     */
    protected Status(Job job) {
      mJob = job;
    }

    /**
     * Determines whether the job has completed.
     *
     * @return Whether the job has completed.
     * @throws IOException If there is an error querying the job.
     */
    public boolean isComplete() throws IOException {
      return mJob.isComplete();
    }

    /**
     * Determines whether the job was successful.  The return value is undefined if the
     * job has not yet completed.
     *
     * @return Whether the job was successful.
     * @throws IOException If there is an error querying the job.
     */
    public boolean isSuccessful() throws IOException {
      return mJob.isSuccessful();
    }
  }

  /**
   * Records information about a completed job into the history table of a Kiji instance.
   *
   * If the attempt fails due to an IOException (a Kiji cannot be made, there is no job history
   * table, its content is corrupted, etc.), we catch the exception and warn the user.
   *
   * @param kiji Kiji instance to write the job record to.
   * @throws IOException on I/O error.
   */
  private void recordJobHistory(Kiji kiji) throws IOException {
    final Job job = getHadoopJob();
    JobHistoryKijiTable jobHistory = null;
    try {
      jobHistory = JobHistoryKijiTable.open(kiji);
      jobHistory.recordJob(job, mJobStartTime, mJobEndTime);
    } catch (IOException ioe) {
      // We swallow errors for recording jobs, because it's a non-fatal error for the task.
        LOG.warn(
            "Error recording job {} in history table of Kiji instance {}:\n"
            + "{}\n"
            + "This does not affect the success of job {}.\n",
            getHadoopJob().getJobID(), kiji.getURI(),
            StringUtils.stringifyException(ioe),
            getHadoopJob().getJobID());
    } finally {
      IOUtils.closeQuietly(jobHistory);
    }
  }

  /**
   * Records information about a completed job into all relevant Kiji instances.
   *
   * Underlying failures are logged but not fatal.
   */
  private void recordJobHistory() {
    final Configuration conf = getHadoopJob().getConfiguration();
    final Set<KijiURI> instanceURIs = Sets.newHashSet();
    if (conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI) != null) {
        instanceURIs.add(KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI))
            .withTableName(null)
            .withColumnNames(Collections.<String>emptyList())
            .build());
    }
    if (conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI) != null) {
        instanceURIs.add(KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI))
            .withTableName(null)
            .withColumnNames(Collections.<String>emptyList())
            .build());
    }

    for (KijiURI instanceURI : instanceURIs) {
      if (instanceURI != null) {
        try {
          final Kiji kiji = Kiji.Factory.open(instanceURI, conf);
          try {
            recordJobHistory(kiji);
          } finally {
            kiji.release();
          }
        } catch (IOException ioe) {
          LOG.warn(
              "Error recording job {} in history table of Kiji instance {}: {}\n"
              + "This does not affect the success of job {}.",
              getHadoopJob().getJobID(),
              instanceURI,
              ioe.getMessage(),
              getHadoopJob().getJobID());
        }
      }
    }
  }

  /**
   * Gives access the underlying Hadoop Job object.
   *
   * @return The Hadoop Job object.
   */
  public Job getHadoopJob() {
    return mJob;
  }

  // Unfortunately, our use of an anonymous inner class in this method confuses checkstyle.
  // We disable it temporarily.
  // CSOFF: VisibilityModifierCheck
  /**
   * Starts the job and return immediately.
   *
   * @return The job status. This can be queried for completion and success or failure.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public Status submit() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStarted = true;
    mJobStartTime = System.currentTimeMillis();
    LOG.debug("Submitting job");
    mJob.submit();
    final Status jobStatus = new Status(mJob);

    // We use an inline defined thread here to poll jobStatus for completion to update
    // our job history table.
    mCompletionPollingThread = new Thread() {
      // Polling interval in milliseconds.
      private final int mPollInterval =
          mJob.getConfiguration().getInt(KijiConfKeys.KIJI_MAPREDUCE_POLL_INTERVAL, 1000);

      public void run() {
        try {
          while (!jobStatus.isComplete()) {
            Thread.sleep(mPollInterval);
          }
          mJobEndTime = System.currentTimeMillis();
          // IOException in recordJobHistory() are caught and logged.
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

    mCompletionPollingThread.setDaemon(true);
    mCompletionPollingThread.start();
    return jobStatus;
  }

  // CSON: VisibilityModifierCheck
  /**
   * Runs the job (blocks until it is complete).
   *
   * @return Whether the job was successful.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public boolean run() throws ClassNotFoundException, IOException, InterruptedException {
    mJobStartTime = System.currentTimeMillis();
    LOG.debug("Running job");
    boolean ret = mJob.waitForCompletion(true);
    mJobEndTime = System.currentTimeMillis();
    try {
      recordJobHistory();
    } catch (Throwable thr) {
      thr.printStackTrace();
    }
    return ret;
  }
}
