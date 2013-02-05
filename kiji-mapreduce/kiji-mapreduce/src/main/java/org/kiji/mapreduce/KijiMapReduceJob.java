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
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/** An implementation of a runnable MapReduce job that interacts with Kiji tables. */
@ApiAudience.Framework
public final class KijiMapReduceJob extends MapReduceJob {
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
   * Records information about a completed job into the history table of a Kiji instance.
   *
   * If the attempt fails due to an IOException (a Kiji cannot be made, there is no job history
   * table, its content is corrupted, etc.), we catch the exception and warn the user.
   *
   * @param kiji Kiji instance to write the job record to.
   * @throws IOException on I/O error.
   */
  private void recordJobHistory(Kiji kiji) throws IOException {
    Job job = getHadoopJob();
    JobHistoryKijiTable jobHistory = null;
    try {
      jobHistory = JobHistoryKijiTable.open(kiji);
      jobHistory.recordJob(job, mJobStartTime, mJobEndTime);
    } catch (KijiTableNotFoundException ktnfe) {
      if (ktnfe.getTableName().equals(JobHistoryKijiTable.getInstallName())) {
        LOG.warn(
            "Error recording job {} in history table of Kiji instance {}: "
            + "job history table is not installed.\n"
            + "This does not affect the success of job {}.\n"
            + "Please run: kiji job-history --install --kiji={}",
            getHadoopJob().getJobID(),
            kiji.getURI(),
            getHadoopJob().getJobID(),
            kiji.getURI());
      } else {
        throw ktnfe;
      }
    } finally {
      ResourceUtils.closeOrLog(jobHistory);
    }
  }

  /**
   * Records information about a completed job into all relevant Kiji instances.
   *
   * Underlying failures are logged but not fatal.
   */
  private void recordJobHistory() {
    final Set<KijiURI> recorded = Sets.newHashSet();
    final Configuration conf = getHadoopJob().getConfiguration();
    final String[] instanceURIs = new String[] {
        conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI),
        conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI),
    };
    for (String instanceURI : instanceURIs) {
      if (instanceURI != null) {
        try {
          final KijiURI uri = KijiURI.newBuilder(instanceURI).build();
          final Kiji kiji = Kiji.Factory.open(uri, conf);
          try {
            if (!recorded.contains(kiji.getURI())) {
              recordJobHistory(kiji);
            }
          } finally {
            recorded.add(kiji.getURI());
            kiji.release();
          }
        } catch (IOException ioe) {
          LOG.warn(
              "Error recording job {} in history table of Kiji instance {}: {}\n"
              + "This does not affect the success of job {}.",
              getHadoopJob().getJobID(),
              instanceURI,
              ioe,
              getHadoopJob().getJobID());
        }
      }
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
    try {
      recordJobHistory();
    } catch (Throwable thr) {
      thr.printStackTrace();
    }
    return ret;
  }
}
