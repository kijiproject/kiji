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

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/** A Hadoop MapReduce job that runs Kiji mappers and reducers. */
@ApiAudience.Public
@Inheritance.Sealed
public abstract class MapReduceJob {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceJob.class);

  /** The wrapped Hadoop Job. */
  private final Job mJob;

  /**
   * Wraps a Hadoop job to be run.
   *
   * @param job The Hadoop job to be run.
   */
  MapReduceJob(Job job) {
    mJob = job;
  }

  /**
   * The status of a job that was started asynchronously using {@link #submit()}.
   */
  public static class Status {
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
   * Gives access the underlying Hadoop Job object.
   *
   * @return The Hadoop Job object.
   */
  public Job getHadoopJob() {
    return mJob;
  }

  /**
   * Starts the job and return immediately.
   *
   * @return The job status. This can be queried for completion and success or failure.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public Status submit() throws ClassNotFoundException, IOException, InterruptedException {
    LOG.debug("Submitting job");
    mJob.submit();
    return new Status(mJob);
  }

  /**
   * Runs the job (blocks until it is complete).
   *
   * @return Whether the job was successful.
   * @throws ClassNotFoundException If a required class cannot be found on the classpath.
   * @throws IOException If there is an IO error.
   * @throws InterruptedException If the thread is interrupted.
   */
  public boolean run() throws ClassNotFoundException, IOException, InterruptedException {
    LOG.debug("Running job");
    return mJob.waitForCompletion(true);
  }
}
