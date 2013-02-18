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

package org.kiji.mapreduce.output.impl;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.tools.framework.JobIOConfKeys;
import org.kiji.schema.KijiURI;

/** MapReduce job output configuration that outputs to a Kiji table. */
@ApiAudience.Private
public abstract class KijiTableMapReduceJobOutput extends MapReduceJobOutput {

  /** URI of the output table. */
  private KijiURI mTableURI;

  /** Number of reduce tasks to use. */
  private int mNumReduceTasks;

  /** Default constructor. Do not use directly. */
  protected KijiTableMapReduceJobOutput() {
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    mTableURI = KijiURI.newBuilder(params.get(JobIOConfKeys.TABLE_KEY)).build();
    mNumReduceTasks = Integer.parseInt(params.get(JobIOConfKeys.NSPLITS_KEY));
  }

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param tableURI The kiji table to write output to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  public KijiTableMapReduceJobOutput(KijiURI tableURI, int numReduceTasks) {
    mTableURI = tableURI;
    mNumReduceTasks = numReduceTasks;
  }

  /** @return the number of reducers. */
  public int getNumReduceTasks() {
    return mNumReduceTasks;
  }

  /** @return the URI of the configured output table. */
  public KijiURI getOutputTableURI() {
    return mTableURI;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format according to getOutputFormatClass()
    super.configure(job);

    final Configuration conf = job.getConfiguration();
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mTableURI.toString());

    job.setNumReduceTasks(getNumReduceTasks());

    // Adds HBase dependency jars to the distributed cache so they appear on the task classpath:
    GenericTableMapReduceUtil.addAllDependencyJars(job);
  }

}
