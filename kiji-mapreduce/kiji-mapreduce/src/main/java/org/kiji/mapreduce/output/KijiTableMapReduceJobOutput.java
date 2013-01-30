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

package org.kiji.mapreduce.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.schema.KijiTable;

/** MapReduce job output configuration that outputs to a Kiji table. */
@ApiAudience.Private
public abstract class KijiTableMapReduceJobOutput extends MapReduceJobOutput {

  /** Kiji table the job intends to write to. */
  private final KijiTable mTable;

  /** Number of reduce tasks to use. */
  private final int mNumReduceTasks;

  /**
   * Creates a new <code>KijiTableMapReduceJobOutput</code> instance.
   *
   * @param table The kiji table to write output to.
   * @param numReduceTasks The number of reduce tasks to use (use zero if using a producer).
   */
  public KijiTableMapReduceJobOutput(KijiTable table, int numReduceTasks) {
    mTable = table;
    mNumReduceTasks = numReduceTasks;
  }

  /** @return the Kiji table to write to. */
  public KijiTable getTable() {
    return mTable;
  }

  /** @return the number of reducers. */
  public int getNumReduceTasks() {
    return mNumReduceTasks;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // sets Hadoop output format according to getOutputFormatClass()
    super.configure(job);

    final Configuration conf = job.getConfiguration();
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, getTable().getURI().toString());

    job.setNumReduceTasks(getNumReduceTasks());

    // Adds HBase dependency jars to the distributed cache so they appear on the task classpath:
    GenericTableMapReduceUtil.addAllDependencyJars(job);
  }

}
