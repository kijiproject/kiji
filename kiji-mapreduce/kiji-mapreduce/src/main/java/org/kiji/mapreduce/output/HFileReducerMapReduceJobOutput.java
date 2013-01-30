/**
 * (c) Copyright 2013 WibiData, Inc.
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.context.HFileWriterContext;

/**
 * M/R job output configuration for a job intending to reduce into HFiles.
 *
 * Used for the special case when the user wants a reducer to write HFiles.
 * Reducers can't write HFiles directly, but must write SequenceFiles that will be post-processed
 * (sorted) by an identity Map/Reduce in order to finally write HFiles.
 */
public class HFileReducerMapReduceJobOutput extends MapReduceJobOutput {
  private final HFileMapReduceJobOutput mJobOutput;

  /**
   * Initializes an instance.
   *
   * @param jobOutput Wraps this HFile M/R job output.
   */
  public HFileReducerMapReduceJobOutput(HFileMapReduceJobOutput jobOutput) {
    mJobOutput = jobOutput;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    super.configure(job);  // sets the Hadoop output format

    final Configuration conf = job.getConfiguration();
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mJobOutput.getTable().getURI().toString());

    // Kiji table context:
    conf.setClass(
        KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS,
        HFileWriterContext.class,
        KijiTableContext.class);

    // Set the output path.
    FileOutputFormat.setOutputPath(job, mJobOutput.getPath());

    job.setNumReduceTasks(mJobOutput.getNumReduceTasks());
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    return SequenceFileOutputFormat.class;
  }
}
