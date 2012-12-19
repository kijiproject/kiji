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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.kiji.mapreduce.MapReduceJobOutput;

/** Base class for MapReduce job output types that write to files. */
public abstract class FileMapReduceJobOutput extends MapReduceJobOutput {
  /** The file system path for the output files. */
  private final Path mFilePath;
  /** The number of output file splits. */
  private final int mNumSplits;

  /**
   * Creates a new <code>FileMapReduceJobOutput</code> instance.
   *
   * @param filePath The file system path for the output files.
   * @param numSplits The number of output file splits.
   */
  protected FileMapReduceJobOutput(Path filePath, int numSplits) {
    mFilePath = filePath;
    mNumSplits = numSplits;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    super.configure(job);
    FileOutputFormat.setOutputPath(job, mFilePath);
    job.setNumReduceTasks(mNumSplits);
  }
}
