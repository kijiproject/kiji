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

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.OutputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.avro.mapreduce.AvroSequenceFileOutputFormat;

/**
 * The class SequenceFileMapReduceJobOutput is used to indicate the usage of Hadoop
 * sequence files as output for a MapReduce job.
 *
 * <h2>Configuring an output:</h2>
 * <p>
 *   SequenceFileMapReduceJobOutput must be configured with a location to write output
 *   sequence files to and the number of output splits to use:
 * </p>
 * <pre>
 *   <code>
 *     final Path outputPath = new Path("/path/to/output/folder");
 *
 *     // Create a sequence file job output with 10 splits.
 *     final MapReduceJobOutput textOutput =
 *         MapReduceJobOutputs.newSequenceFileMapReduceJobOutput(outputPath, 10);
 *   </code>
 * </pre>
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class SequenceFileMapReduceJobOutput extends FileMapReduceJobOutput {
  /** Default constructor. Accessible via {@link MapReduceJobOutputs}. */
  SequenceFileMapReduceJobOutput() {
  }

  /**
   * Creates a new <code>SequenceFileMapReduceJobOutput</code> instance.
   *
   * @param filePath The file system path for the output files.
   * @param numSplits The number of output file splits.
   */
  SequenceFileMapReduceJobOutput(Path filePath, int numSplits) {
    super(filePath, numSplits);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass() {
    return AvroSequenceFileOutputFormat.class;
  }
}
