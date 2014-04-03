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

package org.kiji.mapreduce.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.avro.mapreduce.AvroSequenceFileInputFormat;

/**
 * The class SequenceFileMapReduceJobInput is used to indicate the usage of a Hadoop
 * sequence file as input to a MapReduce job. Any MapReduce job configured to read from a
 * sequence file should expect to receive key-value pairs of same type that were used to
 * write the sequence file.
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   SequenceFileMapReduceJobInput must be configured with the paths of the sequence files
 *   to read from:
 * </p>
 * <pre>
 *   <code>
 *     final Path sequenceFile = new Path("/path/to/sequence/file");
 *     final MapReduceJobInput seqFileJobInput =
 *         MapReduceJobInputs.newSequenceFileMapReduceJobInput(sequenceFile);
 *   </code>
 * </pre>
 */
@ApiAudience.Public
@ApiStability.Stable
public final class SequenceFileMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Constructs job input from a varargs of paths to sequence files.  Accessible via
   * {@link MapReduceJobInputs#newSequenceFileMapReduceJobInput(org.apache.hadoop.fs.Path...)}.
   *
   * @param paths The paths to the input sequence files.
   */
  SequenceFileMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return AvroSequenceFileInputFormat.class;
  }
}
