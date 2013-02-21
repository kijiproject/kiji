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
import org.kiji.avro.mapreduce.AvroKeyValueInputFormat;

/**
 * The class AvroKeyValueMapReduceJobInput is used to indicate the usage of Avro container
 * files containing key-value pairs as input to a MapReduce job. Any MapReduce job configured
 * to read from an Avro container file using this job input should expect to receive key-value
 * pairs of the same type that were used to write the container file.
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   AvroKeyValueMapReduceJobInput must be configured with the paths of Avro container files
 *   to read from:
 * </p>
 * <pre>
 *   <code>
 *     final Path avroContainerFile = new Path("/path/to/avro/container/file");
 *     final MapReduceJobInput avroJobInput = new AvroKeyMapReduceJobInput(avroContainerFile);
 *   </code>
 * </pre>
 * @see AvroKeyMapReduceJobInput
 */
@ApiAudience.Public
public final class AvroKeyValueMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Constructs job input from a varargs of paths to Avro container files.
   *
   * @param paths The paths to the avro input files.
   */
  public AvroKeyValueMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return AvroKeyValueInputFormat.class;
  }
}
