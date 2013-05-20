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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.kiji.annotations.ApiAudience;

/**
 * The class TextMapReduceJobInput is used to indicate the usage of lines in text files
 * in HDFS as input to a MapReduce job. Any MapReduce job configured to read lines from
 * text files in HDFS should expect to receive <code>LongWritable</code> as a key (line
 * number) and <code>Text</code> as a value (text of the line).
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   TextMapReduceJobInput must be configured with the paths of the files to read from.
 *   Each block from the specified files will be read by one MapReduce split. To setup
 *   reading from a single text file:
 * </p>
 * <pre>
 *   <code>
 *     final Path inputFile = new Path("/path/to/input");
 *     final MapReduceJobInput textJobInput = new TextMapReduceJobInput(inputFile);
 *   </code>
 * </pre>
 * @see WholeTextFileMapReduceJobInput
 */
@ApiAudience.Public
public final class TextMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Constructs job input from a varargs of paths to text files.  Accessible via
   * {@link MapReduceJobInputs#newTextMapReduceJobInput(org.apache.hadoop.fs.Path...)}.
   *
   * @param paths The paths to the job input files.
   */
  TextMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return TextInputFormat.class;
  }
}
