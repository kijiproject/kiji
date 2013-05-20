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
import org.kiji.mapreduce.input.impl.WholeFileInputFormat;

/**
 * The class WholeTextFileMapReduceJobInput is used to indicate the usage of entire text
 * files as rows for input to a MapReduce job. Any MapReduce job configured to read whole files
 * from HDFS should expect to receive <code>Text</code> as a key (path to the file) and
 * <code>Text</code> as a value (contents of the file).
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   WholeTextFileMapReduceJobInput must be configured with paths of the files to read:
 * </p>
 * <pre>
 *   <code>
 *     // Get paths of input files in a directory on HDFS.
 *     final FileSystem fs = FileSystem.get(myConf);
 *     final FileStatus[] statuses = fs.listStatus(fs);
 *     final List&lt;Path&gt; paths = new ArrayList&lt;Path&gt;();
 *     for (FileStatus status : statuses) {
 *       paths.add(status.getPath());
 *     }
 *
 *     // Configure the job input.
 *     final MapReduceJobInput wholeFileJobInput =
 *         new WholeTextFileMapReduceJobInput(paths.toArray());
 *   </code>
 * </pre>
 *
 * @see TextMapReduceJobInput
 */
@ApiAudience.Public
public final class WholeTextFileMapReduceJobInput extends FileMapReduceJobInput {

  /**
   * Constructs job input from a varargs of paths to text files.  Accessible via
   * {@link MapReduceJobInputs#newWholeTextFileMapReduceJobInput(org.apache.hadoop.fs.Path...)}.
   *
   * @param paths The paths to the job input files.
   */
  WholeTextFileMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return WholeFileInputFormat.class;
  }
}
