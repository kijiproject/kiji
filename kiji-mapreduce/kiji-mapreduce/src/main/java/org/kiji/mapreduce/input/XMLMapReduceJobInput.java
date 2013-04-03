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

package org.kiji.mapreduce.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.input.impl.XMLInputFormat;

/**
 * The class XMLMapReduceJobInput is used to indicate the usage of XML files
 * in HDFS as input to a MapReduce job. Any MapReduce job configured to read from
 * XML files in HDFS should expect to receive <code>LongWritable</code> as a key (line
 * number) and <code>Text</code> as a value (text of the XML record).
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   XMLMapReduceJobInput must be configured with the paths of the files to read from.
 *   Each block from the specified files will be read by one MapReduce split. To setup
 *   reading from a single XML file:
 * </p>
 * <pre>
 *   <code>
 *     final Path inputFile = new Path("/path/to/input");
 *     final MapReduceJobInput xmlJobInput = new XMLMapReduceJobInput(inputFile);
 *   </code>
 * </pre>
 */
@ApiAudience.Public
public final class XMLMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Constructs job input from a varargs of paths to XML files.
   *
   * @param paths The paths to the job input files.
   */
  public XMLMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return XMLInputFormat.class;
  }
}
