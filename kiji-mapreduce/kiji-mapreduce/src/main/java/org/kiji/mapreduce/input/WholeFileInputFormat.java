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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * An input format for reading multiple files in a single map task.
 * Exactly one (unsplit) file will be read by each record passed to the mapper.
 * Records are constructed from WholeFileRecordReaders.
 */
public class WholeFileInputFormat extends CombineFileInputFormat<Text, Text> {
  /**
   * Guarantees that no file is split.
   *
   * @param context The JobContext.  Unused.
   * @param filename The path to this file.  Unused.
   * @return false so that no files are split.
   */
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // This, combined with returning the entire file as a record
    // guarantees that you get exactly one file for each record.
    return false;
  }

  /**
   * Creates a CombineFileRecordReader to read each file assigned to this InputSplit.
   * Note, that unlike ordinary InputSplits, split must be a CombineFileSplit, and therefore
   * is expected to specify multiple files.
   *
   * @param split The InputSplit to read.  Throws an IllegalArgumentException if this is
   *        not a CombineFileSplit.
   * @param context The context for this task.
   * @return a CombineFileRecordReader to process each file in split.
   *         It will read each file with a WholeFileRecordReader.
   * @throws IOException if there is an error.
   */
  @Override
  public RecordReader<Text, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    if (!(split instanceof CombineFileSplit)) {
      throw new IllegalArgumentException("split must be a CombineFileSplit");
    }
    return new CombineFileRecordReader<Text, Text>((CombineFileSplit) split,
        context, WholeFileRecordReader.class);
  }
}
