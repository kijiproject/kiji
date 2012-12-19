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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

public class TestFileMapReduceJobOutput {
  /** A testable concrete subclass of the FileMapReduceJobOutput. */
  public static class ConcreteFileMapReduceJobOutput extends FileMapReduceJobOutput {
    private Class<? extends OutputFormat> mOutputFormatClass;

    public ConcreteFileMapReduceJobOutput(Path filePath, int numSplits,
        Class<? extends OutputFormat> outputFormatClass) {
      super(filePath, numSplits);
      mOutputFormatClass = outputFormatClass;
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
      return mOutputFormatClass;
    }
  }

  @Test
  public void testConfigure() throws ClassNotFoundException, IOException {
    final Path filePath = new Path("foo/bar");
    final int numSplits = 42;
    final Class<? extends OutputFormat> outputFormatClass = TextOutputFormat.class;
    FileMapReduceJobOutput jobOutput = new ConcreteFileMapReduceJobOutput(
        filePath, numSplits, outputFormatClass);

    Job job = new Job();
    jobOutput.configure(job);

    // The output format class should be set in the job configuration.
    assertEquals(outputFormatClass, job.getOutputFormatClass());
    // The file output path should be set in the job configuration.
    assert(FileOutputFormat.getOutputPath(job).toString().endsWith(filePath.toString()));
    // The number of reduce tasks should be set to the number of splits.
    assertEquals(numSplits, job.getNumReduceTasks());
  }
}
