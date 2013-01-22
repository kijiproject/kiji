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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.MapReduceJobInput;

/** Base class for map reduce job input that is read from files. */
@ApiAudience.Public
@Inheritance.Sealed
public abstract class FileMapReduceJobInput extends MapReduceJobInput {
  /** The path to the input files. */
  private final Path[] mPaths;

  /**
   * Creates a new <code>FileMapReduceJobInput</code> instance.
   *
   * @param paths The paths to the input files.
   */
  FileMapReduceJobInput(Path... paths) {
    mPaths = paths;
  }

  @Override
  public void configure(Job job) throws IOException {
    super.configure(job);
    FileInputFormat.setInputPaths(job, mPaths);
  }
}
