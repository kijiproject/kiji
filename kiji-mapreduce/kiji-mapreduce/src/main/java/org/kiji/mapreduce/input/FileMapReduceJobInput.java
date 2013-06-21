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
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.tools.framework.JobIOConfKeys;

/** Base class for MapReduce job input that is read from files. */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public abstract class FileMapReduceJobInput extends MapReduceJobInput {
  /** The path to the input files. */
  private Path[] mPaths;

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    Preconditions.checkArgument(params.containsKey(JobIOConfKeys.FILE_PATH_KEY),
        "File job input is missing parameter '%s'.", JobIOConfKeys.FILE_PATH_KEY);
    final List<Path> paths = Lists.newArrayList();
    for (String path : params.get(JobIOConfKeys.FILE_PATH_KEY).split(",")) {
      paths.add(new Path(path));
    }
    mPaths = paths.toArray(new Path[paths.size()]);
  }

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
