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

package org.kiji.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/** Describe class <code>MapReduceJobOutput</code> here. */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public abstract class MapReduceJobOutput {
  /**
   * Initializes the job output from command-line parameters.
   *
   * @param params Initialization parameters.
   * @throws IOException on I/O error.
   */
  public abstract void initialize(Map<String, String> params) throws IOException;

  /**
   * Configures the output for a MapReduce job.
   *
   * @param job The job to configure.
   * @throws IOException If there is an error.
   */
  public void configure(Job job) throws IOException {
    job.setOutputFormatClass(getOutputFormatClass());
  }

  /**
   * Gets the Hadoop MapReduce output format class.
   *
   * @return The job output format class.
   */
  protected abstract Class<? extends OutputFormat> getOutputFormatClass();
}
