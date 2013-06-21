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

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/** Base class for types of input to a MapReduce job. */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public abstract class MapReduceJobInput {

  /**
   * Initializes the job input with the given parameters.
   *
   * @param params Parameters, usually parsed from the command-line.
   * @throws IOException on I/O error.
   */
  public abstract void initialize(Map<String, String> params) throws IOException;

  /**
   * Configure a job to use this type of input for the MapReduce.
   *
   * @param job The job to configure with input.
   * @throws IOException If there is an error during configuration.
   */
  public void configure(Job job) throws IOException {
    job.setInputFormatClass(getInputFormatClass());
  }

  /**
   * Returns the class to use for this MapReduce input format type.
   *
   * @return The MR input format class.
   */
  protected abstract Class<? extends InputFormat> getInputFormatClass();
}
