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

package org.kiji.mapreduce.tools;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.MapReduceJobBuilder;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.schema.tools.BaseTool;

/**
 * Base class for tools that run MapReduce jobs.
 *
 * @param <B> The type of job builder to use.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class JobTool<B extends MapReduceJobBuilder> extends BaseTool {

  // TODO(KIJIMR-62): Better usage doc:
  @Flag(name="input",
      usage="Job input specification: --input=\"format=<input-format> ...\".")
  protected String mInputFlag = "";

  @Flag(name="output",
      usage="Job output specification: --output=\"format=<output-format> nsplits=N ...\"")
  protected String mOutputFlag = "";

  @Flag(name="lib", usage="A directory of jars for including user code")
  protected String mLibDir = "";

  @Flag(name="kvstores", usage="KeyValueStore specification XML file to attach to the job")
  protected String mKvStoreFile = "";

  private MapReduceJobInput mJobInput;
  private MapReduceJobOutput mJobOutput;

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    Preconditions.checkArgument(!mInputFlag.isEmpty(),
        "Specify an input to the job with --input=...");
    Preconditions.checkArgument(!mOutputFlag.isEmpty(),
        "Specify an output to the job with --output=...");
    mJobInput = MapReduceJobInputFactory.create().fromSpaceSeparatedMap(mInputFlag);
    mJobOutput = MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
  }

  /**
   * Creates a new job builder.
   *
   * @return A new job builder.
   */
  protected abstract B createJobBuilder();

  /**
   * Configures the job builder by applying the flags from the user.
   *
   * @param jobBuilder The job builder to configure.
   * @throws ClassNotFoundException If a mapper, reducer, or other user class can not be loaded.
   * @throws IOException if there is an error reading configuration input data.
   * @throws JobIOSpecParseException If the input or output for the job can not configured.
   */
  protected void configure(B jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    // Use default environment configuration:
    jobBuilder.withConf(getConf());

    // Add user dependency jars if specified.
    if (!mLibDir.isEmpty()) {
      jobBuilder.addJarDirectory(mLibDir);
    }

    // Add user-specified KVStore definitions, if specified.
    if (!mKvStoreFile.isEmpty()) {
      jobBuilder.withStoreBindingsFile(mKvStoreFile);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final B jobBuilder = createJobBuilder();
    Preconditions.checkNotNull(jobBuilder, "Internal error: unable to create job builder?");
    configure(jobBuilder);
    return jobBuilder.build().run() ? 0 : 1;
  }

  /** @return the job input. */
  protected MapReduceJobInput getJobInput() {
    return mJobInput;
  }

  /** @return the job output. */
  protected MapReduceJobOutput getJobOutput() {
    return mJobOutput;
  }
}
