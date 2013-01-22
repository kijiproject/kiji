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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.MapReduceJobBuilder;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.tools.RequiredFlagException;
import org.kiji.schema.tools.VersionValidatedTool;

/**
 * Base class for tools that run MapReduce jobs.
 *
 * @param <B> The type of job builder to use.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class JobTool<B extends MapReduceJobBuilder> extends VersionValidatedTool {

  @Flag(name="input", usage="Input specification '<format>:<location>'")
  protected String mInputFlag = "";

  @Flag(name="output", usage="Output specification '<format>:<location>@<splits>'")
  protected String mOutputFlag = "";

  @Flag(name="overwrite", usage="Overwrite the output path if it exists")
  protected boolean mOverwriteOutput = false;

  @Flag(name="lib", usage="A directory of jars for including user code")
  protected String mLibDir = "";

  @Flag(name="kvstores", usage="KeyValueStore specification XML file to attach to the job")
  protected String mKvStoreFile = "";

  protected JobInputSpec mInputSpec;
  protected JobOutputSpec mOutputSpec;

  @Override
  protected void validateFlags() throws Exception {
    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    // Parse the input and output specs.
    mInputSpec = JobInputSpec.parse(mInputFlag);
    mOutputSpec = JobOutputSpec.parse(mOutputFlag);
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
    // Add user dependency jars if specified.
    if (!mLibDir.isEmpty()) {
      jobBuilder.addJarDirectory(mLibDir);
    }

    // Add user-specified KVStore definitions, if specified.
    if (!mKvStoreFile.isEmpty()) {
      jobBuilder.withStoreBindingsFile(mKvStoreFile);
    }
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    // Create the job builder.
    B jobBuilder = createJobBuilder();
    if (null == jobBuilder) {
      throw new InternalKijiError("Unable to create job builder.");
    }

    configure(jobBuilder);

    // Delete the output files if they already exist and --overwrite=true
    if (mOutputSpec != null
        && mOutputSpec.getFormat() != JobOutputSpec.Format.KIJI
        && null != mOutputSpec.getLocation() && mOverwriteOutput) {
      FileSystem fs = FileSystem.get(getConf());
      Path outPath = new Path(mOutputSpec.getLocation());
      if (fs.exists(outPath)) {
        fs.delete(outPath, true);
      }
    }

    // Run the job.
    return jobBuilder.build().run() ? 0 : 1;
  }
}
