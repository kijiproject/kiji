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

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.impl.KijiMappers;
import org.kiji.mapreduce.impl.KijiReducers;
import org.kiji.mapreduce.tools.framework.JobTool;
import org.kiji.mapreduce.tools.framework.MapReduceJobInputFactory;
import org.kiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;

/** Launch an arbitrary KijiMapReduceJob. */
@ApiAudience.Private
public final class KijiLaunchMapReduce extends JobTool<KijiMapReduceJobBuilder> {
  @Flag(name="mapper", usage="Fully-qualified class name of the kiji mapper to run")
  private String mMapperName = "";

  @Flag(name="combiner", usage="Fully-qualifier class name of the combiner to use (optional)")
  private String mCombinerName = "";

  @Flag(name="reducer", usage="Fully-qualified class name of the kiji reducer to run")
  private String mReducerName = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "mapreduce";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a mapreduce job with KijiMapper and KijiReducers";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mMapperName.isEmpty()) {
      throw new RequiredFlagException("mapper");
    }
  }

  @Override
  protected KijiMapReduceJobBuilder createJobBuilder() {
    return KijiMapReduceJobBuilder.create();
  }

  @Override
  protected void configure(KijiMapReduceJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException {
    // Configure lib jars and KV stores:
    super.configure(jobBuilder);

    jobBuilder
        .withConf(getConf())
        .withInput(MapReduceJobInputFactory.create().fromSpaceSeparatedMap(mInputFlag))
        .withOutput(MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag))
        .withMapper(KijiMappers.forName(mMapperName));

    if (!mCombinerName.isEmpty()) {
      jobBuilder.withCombiner(KijiReducers.forName(mCombinerName));
    }
    if (!mReducerName.isEmpty()) {
      jobBuilder.withReducer(KijiReducers.forName(mReducerName));
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiLaunchMapReduce(), args));
  }
}
