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
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.KijiTransformJobBuilder;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;

/** Transforms data using a Kiji MapReduce job. */
@ApiAudience.Private
public final class KijiTransform extends JobTool<KijiTransformJobBuilder> {
  @Flag(name="mapper", usage="Fully-qualified class name of the mapper to run")
  private String mMapperName = "";

  @Flag(name="combiner", usage="Fully-qualifier class name of the combiner to use (optional)")
  private String mCombinerName = "";

  @Flag(name="reducer", usage="Fully-qualified class name of the reducer to run")
  private String mReducerName = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "transform";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Transform data using MapReduce";
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
  protected KijiTransformJobBuilder createJobBuilder() {
    return KijiTransformJobBuilder.create();
  }

  @Override
  protected void configure(KijiTransformJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    super.configure(jobBuilder);
    jobBuilder.withKijiConfiguration(new KijiConfiguration(getConf(), getURI().getInstance()));

    // Input.
    MapReduceJobInputFactory inputFactory = MapReduceJobInputFactory.create();
    jobBuilder.withInput(inputFactory.createFromInputSpec(mInputSpec));

    // Mapper.
    jobBuilder.withMapper(Class.forName(mMapperName).asSubclass(KijiMapper.class));

    // Combiner.
    if (!mCombinerName.isEmpty()) {
      jobBuilder.withCombiner(Class.forName(mCombinerName).asSubclass(KijiReducer.class));
    }

    // Reducer.
    if (!mReducerName.isEmpty()) {
      jobBuilder.withReducer(Class.forName(mReducerName).asSubclass(KijiReducer.class));
    }

    // Output.
    MapReduceJobOutputFactory outputFactory = MapReduceJobOutputFactory.create();
    jobBuilder.withOutput(outputFactory.createFromOutputSpec(mOutputSpec));
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiTransform(), args));
  }
}
