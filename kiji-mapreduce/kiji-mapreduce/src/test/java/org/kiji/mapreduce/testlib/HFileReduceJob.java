/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiTransformJobBuilder;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.input.SequenceFileMapReduceJobInput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/** Processes the output of a KijiTableReducer that intends to generate HFiles. */
public final class HFileReduceJob {
  @Flag(
      name = "input-path",
      usage = "Path to the input SequenceFile<HFileKeyValue, NullWritable>.")
  private String mInputPath;

  @Flag(
      name = "output-path",
      usage = "Write the output HFiles under this path.")
  private String mOutputPath;

  @Flag(
      name = "output-table",
      usage = "KijiURI of the target table to write to.")
  private String mOutputTable;

  @Flag(
      name = "nsplits",
      usage = "Number of splits")
  private int mNumSplits = 0;

  /** An identity mapper. */
  private static class IdentityMapper
      extends Mapper<HFileKeyValue, NullWritable, HFileKeyValue, NullWritable>
      implements KijiMapper {

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return HFileKeyValue.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }

    /** {@inheritDoc} */
    @Override
    protected void map(HFileKeyValue key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public void run(String[] args) throws Exception {
    FlagParser.init(this,  args);

    final Path inputPath = new Path(mInputPath);
    final Path outputPath = new Path(mOutputPath);
    final KijiURI tableURI = KijiURI.newBuilder(mOutputTable).build();
    final Kiji kiji = Kiji.Factory.open(tableURI);
    final KijiTable table = kiji.openTable(tableURI.getTable());

    final KijiConfiguration kijiConf =
        new KijiConfiguration(kiji.getConf(), kiji.getURI().getInstance());

    final MapReduceJob mrjob = KijiTransformJobBuilder.create()
        .withKijiConfiguration(kijiConf)
        .withInput(new SequenceFileMapReduceJobInput(inputPath))
        .withOutput(new HFileMapReduceJobOutput(table, outputPath, mNumSplits))
        .withMapper(IdentityMapper.class)
        .withReducer(IdentityReducer.class)
        .build();

    if (!mrjob.run()) {
      System.err.println("Job failed.");
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    new HFileReduceJob().run(args);
  }
}
