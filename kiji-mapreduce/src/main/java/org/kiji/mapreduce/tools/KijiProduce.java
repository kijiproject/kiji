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

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.common.flags.IllegalFlagValueException;
import org.kiji.mapreduce.KijiProduceJobBuilder;
import org.kiji.mapreduce.KijiProducer;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.schema.tools.KijiToolLauncher;

/** Program for running a Kiji producer in a MapReduce job. */
public class KijiProduce extends KijiJobTool<KijiProduceJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiProduce.class);

  @Flag(name = "producer", usage = "Fully-qualified class name of the producer to run")
  private String mProducerName = "";

  @Flag(name = "num-threads", usage = "Positive integer number of threads to use")
  private int mNumThreadsPerMapper = 1;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "produce";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a KijiProducer over a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mProducerName.isEmpty()) {
      throw new RequiredFlagException("producer");
    }
    if (mNumThreadsPerMapper < 1) {
      throw new IllegalFlagValueException("num-threads", Integer.toString(mNumThreadsPerMapper));
    }
  }

  @Override
  protected KijiProduceJobBuilder createJobBuilder() {
    return new KijiProduceJobBuilder();
  }

  @Override
  protected void configure(KijiProduceJobBuilder jobBuilder) throws ClassNotFoundException,
      IOException, JobIOSpecParseException {
    super.configure(jobBuilder);
    jobBuilder.withProducer(Class.forName(mProducerName).asSubclass(KijiProducer.class));
    if (JobOutputSpec.Format.KIJI == mOutputSpec.getFormat()) {
      jobBuilder.withOutput(new KijiTableMapReduceJobOutput(getInputTable()));
    } else if (JobOutputSpec.Format.HFILE == mOutputSpec.getFormat()) {
      jobBuilder.withOutput(new HFileMapReduceJobOutput(
          getInputTable(), new Path(mOutputSpec.getLocation()), mOutputSpec.getSplits()));
    } else {
      MapReduceJobOutputFactory outputFactory = new MapReduceJobOutputFactory();
      jobBuilder.withOutput(outputFactory.createFromOutputSpec(mOutputSpec));
    }
    jobBuilder.withNumThreads(mNumThreadsPerMapper);
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    int jobStatus = super.run(nonFlagArgs);

    // If HFiles were produced, then a bulk-load command should be run afterwards.
    if (JobOutputSpec.Format.HFILE == mOutputSpec.getFormat()) {
      if (0 != jobStatus) {
        LOG.error("HFiles were not generated successfully.");
      } else {
        LOG.info("To complete loading of job results into table, run the kiji bulk-load command");
        LOG.info("    e.g. kiji bulk-load --table=" + getOutputTableName() + " --input="
            + mOutputSpec.getLocation()
            + (getURI().getInstance() == null ? "" : " --instance=" + getURI().getInstance()));
      }
    }
    return jobStatus;
  }

  /**
   * Returns the name of the output table name.
   *
   * In a produce job, the input and output tables are identical.
   * This method is here to make it explicit that we want the
   * output table name.
   *
   * @return The output table name.
   */
  private String getOutputTableName() {
    return mInputSpec.getLocation();
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiProduce(), args));
  }
}
