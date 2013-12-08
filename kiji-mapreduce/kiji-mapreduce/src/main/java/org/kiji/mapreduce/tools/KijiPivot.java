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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.pivot.KijiPivotJobBuilder;
import org.kiji.mapreduce.pivot.impl.KijiPivoters;
import org.kiji.mapreduce.tools.framework.KijiJobTool;
import org.kiji.mapreduce.tools.framework.MapReduceJobInputFactory;
import org.kiji.mapreduce.tools.framework.MapReduceJobOutputFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;
import org.kiji.schema.util.ResourceUtils;

/**
 * Runs a KijiPivoter a Kiji table.
 *
 * <p>
 * Here is an example of command-line to launch a KijiPivoter named {@code package.SomePivoter}
 * on the rows from an input table {@code kiji://.env/input_instance/input_table}
 * while writing cells to another output table {@code kiji://.env/output_instance/output_table}:
 * <pre><blockquote>
 *   $ kiji pivot \
 *       --pivoter='package.SomePivoter' \
 *       --input="format=kiji table=kiji://.env/default/input_table" \
 *       --output="format=kiji table=kiji://.env/default/output_table nsplits=5" \
 *       --lib=/path/to/libdir/
 * </blockquote></pre>
 * </p>
 */
@ApiAudience.Private
public final class KijiPivot extends KijiJobTool<KijiPivotJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiPivot.class);

  @Flag(name="pivoter",
      usage="KijiPivoter class to run over the table.")
  private String mPivoter= "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "pivot";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Run a pivoter over a Kiji table.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "MapReduce";
  }

  /** Kiji instance where the output table lives. */
  private Kiji mKiji;

  /** KijiTable to import data into. */
  private KijiTable mTable;

  /** Job output. */
  private KijiTableMapReduceJobOutput mOutput;

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    // Do NOT call super.validateFlags()

    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mPivoter.isEmpty()) {
      throw new RequiredFlagException("pivoter");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    final MapReduceJobOutput mrJobOutput =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    Preconditions.checkArgument(mrJobOutput instanceof KijiTableMapReduceJobOutput,
        "Pivot jobs output format must be 'hfile' or 'kiji', but got output spec '%s'.",
        mOutputFlag);
    mOutput = (KijiTableMapReduceJobOutput) mrJobOutput;

    Preconditions.checkArgument(mOutput.getOutputTableURI().getTable() != null,
        "Specify the table to import data into with --output=...");
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mKiji = Kiji.Factory.open(mOutput.getOutputTableURI(), getConf());
    mTable = mKiji.openTable(mOutput.getOutputTableURI().getTable());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiPivotJobBuilder createJobBuilder() {
    return KijiPivotJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(KijiPivotJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException {

    // Resolve job input:
    final MapReduceJobInput input =
        MapReduceJobInputFactory.create().fromSpaceSeparatedMap(mInputFlag);
    if (!(input instanceof KijiTableMapReduceJobInput)) {
      throw new RuntimeException(String.format(
          "Invalid pivot job input: '%s'; must input from a Kiji table.", mInputFlag));
    }
    final KijiTableMapReduceJobInput tableInput = (KijiTableMapReduceJobInput) input;

    // Resolve job output:
    final MapReduceJobOutput output =
        MapReduceJobOutputFactory.create().fromSpaceSeparatedMap(mOutputFlag);
    if (!(output instanceof KijiTableMapReduceJobOutput)) {
      throw new RuntimeException(String.format(
          "Invalid pivot job output: '%s'; must output to a Kiji table.", mOutputFlag));
    }
    mOutput = (KijiTableMapReduceJobOutput) output;

    // Configure job:
    super.configure(jobBuilder);
    jobBuilder
        .withPivoter(KijiPivoters.forName(mPivoter))
        .withInputTable(tableInput.getInputTableURI())
        .withOutput(mOutput);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final int jobStatus = super.run(nonFlagArgs);

    // TODO: Make this a method of job outputs?
    if (mOutput instanceof DirectKijiTableMapReduceJobOutput) {
      if (jobStatus == 0) {
        LOG.info("Pivot job for table {} completed successfully.",
            mOutput.getOutputTableURI());
      } else {
        LOG.error("Pivot job failed, output table {} may have partial writes.",
            mOutput.getOutputTableURI());
      }
    } else if (mOutput instanceof HFileMapReduceJobOutput) {
      if (jobStatus == 0) {
        // Provide instructions for completing the bulk import.
        LOG.info("Pivot job completed successfully. "
            + "HFiles may now be bulk-loaded into table {} with: {}",
            mOutput.getOutputTableURI(),
            String.format("kiji bulk-load --table=%s", mOutput.getOutputTableURI()));
      } else {
        LOG.error(
            "Pivot job failed: HFiles for table {} were not generated successfully.",
            mOutput.getOutputTableURI());
      }
    } else {
      LOG.error("Unknown job output format: {}", mOutput.getClass().getName());
    }
    return jobStatus;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiPivot(), args));
  }
}
