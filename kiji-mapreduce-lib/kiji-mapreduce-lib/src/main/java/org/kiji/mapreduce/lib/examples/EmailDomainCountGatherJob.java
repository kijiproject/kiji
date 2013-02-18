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

package org.kiji.mapreduce.lib.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.lib.reduce.IntSumReducer;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * A program that runs the {@link org.kiji.mapreduce.lib.examples.EmailDomainCountGatherer}
 * over a Kiji table.
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$KIJI_HOME/bin/kiji classpath` \
 * &gt;   org.kiji.mapreduce.lib.examples.EmailDomainCountGatherJob \
 * &gt;   instance-name table-name output-path num-splits
 * </pre>
 */
public class EmailDomainCountGatherJob extends Configured implements Tool {
  /** A logger. */
  private static final Logger LOG = LoggerFactory.getLogger(EmailDomainCountGatherJob.class);

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    if (4 != args.length) {
      throw new IllegalArgumentException("Invalid number of arguments. "
          + "Requires instance-name, table-name, output-path, and num-splits.");
    }

    // Read the arguments from the commmand-line.
    String instanceName = args[0];
    String kijiTableName = args[1];
    Path outputPath = new Path(args[2]);
    int numSplits = Integer.parseInt(args[3]);
    LOG.info("Configuring a gather job over table " + kijiTableName + ".");
    LOG.info("Writing output to " + outputPath + ".");
    LOG.info("Using " + numSplits + " reducers.");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a kiji connection...");
    KijiURI kijiURI = KijiURI.newBuilder().withInstanceName(instanceName).build();
    Kiji kiji = Kiji.Factory.open(kijiURI, getConf());

    LOG.info("Opening kiji table " + kijiTableName + "...");
    KijiTable table = kiji.openTable(kijiTableName);

    LOG.info("Configuring a gather job...");
    KijiGatherJobBuilder jobBuilder = KijiGatherJobBuilder.create()
        .withInputTable(table.getURI())
        .withGatherer(EmailDomainCountGatherer.class)
        .withCombiner(IntSumReducer.class)
        .withReducer(IntSumReducer.class)
        .withOutput(new TextMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the gather job...");
    MapReduceJob job = jobBuilder.build();

    LOG.info("Running the job...");
    boolean isSuccessful = job.run();

    table.close();
    kiji.release();

    LOG.info(isSuccessful ? "Job succeeded." : "Job failed.");
    return isSuccessful ? 0 : 1;
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     EmailDomainCountGatherJob &lt;kiji-instance&gt; &lt;kiji-table&gt; &lt;output-path&gt;
   *       &lt;num-splits&gt;
   *
   * ARGUMENTS:
   *
   *     kiji-instance: Name of the kiji instance the table is in.
   *
   *     kiji-table: Name of the kiji table gather over.
   *
   *     output-path: The path to the output files to generate.
   *
   *     num-splits: The number of output file shards to generate (determines number of reducers).
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new EmailDomainCountGatherJob(), args));
  }
}
