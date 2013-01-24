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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiProduceJobBuilder;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;

/**
 * A program that runs the {@link org.kiji.mapreduce.lib.examples.EmailDomainProducer}
 * over a Kiji table.
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$KIJI_HOME/bin/kiji classpath` \
 * &gt;   org.kiji.mapreduce.lib.examples.EmailDomainProduceJob \
 * &gt;   instance-name table-name
 * </pre>
 */
public class EmailDomainProduceJob extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(EmailDomainProduceJob.class);

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    if (2 != args.length) {
      throw new IllegalArgumentException(
          "Invalid number of arguments. Requires kiji-instance-name and table-name.");
    }

    // Read the arguments from the command-line.
    String instanceName = args[0];
    String kijiTableName = args[1];
    LOG.info("Configuring a produce job over table " + kijiTableName + ".");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a kiji connection...");
    KijiConfiguration kijiConf = new KijiConfiguration(getConf(), instanceName);
    Kiji kiji = Kiji.Factory.open(kijiConf);

    LOG.info("Opening kiji table " + kijiTableName + "...");
    KijiTable table = kiji.openTable(kijiTableName);

    LOG.info("Configuring a produce job...");
    KijiProduceJobBuilder jobBuilder = KijiProduceJobBuilder.create()
        .withInputTable(table)
        .withProducer(EmailDomainProducer.class)
        .withOutput(new KijiTableMapReduceJobOutput(table));

    LOG.info("Building the produce job...");
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
   *     EmailDomainProduceJob &lt;kiji-instance&gt; &lt;kiji-table&gt;
   *
   * ARGUMENTS:
   *
   *     kiji-instance: Name of the kiji instance the table is in.
   *
   *     kiji-table: Name of the kiji table to produce over.
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new EmailDomainProduceJob(), args));
  }
}
