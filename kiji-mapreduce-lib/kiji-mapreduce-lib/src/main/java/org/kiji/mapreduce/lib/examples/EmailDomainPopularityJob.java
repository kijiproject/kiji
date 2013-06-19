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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.lib.reduce.IntSumReducer;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * A program that generates an Avro file of email domains sorted by decreasing popularity.
 *
 * <p>This program runs two MapReduce jobs:</p>
 *
 * <ol>
 *   <li>The EmailDomainCountGatherer and IntSumReducer are used to generate a map from
 *       email domains to their popularity.</li>
 *   <li>The InvertCountMapper and TextListReducer are used to invert the output into a
 *       sorted map from popularity to list of email domains.</li>
 * </ol>
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$KIJI_HOME/bin/kiji classpath` \
 * &gt;   org.kiji.mapreduce.lib.examples.EmailDomainPopularityJob \
 * &gt;   instance-name table-name output-path num-splits
 * </pre>
 */
public class EmailDomainPopularityJob extends Configured implements Tool {
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
    final String instanceName = args[0];
    final String kijiTableName = args[1];
    final Path outputPath = new Path(args[2]);
    final int numSplits = Integer.parseInt(args[3]);

    final KijiURI tableURI = KijiURI
        .newBuilder(String.format("kiji://.env/%s/%s", instanceName, kijiTableName))
        .build();

    LOG.info("Configuring a gather job over table " + kijiTableName + ".");
    LOG.info("Writing output to " + outputPath + ".");
    LOG.info("Using " + numSplits + " reducers.");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a kiji connection...");
    final Kiji kiji = Kiji.Factory.open(tableURI, getConf());

    LOG.info("Opening kiji table " + kijiTableName + "...");
    final KijiTable table = kiji.openTable(kijiTableName);

    LOG.info("Running the first job: Count email domain popularity...");
    Path emailDomainCountPath = new Path(outputPath, "email-domain-count");
    boolean isFirstJobSuccessful = countEmailDomainPopularity(
        table, emailDomainCountPath, numSplits);
    if (!isFirstJobSuccessful) {
      LOG.error("First job failed.");
      return 1;
    }

    LOG.info("Running the second job: Invert and sort...");
    Path sortedPopularityPath = new Path(outputPath, "sorted-popularity");
    boolean isSecondJobSuccessful = invertAndSortByPopularity(
        emailDomainCountPath, sortedPopularityPath, numSplits, getConf());
    if (!isSecondJobSuccessful) {
      LOG.error("Second job failed.");
      return 2;
    }

    table.release();
    kiji.release();

    return 0;
  }

  /**
   * Runs the email domain count gather job to generate a map from email domain to popularity.
   *
   * @param table The input kiji table of users.
   * @param outputPath The output path for the map from email domains to their popularity.
   * @param numSplits The number of output file shards to write.
   * @return Whether the job was successful.
   * @throws Exception If there is an exception.
   */
  private boolean countEmailDomainPopularity(KijiTable table, Path outputPath, int numSplits)
      throws Exception {
    LOG.info("Configuring a gather job...");
    KijiGatherJobBuilder jobBuilder = KijiGatherJobBuilder.create()
        .withInputTable(table.getURI())
        .withGatherer(EmailDomainCountGatherer.class)
        .withCombiner(IntSumReducer.class)
        .withReducer(IntSumReducer.class)
        .withOutput(MapReduceJobOutputs.newSequenceFileMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the gather job...");
    KijiMapReduceJob job = jobBuilder.build();

    LOG.info("Running the gather job...");
    return job.run();
  }

  /**
   * Runs the job to invert the email domain popularity map and sort by popularity.
   *
   * @param inputPath The map from email domains to their popularity.
   * @param outputPath The output path for the sorted map of popularity to email domains.
   * @param numSplits The number of output file shards to write.
   * @param conf Base Hadoop configuration.
   * @return Whether the job was successful.
   * @throws Exception If there is an exception.
   */
  private boolean invertAndSortByPopularity(
      Path inputPath, Path outputPath, int numSplits, Configuration conf)
      throws Exception {
    LOG.info("Configuring a kiji mapreduce job...");
    KijiMapReduceJobBuilder jobBuilder = KijiMapReduceJobBuilder.create()
        .withConf(conf)
        .withInput(MapReduceJobInputs.newSequenceFileMapReduceJobInput(inputPath))
        .withMapper(InvertCountMapper.class)
        .withReducer(TextListReducer.class)
        .withOutput(MapReduceJobOutputs.newAvroKeyValueMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the transform job...");
    KijiMapReduceJob job = jobBuilder.build();

    // Configure the job to sort by decreasing key, so the most popular email domain is first.
    job.getHadoopJob().setSortComparatorClass(DescendingIntWritableComparator.class);

    LOG.info("Running the transform job...");
    return job.run();
  }

  /** A comparator that sorts IntWritables in descending order. */
  public static class DescendingIntWritableComparator extends IntWritable.Comparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // Invert the order.
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     EmailDomainPopularityJob &lt;kiji-instance&gt; &lt;kiji-table&gt; &lt;output-path&gt;
   *      &lt;num-splits&gt;
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
    System.exit(ToolRunner.run(new EmailDomainPopularityJob(), args));
  }
}
