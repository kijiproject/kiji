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

package org.kiji.examples.phonebook;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.odiago.common.flags.Flag;
import com.odiago.common.flags.FlagParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.mapreduce.ContextKijiTableWriter;
import org.kiji.schema.mapreduce.DistributedCacheJars;
import org.kiji.schema.mapreduce.KijiOutput;
import org.kiji.schema.mapreduce.KijiTableInputFormat;
import org.kiji.schema.mapreduce.KijiTableOutputFormat;

/**
 * Deletes all entries from the phonebook table that have an address from a particular US state.
 */
public class DeleteEntriesByState extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /** Populated in the run() method with the contents of the --state command line argument. */
  @Flag(name="state", usage="This program will delete entries with this 2-letter US state code.")
  private String mState;

  /**
   * A Mapper that will be run over the rows of the phonebook table,
   * deleting the entries with an address from the US state specified
   * in the "delete.state" job configuration variable.
   */
  public static class DeleteEntriesByStateMapper
      extends Mapper<EntityId, KijiRowData, NullWritable, KijiOutput> {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntriesByStateMapper.class);

    /** Configuration variable that will contain the 2-letter US state code. */
    public static final String CONF_STATE = "delete.state";

    /** Job counters. */
    public static enum Counter {
      /** Counts the number of rows with a missing info:address column. */
      MISSING_ADDRESS
    }

    /**
     * This method will be called once for each row of the phonebook table.
     *
     * @param entityId The entity id for the row.
     * @param row The data from the row (in this case, it would only
     *     include the address column because that is all we requested
     *     when configuring the input format).
     * @param context The MapReduce job context used to emit output.
     * @throws IOException If there is an IO error.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    public void map(EntityId entityId, KijiRowData row, Context context)
        throws IOException, InterruptedException {
      // Check that the row has the info:address column.
      // The column names are specified as constants in the Fields.java class.
      if (!row.containsColumn(Fields.INFO_FAMILY, Fields.ADDRESS)) {
        LOG.info("Missing address field in row: " + entityId);
        context.getCounter(Counter.MISSING_ADDRESS).increment(1L);
        return;
      }

      final String victimState = context.getConfiguration().get(CONF_STATE, "");
      final Address address = row.getValue(Fields.INFO_FAMILY, Fields.ADDRESS, Address.class);

      if (victimState.equals(address.getState().toString())) {
        // Delete the entry.
        final ContextKijiTableWriter writer = new ContextKijiTableWriter(context);
        try {
          writer.deleteRow(entityId);
        } finally {
          writer.close();
        }
      }
    }
  }

  /**
   * Deletes all entries from the phonebook table from a specified US state.
   *
   * @param args The command line arguments (we expect a --state=XX arg here).
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Kiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    final List<String> nonFlagArgs = FlagParser.init(this, args);
    if (null == nonFlagArgs) {
      // The flags were not parsed.
      return 1;
    }

    // Load HBase configuration before connecting to Kiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that deletes entries from the specified state (mState);
    final Job job = new Job(getConf(), "DeleteEntriesByState");

    // Read from the Kiji phonebook table.
    job.setInputFormatClass(KijiTableInputFormat.class);
    final KijiDataRequest dataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column(Fields.INFO_FAMILY, Fields.ADDRESS));
    KijiTableInputFormat.setOptions(TABLE_NAME, dataRequest, job);

    // Run the mapper that will delete entries from the specified state.
    job.setMapperClass(DeleteEntriesByStateMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(KijiOutput.class);
    job.getConfiguration().set(DeleteEntriesByStateMapper.CONF_STATE, mState);

    // Use no reducer (this is map-only job).
    job.setNumReduceTasks(0);

    // Delete data from the Kiji phonebook table.
    job.setOutputFormatClass(KijiTableOutputFormat.class);
    KijiTableOutputFormat.setOptions(job, TABLE_NAME);

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(DeleteEntriesByState.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(
        job, new File(System.getenv("KIJI_HOME"), "lib"));
    job.setUserClassesTakesPrecedence(true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point.
   *
   * @param args The command line arguments (we expect  a --state=XX here).
   * @throws Exception If there is an exception thrown from the application.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DeleteEntriesByState(), args));
  }
}
