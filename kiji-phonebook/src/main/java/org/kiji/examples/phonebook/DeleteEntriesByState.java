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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.mapreduce.DistributedCacheJars;
import org.kiji.schema.mapreduce.KijiConfKeys;
import org.kiji.schema.mapreduce.KijiTableInputFormat;
import org.kiji.schema.util.ResourceUtils;

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
      extends Mapper<EntityId, KijiRowData, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntriesByStateMapper.class);

    /** Configuration variable that will contain the 2-letter US state code. */
    public static final String CONF_STATE = "delete.state";

    /** Job counters. */
    public static enum Counter {
      /** Counts the number of rows with a missing info:address column. */
      MISSING_ADDRESS
    }

    /** Kiji instance to delete from. */
    private Kiji mKiji;

    /** Kiji table to delete from. */
    private KijiTable mTable;

    /** Writer to deletes. */
    private KijiTableWriter mWriter;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context hadoopContext) throws IOException, InterruptedException {
      super.setup(hadoopContext);
      final Configuration conf = hadoopContext.getConfiguration();
      KijiURI tableURI;
      try {
        tableURI = KijiURI.newBuilder(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI)).build();
      } catch (KijiURIException kue) {
        throw new IOException(kue);
      }
      mKiji = Kiji.Factory.open(tableURI, hadoopContext.getConfiguration());
      mTable = mKiji.openTable(tableURI.getTable());
      mWriter = mTable.openTableWriter();
    }

    /**
     * This method will be called once for each row of the phonebook table.
     *
     * @param entityId The entity id for the row.
     * @param row The data from the row (in this case, it would only
     *     include the address column because that is all we requested
     *     when configuring the input format).
     * @param hadoopContext The MapReduce job context used to emit output.
     * @throws IOException If there is an IO error.
     */
    @Override
    public void map(EntityId entityId, KijiRowData row, Context hadoopContext)
        throws IOException {
      // Check that the row has the info:address column.
      // The column names are specified as constants in the Fields.java class.
      if (!row.containsColumn(Fields.INFO_FAMILY, Fields.ADDRESS)) {
        LOG.info("Missing address field in row: " + entityId);
        hadoopContext.getCounter(Counter.MISSING_ADDRESS).increment(1L);
        return;
      }

      final String victimState = hadoopContext.getConfiguration().get(CONF_STATE, "");
      final Address address = row.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS);

      if (victimState.equals(address.getState().toString())) {
        // Delete the entry.
        mWriter.deleteRow(entityId);
      }
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
      ResourceUtils.closeOrLog(mWriter);
      ResourceUtils.closeOrLog(mTable);
      ResourceUtils.releaseOrLog(mKiji);
      super.cleanup(hadoopContext);
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
    final KijiURI tableURI =
        KijiURI.newBuilder(String.format("kiji://.env/default/%s", TABLE_NAME)).build();
    KijiTableInputFormat.configureJob(
        job,
        tableURI,
        dataRequest,
        /* start row */ null,
        /* end row */ null);

    // Run the mapper that will delete entries from the specified state.
    job.setMapperClass(DeleteEntriesByStateMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    // Since table writers do not emit any key-value pairs, we set the output format to Null.
    job.setOutputFormatClass(NullOutputFormat.class);

    job.getConfiguration().set(DeleteEntriesByStateMapper.CONF_STATE, mState);

    // Use no reducer (this is map-only job).
    job.setNumReduceTasks(0);

    // Delete data from the Kiji phonebook table.
    job.getConfiguration().set(KijiConfKeys.OUTPUT_KIJI_TABLE_URI, tableURI.toString());

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
