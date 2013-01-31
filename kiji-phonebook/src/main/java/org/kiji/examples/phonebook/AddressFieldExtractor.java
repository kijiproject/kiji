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
 * Extracts fields from the address column into individual columns in the derived column family.
 */
public class AddressFieldExtractor extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Map task that will take the Avro address field and place all the address
   * fields into individual fields in the derived column family.
   */
  public static class AddressMapper
      extends Mapper<EntityId, KijiRowData, NullWritable, NullWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(AddressMapper.class);

    /** Job counters. */
    public static enum Counter {
      /** Counts the number of rows with a missing info:address column. */
      MISSING_ADDRESS
    }

    private Kiji mKiji;
    private KijiTable mTable;
    private KijiTableWriter mTableWriter;

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

      mKiji = Kiji.Factory.open(tableURI, conf);
      mTable = mKiji.openTable(TABLE_NAME);
      mTableWriter = mTable.openTableWriter();
    }

    /**
     * Called once per row in the phonebook table. Extracts the Avro
     * address record into the individual fields in the derived column
     * family.
     *
     * @param entityId The id for the row.
     * @param row The row data requested (in this case, just the address column).
     * @param hadoopContext The MapReduce task context.
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
      final Address address = row.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS);

      // Write the data in the address record into individual columns.
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ADDR_LINE_1, address.getAddr1());

      // Optional.
      if (null != address.getApt()) {
        mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.APT_NUMBER, address.getApt());
      }

      // Optional.
      if (null != address.getAddr2()) {
        mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ADDR_LINE_2, address.getAddr2());
      }

      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.CITY, address.getCity());
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.STATE, address.getState());
      mTableWriter.put(entityId, Fields.DERIVED_FAMILY, Fields.ZIP, address.getZip());
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
      ResourceUtils.closeOrLog(mTableWriter);
      ResourceUtils.closeOrLog(mTable);
      ResourceUtils.releaseOrLog(mKiji);
      super.cleanup(hadoopContext);
    }
  }

  /**
   * Submits the AddressMapper job to Hadoop.
   *
   * @param args Command line arguments; none expected.
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Kiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    // Load HBase configuration before connecting to Kiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that extracts address records into the individual fields.
    final Job job = new Job(getConf(), "AddressFieldExtractor");

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

    // Run the mapper that will do the address extraction.
    job.setMapperClass(AddressMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    // Since table writers do not emit any key-value pairs, we set the output format to Null.
    job.setOutputFormatClass(NullOutputFormat.class);

    // Use no reducer (this is map-only job).
    job.setNumReduceTasks(0);

    // Write extracted data to the Kiji phonebook table.
    job.getConfiguration().set(KijiConfKeys.OUTPUT_KIJI_TABLE_URI, tableURI.toString());

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(AddressMapper.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(job,
        new File(System.getenv("KIJI_HOME"), "lib"));
    job.setUserClassesTakesPrecedence(true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point.
   *
   * @param args Arguments to AddressFieldExtractor job
   * @throws Exception General main exceptions
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AddressFieldExtractor(), args));
  }
}
