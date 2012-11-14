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

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.mapreduce.ContextKijiTableWriter;
import org.kiji.schema.mapreduce.DistributedCacheJars;
import org.kiji.schema.mapreduce.KijiOutput;
import org.kiji.schema.mapreduce.KijiTableOutputFormat;

/**
 * Shell for the PhonebookImportMapper class.  This class manages
 * the command line arguments and job setup for the mapper class.
 */
public class PhonebookImporter extends Configured implements Tool {
  /** Name of the table to insert phonebook entries into. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Map task that will parse user records from a text file and insert the records
   * into the phonebook table.
   */
  public static class PhonebookImportMapper
      extends Mapper<LongWritable, Text, NullWritable, KijiOutput> {
    private static final Logger LOG = LoggerFactory.getLogger(PhonebookImportMapper.class);

    private Kiji mKiji;
    private KijiTable mTable;
    private KijiTableWriter mWriter;

    /**
     * Called once per MapReduce task to initialize.
     *
     * @param context The MapReduce task context.
     * @throws IOException If there is an IO error.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      final KijiConfiguration conf = new KijiConfiguration(
          context.getConfiguration(), KijiConfiguration.DEFAULT_INSTANCE_NAME);
      mKiji = Kiji.open(conf);
      mTable = mKiji.openTable(TABLE_NAME);
      mWriter = new ContextKijiTableWriter(context);
    }

    /**
     * Called once per line from the input file. Inserts a row into the phonebook table.
     *
     * @param byteOffset The byte offset into the file for this line.
     * @param line The line of the file to process.
     * @param context The MapReduce task context.
     * @throws IOException If there is an IO error.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    public void map(LongWritable byteOffset, Text line, Context context)
        throws IOException, InterruptedException {
      // Each line of the text file has the form:
      //
      //     firstname|lastname|email|telephone|addressJson
      //
      // Split the input line by the pipe '|' character.
      final String[] fields = line.toString().split("\\|");

      if (5 != fields.length) {
        LOG.error("Invalid number of fields (" + fields.length + ") in line: " + line);
        return; // No inserts for this malformed line.
      }

      // Extract the relevant fields.
      final String firstName = fields[0];
      final String lastName = fields[1];
      final String email = fields[2];
      final String telephone = fields[3];
      final String addressJson = fields[4];

      // addressJson contains a JSON-encoded Avro "Address" record. Parse this into
      // an object and write it to the person's record.
      // The Address record type is generated from src/main/avro/Address.avsc as part
      // of the build process (see avro-maven-plugin in pom.xml).
      final SpecificDatumReader<Address> datumReader =
          new SpecificDatumReader<Address>(Address.SCHEMA$);
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Address.SCHEMA$, addressJson);
      final Address streetAddr = datumReader.read(null, decoder);

      // Create a row ID with the first and last name.
      final EntityId user = mTable.getEntityId(firstName + "," + lastName);

      // Write the fields to appropriate table columns in the row.
      // The column names are specified as constants in the Fields.java class.
      mWriter.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, firstName);
      mWriter.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, lastName);
      mWriter.put(user, Fields.INFO_FAMILY, Fields.EMAIL, email);
      mWriter.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, telephone);
      mWriter.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, streetAddr);
    }

    /**
     * Called once at the end of the MapReduce task.
     *
     * @param context The MapReduce task context.
     * @throws IOException If there is an IO error.
     * @throws InterruptedException If the thread is interrupted.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      IOUtils.closeQuietly(mWriter);
      IOUtils.closeQuietly(mTable);
      IOUtils.closeQuietly(mKiji);
    }
  }

  /**
   * Submits the PhonebookImportMapper job to Hadoop.
   *
   * @param args Command line arguments; contains the path to the input text file to import.
   * @return The status code for the application; 0 indicates success.
   * @throws Exception If there is an error running the Kiji program.
   */
  @Override
  public int run(String[] args) throws Exception {
    // Load HBase configuration before connecting to Kiji.
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    // Configure a map-only job that imports phonebook entries from a file into the table.
    final Job job = new Job(getConf(), "PhonebookImporter");

    // Read from a text file.
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    // Run the mapper that will import entries from the input file.
    job.setMapperClass(PhonebookImportMapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(KijiOutput.class);

    // Use no reducer (this is a map-only job).
    job.setNumReduceTasks(0);

    // Direct the job output to the phonebook table.
    job.setOutputFormatClass(KijiTableOutputFormat.class);
    KijiTableOutputFormat.setOptions(job, TABLE_NAME);

    // Tell Hadoop where the java dependencies are located, so they
    // can be shipped to the cluster during execution.
    job.setJarByClass(PhonebookImporter.class);
    GenericTableMapReduceUtil.addAllDependencyJars(job);
    DistributedCacheJars.addJarsToDistributedCache(
        job, new File(System.getenv("KIJI_HOME"), "lib"));
    job.setUserClassesTakesPrecedence(true);

    // Run the job.
    final boolean isSuccessful = job.waitForCompletion(true);

    return isSuccessful ? 0 : 1;
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args The arguments from the command line. May start with Hadoop "-D" options.
   * @throws Exception If the application encounters an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PhonebookImporter(), args));
  }
}
