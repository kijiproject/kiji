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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;

/**
 * A local program that will parse user records from a text file and insert the records
 * into the phonebook table.
 */
public class StandalonePhonebookImporter extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Reads a file one line at a time, inserting rows into the phonebook table.
   *
   * @param args Command line arguments; should include the path to the input file.
   * @return The status code for the application; 0 indicates success.
   * @throws IOException If there is an error contacting Kiji or reading the input file.
   */
  @Override
  public int run(String[] args) throws IOException {
    // Load HBase configuration before connecting to Kiji.
    setConf(HBaseConfiguration.create(getConf()));

    Kiji kiji = null;
    KijiTable table = null;
    KijiTableWriter writer = null;
    BufferedReader reader = null;
    try {
      // Connect to Kiji and open the table.
      kiji = Kiji.open(new KijiConfiguration(getConf(), KijiConfiguration.DEFAULT_INSTANCE_NAME));
      table = kiji.openTable(TABLE_NAME);
      writer = table.openTableWriter();

      // Read in the input file
      reader = new BufferedReader(new FileReader(args[0]));

      String line;
      while ((line = reader.readLine()) != null) {
        importLine(line, table, writer);
      }
    } catch (Exception e) {
      System.out.println("Error importing records: " + e);
      return 1;
    } finally {
      IOUtils.closeQuietly(reader);
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(table);
      IOUtils.closeQuietly(kiji);
    }
    return 0;
  }

  /**
   * Parses a single line from the input file and inserts it into a
   * row in the table.
   *
   * @param phonebookLine Line of text representing a user record.
   * @param table The table.
   * @param writer The table writer.
   * @throws IOException If there is an issue parsing the line or writing to the table.
   * @throws InterruptedException If this thread is interrupted.
   */
  private void importLine(String phonebookLine, KijiTable table, KijiTableWriter writer)
      throws IOException, InterruptedException {
    final String[] fields = phonebookLine.split("\\|");

    if (fields.length != 5) {
      System.out.println(
          "Invalid number of fields (" + fields.length + ") in line: " + phonebookLine);
      return; // No inserts for this malformed line.
    }

    final String firstName = fields[0];
    final String lastName = fields[1];
    final String email = fields[2];
    final String telephone = fields[3];
    final String addressJson = fields[4];

    // addressJson contains a JSON-encoded Avro "Address" record. Parse this
    // into an object and write it to the person's record.
    // The Address record type is generated from src/main/avro/Address.avsc as
    // part of the build process (see avro-maven-plugin in pom.xml).
    final SpecificDatumReader<Address> datumReader =
        new SpecificDatumReader<Address>(Address.SCHEMA$);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Address.SCHEMA$, addressJson);
    final Address streetAddr = datumReader.read(null, decoder);

    // Create a row ID with the first and last name.
    final EntityId user = table.getEntityId(firstName + "," + lastName);

    // Write the record fields to appropriate table columns in the row.
    // The column names are specified as constants in the Fields.java class.
    final long timestamp = System.currentTimeMillis();
    writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, firstName);
    writer.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, timestamp, lastName);
    writer.put(user, Fields.INFO_FAMILY, Fields.EMAIL, timestamp, email);
    writer.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, timestamp, telephone);
    writer.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, timestamp, streetAddr);
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args The arguments from the command line. May start with Hadoop "-D" options.
   * @throws Exception If the application encounters an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new StandalonePhonebookImporter(), args));
  }
}
