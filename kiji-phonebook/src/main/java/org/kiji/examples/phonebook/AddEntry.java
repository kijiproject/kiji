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

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.examples.phonebook.util.ConsolePrompt;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.util.ReferenceCountableUtils;

/**
 * Interactively create a phonebook entry and add it to the Kiji table.
 */
public class AddEntry extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /**
   * Run the entry addition system. Asks the user for values for all fields
   * and then fills them in.
   *
   * @param args Command line arguments; this is expected to be empty.
   * @return Exit status code for the application; 0 indicates success.
   * @throws IOException If an error contacting Kiji occurs.
   * @throws InterruptedException If the process is interrupted while performing I/O.
   */
  @Override
  public int run(String[] args) throws IOException, InterruptedException {
    final ConsolePrompt console = new ConsolePrompt();

    // Interactively prompt the user for the record fields from the console.
    final String first = console.readLine("First name: ");
    final String last = console.readLine("Last name: ");
    final String email = console.readLine("Email address: ");
    final String telephone = console.readLine("Telephone: ");

    final Address addr = new Address();
    addr.setAddr1(console.readLine("Address line 1: "));

    // Optional: apartment.
    final String aptNumStr = console.readLine("Apartment: ");
    if (!aptNumStr.isEmpty()) {
      addr.setApt(aptNumStr);
    }

    // Optional: address line 2.
    addr.setAddr2(console.readLine("Address line 2: "));
    if (addr.getAddr2().length() == 0) {
      addr.setAddr2(null);
    }

    addr.setCity(console.readLine("City: "));
    addr.setState(console.readLine("State: "));
    addr.setZip(Integer.valueOf(console.readLine("Zip: ")));

    Kiji kiji = null;
    KijiTable table = null;
    KijiTableWriter writer = null;
    try {
      // Load HBase configuration before connecting to Kiji.
      setConf(HBaseConfiguration.addHbaseResources(getConf()));

      // Connect to Kiji and open the table.
      kiji = Kiji.Factory.open(
          new KijiConfiguration(getConf(), KijiConfiguration.DEFAULT_INSTANCE_NAME));
      table = kiji.openTable(TABLE_NAME);
      writer = table.openTableWriter();

      // Create a row ID with the first and last name.
      final EntityId user = table.getEntityId(first + "," + last);

      // Write the record fields to appropriate table columns in the row.
      // The column names are specified as constants in the Fields.java class.
      final long timestamp = System.currentTimeMillis();
      writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, first);
      writer.put(user, Fields.INFO_FAMILY, Fields.LAST_NAME, timestamp, last);
      writer.put(user, Fields.INFO_FAMILY, Fields.EMAIL, timestamp, email);
      writer.put(user, Fields.INFO_FAMILY, Fields.TELEPHONE, timestamp, telephone);
      writer.put(user, Fields.INFO_FAMILY, Fields.ADDRESS, timestamp, addr);
    } catch (KijiTableNotFoundException e) {
      System.out.println("Could not find Kiji table: " + TABLE_NAME);
      return 1;
    } finally {
      // Safely free up resources by closing in reverse order.
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(table);
      ReferenceCountableUtils.releaseQuietly(kiji);
      IOUtils.closeQuietly(console);
    }

    return 0;
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args The arguments from the command line. May start with Hadoop "-D" options.
   * @throws Exception If the application encounters an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AddEntry(), args));
  }
}
