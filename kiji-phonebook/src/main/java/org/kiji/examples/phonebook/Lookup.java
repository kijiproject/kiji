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
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * Lookup a phonebook entry by user id and return the information.
 */
public class Lookup extends Configured implements Tool {
  /** Name of the table to read for phonebook entries. */
  public static final String TABLE_NAME = "phonebook";

  /** Populated in the run() method with the contents of the --first command line argument. */
  @Flag(name="first", usage="First name")
  private String mFirst = "";

  /** Populated in the run() method with the contents of the --last command line argument. */
  @Flag(name="last", usage="Last name")
  private String mLast = "";

  @Flag(
      name="kiji",
      usage="Specify the Kiji instance containing the 'phonebook' table."
  )
  private String mKijiUri = "kiji://.env/default";

  /**
   * Run a lookup from the command line.
   *
   * @param args Command line arguments; expected to contain --first and --last.
   * @return Exit status code for the application; 0 indicates success.
   * @throws IOException If an error contacting Kiji occurs.
   */
  @Override
  public int run(String[] args) throws IOException {
    // Use kiji-common-flags to parse arguments, populating mFirst, mLast, mKijiUri.
    List<String> nonFlagArgs = FlagParser.init(this, args);
    if (null == nonFlagArgs) {
      // There was a problem parsing the flags.
      return 1;
    }

    Kiji kiji = null;
    KijiTable table = null;
    KijiTableReader reader = null;
    try {
      // Load HBase configuration before connecting to Kiji.
      setConf(HBaseConfiguration.create(getConf()));

      // Connect to Kiji, open the table and reader.
      kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
      table = kiji.openTable(TABLE_NAME);
      reader = table.openTableReader();

      // Specify the row and column data to read.
      // The column names are specified as constants in the Fields.java class.
      final EntityId entityId = table.getEntityId(mFirst + "," + mLast);
      final KijiDataRequestBuilder reqBuilder = KijiDataRequest.builder();
      reqBuilder.newColumnsDef()
          .add(Fields.INFO_FAMILY, Fields.FIRST_NAME)
          .add(Fields.INFO_FAMILY, Fields.LAST_NAME)
          .add(Fields.INFO_FAMILY, Fields.EMAIL)
          .add(Fields.INFO_FAMILY, Fields.TELEPHONE)
          .add(Fields.INFO_FAMILY, Fields.ADDRESS);
      final KijiDataRequest dataRequest = reqBuilder.build();
      final KijiRowData rowData = reader.get(entityId, dataRequest);

      if (!rowData.containsColumn(Fields.INFO_FAMILY, Fields.FIRST_NAME)) {
        // We got a row back without a firstname. Indicates that the
        // user (row) does not exist.
        System.out.println("No such user: " + mFirst + " " + mLast);
        return 1;
      }

      final String formattedOutput = String.format("%s %s: email=%s, tel=%s, addr=%s",
          rowData.getMostRecentValue(Fields.INFO_FAMILY, Fields.FIRST_NAME),
          rowData.getMostRecentValue(Fields.INFO_FAMILY, Fields.LAST_NAME),
          rowData.getMostRecentValue(Fields.INFO_FAMILY, Fields.EMAIL),
          rowData.getMostRecentValue(Fields.INFO_FAMILY, Fields.TELEPHONE),
          rowData.getMostRecentValue(Fields.INFO_FAMILY, Fields.ADDRESS));
      System.out.println(formattedOutput);

    } catch (KijiTableNotFoundException e) {
      System.out.println("Could not find Kiji table: " + TABLE_NAME);
      return 1;
    } finally {
      // Safely free up resources by closing in reverse order.
      ResourceUtils.closeOrLog(reader);
      ResourceUtils.releaseOrLog(table);
      ResourceUtils.releaseOrLog(kiji);
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
    System.exit(ToolRunner.run(new Lookup(), args));
  }
}
