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

package org.kiji.mapreduce.tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.HFileLoader;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;
import org.kiji.schema.tools.VersionValidatedTool;

/** Bulk loads HFiles into a Kiji table. */
@ApiAudience.Private
public class KijiBulkLoad extends VersionValidatedTool {
  @Flag(name="input", usage="HFile location")
  protected String mInputFlag = "";

  @Flag(name="table", usage="Kiji table name")
  private String mTableName = "";

  /** The table to import data into. */
  private KijiTable mOutputTable;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "bulk-load";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Bulk load HFiles into a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Bulk";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }

    if (mTableName.isEmpty()) {
      throw new RequiredFlagException("table");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mOutputTable = getKiji().openTable(mTableName);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    if (null != mOutputTable) {
      mOutputTable.close();
    }
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    // Load the HFiles
    HFileLoader hFileLoader = new HFileLoader(getConf());
    hFileLoader.load(new Path(mInputFlag), mOutputTable);

    return 0;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiBulkLoad(), args));
  }
}
