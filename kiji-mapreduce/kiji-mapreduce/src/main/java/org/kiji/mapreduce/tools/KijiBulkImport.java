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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.KijiBulkImporter;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;

/** Bulk imports a file into a Kiji table. */
@ApiAudience.Private
public final class KijiBulkImport extends JobTool<KijiBulkImportJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiBulkImport.class);

  @Flag(name="table", usage="Kiji table name")
  private String mTableName = "";

  @Flag(name="importer", usage="KijiBulkImporter class to use")
  //private String mImporter = DelimitedJsonBulkImporter.class.getName();
  private String mImporter = "";

  /** The table to import data into. */
  private KijiTable mOutputTable;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "bulk-import";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Bulk import data into a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Bulk";
  }

  @Override
  protected void validateFlags() throws Exception {
    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mTableName.isEmpty()) {
      throw new RequiredFlagException("table");
    }
    if (mImporter.isEmpty()) {
      throw new RequiredFlagException("importer");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    mInputSpec = JobInputSpec.parse(mInputFlag);
    mOutputSpec = JobOutputSpec.parse(mOutputFlag);
    if (JobOutputSpec.Format.HFILE != mOutputSpec.getFormat()
        && JobOutputSpec.Format.KIJI != mOutputSpec.getFormat()) {
      throw new RuntimeException("Invalid format specified '"
          + mOutputSpec.getFormat().getName() + "'. Bulk imports require the "
          + JobOutputSpec.Format.HFILE.getName() + " or "
          + JobOutputSpec.Format.KIJI.getName() + " output format.");
    }
  }

  @Override
  protected void setup() throws Exception {
    super.setup();
    mOutputTable = getKiji().openTable(mTableName);
  }

  @Override
  protected void cleanup() throws IOException {
    IOUtils.closeQuietly(mOutputTable);
    super.cleanup();
  }

  @Override
  protected KijiBulkImportJobBuilder createJobBuilder() {
    return KijiBulkImportJobBuilder.create();
  }

  @Override
  protected void configure(KijiBulkImportJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    super.configure(jobBuilder);

    MapReduceJobInputFactory inputFactory = MapReduceJobInputFactory.create();
    jobBuilder.withInput(inputFactory.createFromInputSpec(mInputSpec));
    jobBuilder.withBulkImporter(KijiBulkImporter.forName(mImporter));
    if (JobOutputSpec.Format.KIJI == mOutputSpec.getFormat()) {
      jobBuilder.withOutput(new DirectKijiTableMapReduceJobOutput(mOutputTable));
    } else {
      jobBuilder.withOutput(new HFileMapReduceJobOutput(
              mOutputTable, new Path(mOutputSpec.getLocation()), mOutputSpec.getSplits()));
    }
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    int jobStatus = super.run(nonFlagArgs);

    if (JobOutputSpec.Format.KIJI == mOutputSpec.getFormat()) {
      // Imported directly to the table, no need to do anything else.
      LOG.info("Bulk import complete.");
      return jobStatus;
    }

    if (0 != jobStatus) {
      LOG.error("HFiles were not generated successfully.");
    } else {
      // Provide instructions for completing the bulk import.
      if (JobOutputSpec.Format.HFILE == mOutputSpec.getFormat()) {
        LOG.info("To complete loading of job results into table, run the kiji bulk-load command");
        LOG.info("    e.g. kiji bulk-load --table=" + mTableName + " --input="
            + mOutputSpec.getLocation() + (getURI().getInstance() == null ? "" : " --instance="
                + getURI().getInstance()));
      }
    }

    return jobStatus;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiBulkImport(), args));
  }
}
