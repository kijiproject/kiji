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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.HFileLoader;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.util.ResourceUtils;

/** Bulk loads HFiles into a Kiji table. */
@ApiAudience.Private
public final class KijiBulkLoad extends BaseTool {
  @Flag(name="hfile", usage="Path of the directory containing HFile(s) to bulk-load. "
      + "Typically --hfile=hdfs://hdfs-cluster-address/path/to/hfile.dir/")
  private String mHFileFlag = null;

  @Flag(name="table", usage="URI of the Kiji table to bulk-load into.")
  private String mTableURIFlag = null;

  /** URI of the Kiji table to bulk-load into. */
  private KijiURI mTableURI = null;

  /** Path of the HFile(s) to bulk-load. */
  private Path mHFile = null;

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
    super.validateFlags();
    Preconditions.checkArgument((mTableURIFlag != null) && !mTableURIFlag.isEmpty(),
        "Specify the table to bulk-load into with "
        + "--table=kiji://hbase-address/kiji-instance/table");
    mTableURI = KijiURI.newBuilder(mTableURIFlag).build();
    Preconditions.checkArgument(mTableURI.getTable() != null,
        "Specify the table to bulk-load into with "
        + "--table=kiji://hbase-address/kiji-instance/table");

    Preconditions.checkArgument((mHFileFlag != null) && !mHFileFlag.isEmpty(),
        "Specify the HFiles to bulk-load. "
        + "E.g. --hfile=hdfs://hdfs-cluster-address/path/to/hfile.dir/");
    mHFile = new Path(mHFileFlag);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    final Kiji kiji = Kiji.Factory.open(mTableURI, getConf());
    try {
      final KijiTable table = kiji.openTable(mTableURI.getTable());
      try {
        // Load the HFiles.
        //
        // TODO: Consolidate this logic in a single central place: We must consolidate the
        // logic to properly initialize a Configuration object to target a specific HBase
        // cluster (hence the manual override of the ZooKeeper quorum/client-port).
        //
        // The reason for this manual override here is : KijiBulkLoad needs a
        // Configuration to create an HBaseLoader for the HBase instance targeted at from
        // the table URI.  KijiTable does not expose its internal Configuration and
        // Kiji.getConf() is deprecated, so we have to construct one externally.
        final Configuration conf = getConf();
        conf.set(HConstants.ZOOKEEPER_QUORUM,
            Joiner.on(",").join(mTableURI.getZookeeperQuorumOrdered()));
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, mTableURI.getZookeeperClientPort());

        final HFileLoader hFileLoader = HFileLoader.create(conf);
        hFileLoader.load(mHFile, table);
        return SUCCESS;
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
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
