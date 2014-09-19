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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
  /** ExecutorService to execute the callables when bulk-loading. */
  private ExecutorService mExecutorService = Executors.newCachedThreadPool();

  @Flag(name="hfile", usage="Path of the directory containing HFile(s) to bulk-load. "
      + "Typically --hfile=hdfs://hdfs-cluster-address/path/to/hfile.dir/")
  private String mHFileFlag = null;

  @Flag(name="table", usage="URI of the Kiji table to bulk-load into.")
  private String mTableURIFlag = null;

  @Flag(name="timeout-milliseconds", usage="Timeout in milliseconds to wait for a bulk-load to "
      + "succeed. Default 10 seconds (10000 milliseconds).")
  private final Long mLoadTimeoutMilliseconds = 10000L; // 10 seconds

  @Flag(name="chmod-interactive", usage="When false, does not prompt for confirmation before "
      + "chmod'ing the hfile directory.")
  private Boolean mChmodInteractive = true;

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

  /**
   * Recursively sets the permissions to 777 on the HFiles.  There is no built-in way in the
   * Hadoop Java API to recursively set permissions on a directory, so we implement it here.
   *
   * @param path The Path to the directory to chmod.
   * @throws IOException on IOException.
   */
  private void recursiveGrantAllHFilePermissions(Path path) throws IOException {
    FileSystem hdfs = path.getFileSystem(getConf());
    // Set the permissions on the path itself.
    hdfs.setPermission(path, FsPermission.createImmutable((short) 0777));
    // Recurse into any files and directories in the path.
    // We must use listStatus because listFiles does not list subdirectories.
    FileStatus[] fileStatuses = hdfs.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.getPath().equals(path)) {
        recursiveGrantAllHFilePermissions(fileStatus.getPath());
      }
    }
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

        // Create a new Callable for loading HFiles.  This is used later to execute the HFile
        // loading in a separate thread, so we can check if it's done and, if necessary, chmod 777
        // the HFile directory concurrently while it runs.
        Callable<Void> hFileLoadCallable = new Callable<Void>() {
          public Void call() throws Exception {
            hFileLoader.load(mHFile, table);
            return null;
          }
        };

        Future<Void> hFileLoadTask = mExecutorService.submit(hFileLoadCallable);
        Long startTime = System.currentTimeMillis();
        // Wait until mLoadTimeoutMilliseconds has passed.  We do not use Future#get(long timeout)
        // because that will cancel the future if it isn't done.
        while (
            System.currentTimeMillis() < startTime + mLoadTimeoutMilliseconds
            && !hFileLoadTask.isDone()) {
          Thread.sleep(100);
        }
        if (hFileLoadTask.isDone()) {
          return SUCCESS;
        }
        // If it did not complete in mLoadTimeoutMilliseconds, try to chmod the directory.
        if (mChmodInteractive) {
          if (!inputConfirmation(
              "First attempt at bulk-load timed out after " + loadTimeoutMilliseconds56
                  + " milliseconds.  Do you want to chmod -R 777 the HFile directory?",
              mHFile.getName()
          )) {
            getPrintStream().println("Bulk-load timed out, not retrying.");
            return FAILURE;
          } else {
            recursiveGrantAllHFilePermissions(mHFile);
            try {
              hFileLoadTask.get(mLoadTimeoutMilliseconds, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
              getPrintStream().println("Bulk-load failed due to a second timeout.");
              return FAILURE;
            }
          }
        } else {
          recursiveGrantAllHFilePermissions(mHFile);
          try {
            hFileLoadTask.get(mLoadTimeoutMilliseconds, TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            getPrintStream().println("Bulk-load failed due to a second timeout.");
            return FAILURE;
          }
        }
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
