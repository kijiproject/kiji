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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(KijiBulkLoad.class);

  @Flag(name="hfile", usage="Path of the directory containing HFile(s) to bulk-load. "
      + "Typically --hfile=hdfs://hdfs-cluster-address/path/to/hfile.dir/")
  private String mHFileFlag = null;

  @Flag(name="table", usage="URI of the Kiji table to bulk-load into.")
  private String mTableURIFlag = null;

  @Flag(name="timeout-seconds", usage="Timeout in seconds to wait for a bulk-load to "
      + "succeed. Default is 60 seconds")
  private final Long mTimeoutSeconds = 60L;

  @Flag(name="chmod-background", usage="When true, while bulk loading, periodically scan the "
      + "hfile directory recursively to add all read-write permissions to files created by "
      + "splits so that they will be usable by HBase. See "
      + "https://issues.apache.org/jira/browse/HBASE-6422.")
  private Boolean mChmodBackground = false;

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
   * Recursively grant additional read and write permissions to all. There is no
   * built-in way in the Hadoop Java API to recursively set permissions on a directory,
   * so we implement it here.
   *
   * @param path The Path to the directory to chmod.
   * @throws IOException on IOException.
   */
  private void recursiveGrantAllReadWritePermissions(Path path) throws IOException {
    FileSystem hdfs = path.getFileSystem(getConf());
    recursiveGrantAllReadWritePermissions(hdfs, hdfs.getFileStatus(path));
  }

  /**
   * Helper method used by recursiveGrantAllReadWritePermissions to actually grant the
   * additional read and write permissions to all.  It deals with FileStatus objects
   * since that is the object that supports listStatus.
   *
   * @param hdfs The FileSystem on which the file exists.
   * @param status The status of the file whose permissions are checked and on whose children
   *     this method is called recursively.
   * @throws IOException on IOException.
   */
  private void recursiveGrantAllReadWritePermissions(FileSystem hdfs, FileStatus status)
      throws IOException {
    final FsPermission currentPermissions = status.getPermission();
    if (!currentPermissions.getOtherAction().implies(FsAction.READ_WRITE)) {
      LOG.info("Adding a+rw to permissions for {}: {}", status.getPath(), currentPermissions);
      hdfs.setPermission(
          status.getPath(),
          new FsPermission(
              currentPermissions.getUserAction(),
              currentPermissions.getGroupAction().or(FsAction.READ_WRITE),
              currentPermissions.getOtherAction().or(FsAction.READ_WRITE)));
    }
    // Recurse into any files and directories in the path.
    // We must use listStatus because listFiles does not list subdirectories.
    FileStatus[] subStatuses = hdfs.listStatus(status.getPath());
    for (FileStatus subStatus : subStatuses) {
      if (!subStatus.equals(status)) {
        recursiveGrantAllReadWritePermissions(hdfs, subStatus);
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

  // For some reason checkstyle complains if we include an explicit @inheritDoc annotation here.
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

        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        Future<?> chmodTask = null;
        try {
          // We have to continually run chmod a+rw so that files newly created by splits will
          // be usable by hbase.  See https://issues.apache.org/jira/browse/HBASE-6422.
          // HBase proposes to solve this with the Secure Bulk Loader, but, for now, this
          // band-aid works.
          if (mChmodBackground) {
            final Runnable chmodRunnable = new Runnable() {
              static final int MAX_CONSECUTIVE_ERRORS=5;

              private int mNumConsecutiveErrors = 0;

              /** {@inheritDoc} */
              @Override
              public void run() {
                try {
                  recursiveGrantAllReadWritePermissions(mHFile);
                  mNumConsecutiveErrors = 0;
                } catch (IOException ex) {
                  LOG.warn("recursiveGrantAllReadWritePermissions raised exception: {}", ex);
                  mNumConsecutiveErrors += 1;
                  if (mNumConsecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                    throw new RuntimeException("too many IOExceptions", ex);
                  }
                }
              }
            };
            chmodTask = executorService.scheduleAtFixedRate(chmodRunnable, 0, 1, TimeUnit.SECONDS);
          }
          // NOTE: HFileLoader never uses conf.
          final HFileLoader hFileLoader = HFileLoader.create(conf);
          // launch the load on a separate thread and wait to cancel if timeout is exceeded
          final Callable<Void> hFileLoadCallable = new Callable<Void>() {
            public Void call() throws Exception {
              hFileLoader.load(mHFile, table);
              return null;
            }
          };
          final Future<Void> hFileLoadTask = executorService.submit(hFileLoadCallable);
          try {
            hFileLoadTask.get(mTimeoutSeconds, TimeUnit.SECONDS);
          } catch (TimeoutException ex) {
            getPrintStream().println(
                "Bulk-load failed due to a timeout after " + mTimeoutSeconds + "s.");
            hFileLoadTask.cancel(true);
            return FAILURE;
          } catch (ExecutionException executionException) {
            // try to unpack the exception that bulk load raised
            Exception cause = null;
            try {
              cause = (Exception) executionException.getCause();
              if (cause == null) {
                // There was no cause? Fall back to the original ExecutionException.
                cause = executionException;
              }
            } catch (ClassCastException castException) {
              // Cause wasn't an exception? Fall back to the original ExecutionException.
              cause = executionException;
            }
            throw cause;
          }
        } finally {
          if (chmodTask != null) {
            chmodTask.cancel(false);
          }
          executorService.shutdown();
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
