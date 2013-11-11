/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.bento.box.tools;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.box.CheckinThread;
import org.kiji.checkin.CheckinClient;
import org.kiji.checkin.CheckinUtils;
import org.kiji.checkin.UUIDTools;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * <p>A tool that launches a thread that periodically checks-in with the upgrade server, then
 * waits for that thread to complete before terminating.</p>
 *
 * <p>This tool expects file recording the usage of the kiji script and a UUID for the user to be
 * located in the user's home directory. It also expects to write upgrade information to a file
 * in bento cluster's state directory.</p>
 *
 * <p>This tool will write a PID file to bento cluster's state directory. A shutdown hook is also
 * installed so that the thread performing check-ins is gracefully shutdown when the SIGTERM
 * signal is received. The method used to obtain PIDs is not portable to non-Unix systems.</p>
 */
public final class UpgradeDaemonTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDaemonTool.class);

  /** The name of the file where the kiji usage timestamp is stored. */
  private static final String TIMESTAMP_FILE_NAME = ".kiji-last-used";

  /** The name of the file where upgrade information should be written. */
  private static final String UPGRADE_FILE_NAME = ".kiji-bento-upgrade";

  /** The name of the file that stores a PID for this process. */
  private static final String PID_FILE_NAME = "checkin-daemon.pid";

  /**
   * The path to the bento-cluster state dir, where upgrade information files should be
   * written.
   */
  @Flag(name = "state-dir",
      usage = "The path used by bento-cluster for its state and configuration.")
  private String mStateDirPath = "";

  @Flag(name = "checkin-period-millis",
      usage = "The number of milliseconds between check-ins with the upgrade server.")
  // By default, check-in daily.
  private long mCheckinPeriodMillis = 24 * 60 * 60 * 1000;

  @Flag(name = "upgrade-server-url",
      usage = "The URL of an upgrade server to send check-in messages to.")
  private String mUpgradeServerURL = "";

  /** The thread that periodically performs check-ins. */
  private CheckinThread mCheckinThread;

  /**
   * Uses the command-line argument <code>--state-dir</code> to obtain bento cluster's state
   * directory.
   *
   * @return the directory, or <code>null</code> if the directory was not specified or if
   *     <code>null</code> it does not exist.
   */
  private File getBentoClusterStateDir() {
    if (null == mStateDirPath || mStateDirPath.isEmpty()) {
      LOG.error("The argument --state-dir was not specified. Cannot continue.");
      return null;
    }
    File stateDir = new File(mStateDirPath);
    if (!stateDir.exists() || !stateDir.isDirectory()) {
      LOG.error("The path provided for the bento-cluster state dir either does not exist or "
          + "is not a directory: " + stateDir.getAbsolutePath());
      return null;
    }
    return stateDir;
  }

  /**
   * Gets the file containing the usage timestamp for the Kiji script.
   *
   * @param directory that should contain the file.
   * @return the file containing the usage timestamp for the Kiji script.
   */
  private File getUsageTimestampFile(File directory) {
    return new File(directory, TIMESTAMP_FILE_NAME);
  }

  /**
   * Gets the file where upgrade information obtained from the check-in server will be written.
   *
   * @param directory that should contain the file.
   * @return the file where upgrade information obtained from the check-in server will be written.
   */
  private File getUpgradeInfoFile(File directory) {
    return new File(directory, UPGRADE_FILE_NAME);
  }

  /**
   * Gets the file where the process PID will be written.
   *
   * @param directory that should contain the file.
   * @return the file where the process PID should be written.
   */
  private File getPidFile(File directory) {
    return new File(directory, PID_FILE_NAME);
  }

  /**
   * Uses the command-line argument <code>--upgrade-server-url</code> to obtain an {@link URI}
   * for the upgrade server to check-in with.
   *
   * @return a URI for the upgrade check-in server, or <code>null</code> if there was an error
   *     parsing the specified address.
   */
  private URI getUpgradeServerURI() {
    return getUpgradeServerURI(mUpgradeServerURL);
  }


  /**
   * Parses the supplied raw URI as a URI to connect to.
   *
   * @param rawUri the string representing the URI to parse.
   * @return a URI for the upgrade check-in server, or <code>null</code> if there was an error
   *     parsing the specified address.
   */
  static URI getUpgradeServerURI(String rawUri) {
    if (null == rawUri || rawUri.isEmpty()) {
      LOG.error("The argument --checkin-server-url was not specified. Cannot continue.");
      return null;
    }
    try {
      return new URI(rawUri);
    } catch (Exception e) {
      LOG.error("Could not parse the provided URL for the check-in server: " + rawUri,
          e);
      return null;
    }
  }

  /**
   * Gets the pid for this process. This won't be portable on non-UNIX systems.
   *
   * @return the pid of this JVM.
   */
  private Integer getPid() {
    String processString = ManagementFactory.getRuntimeMXBean().getName();
    return Integer.valueOf(processString.split("@")[0]);
  }

  /**
   * Reads a pid from a file.
   *
   * @param pidFile to read a PID from.
   * @return the pid extracted from the file, or <code>null</code> if the file could not be read.
   */
  private Integer readPidFile(File pidFile) {
    try {
      return Integer.parseInt(CheckinUtils.readFileAsString(pidFile).trim());
    } catch (Exception e) {
      LOG.error("Could not read contents of existing PID file: " + pidFile.getAbsolutePath(), e);
      return null;
    }
  }

  /**
   * Writes the PID of this process to a file. If the PID file already exists,
   * this method will fail.
   *
   * @param pidFile to write the pid to.
   * @return <code>true</code> if a new PID file was successfully written, <code>false</code>
   *     otherwise.
   */
  private boolean createPidFile(File pidFile) {
    if (pidFile.exists()) {
      Integer pid = readPidFile(pidFile);
      if (null == pid) {
        LOG.error("PID file already exists, but couldn't be read: " + pidFile.getAbsolutePath()
            + " May need to be cleaned manually.");
      } else {
        LOG.error("Upgrade check-in daemon already running with PID: " + pid);
      }
      return false;
    }
    try {
      CheckinUtils.writeObjectToFile(pidFile, getPid());
      pidFile.deleteOnExit();
    } catch (Exception e) {
      LOG.error("Error encountered while writing PID file: " + pidFile.getAbsolutePath(), e);
      return false;
    }
    return true;
  }

  /**
   * Signals to the check-in thread that it should terminate.
   */
  private void shutdownCheckinThread() {
    mCheckinThread.finish();
    mCheckinThread.interrupt();
  }

  /**
   * Blocks until the check-in thread is dead.
   */
  private void waitForCheckinThreadShutdown() {
    try {
      mCheckinThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for check-in thread to shutdown.", e);
    }
  }

  /**
   * Installs a shutdown hook with the runtime that gracefully stops the check-in thread. The
   * shutdown hook should run when SIGTERM is caught by the jvm.
   */
  private void installShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Received TERM signal, so shutting down check-in thread.");
        shutdownCheckinThread();
      }
    });
  }

  /**
   * Launches the check-in thread and waits for its completion.
   *
   * @param args passed in from the command-line.
   * @return <code>0</code> if no errors are encountered, <code>1</code> otherwise.
   */
  public int run(String[] args) {
    // Parse the command-line arguments.
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      LOG.error("There was an error parsing command-line flags. Cannot continue.");
      return 1;
    }

    // Get the user's home directory.
    File homeDirectory = CheckinUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return 1;
    }

    // Get bento-cluster's state dir.
    File stateDir = getBentoClusterStateDir();
    if (null == stateDir) {
      return 1;
    }

    // Get the UUID of this user.
    String uuid = UUIDTools.getUserUUID(homeDirectory);
    if (null == uuid) {
      return 1;
    }

    // Get needed files, and URI for checkin server.
    File timestampFile = getUsageTimestampFile(homeDirectory);
    File upgradeInfoFile = getUpgradeInfoFile(stateDir);
    URI checkinServerURI = getUpgradeServerURI();
    if (null == checkinServerURI) {
      return 1;
    }

    // Create an upgrade server client for use with the check-in thread.
    HttpClient httpClient = new DefaultHttpClient();
    CheckinClient upgradeClient = CheckinClient.create(httpClient, checkinServerURI);

    // Install a shutdown hook that will take care of shutting down the check-in thread when this
    // process is killed.
    installShutdownHook();

    // Write a PID file, and stop the world if we can't.
    File pidFile = getPidFile(stateDir);
    if (!createPidFile(pidFile)) {
      return 1;
    }

    // Create a check-in thread and start it.
    mCheckinThread = new CheckinThread(
        uuid,
        timestampFile,
        upgradeInfoFile,
        mCheckinPeriodMillis,
        upgradeClient);
    mCheckinThread.start();

    // Wait until the shutdown hook stops the thread.
    waitForCheckinThreadShutdown();
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args passed in from the command-line.
   */
  public static void main(String[] args) {
    System.exit(new UpgradeDaemonTool().run(args));
  }
}
