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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.VersionInfo;
import org.kiji.checkin.models.UpgradeResponse;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * <p>A tool that reminds the user of an available upgrade to Kiji BentoBox. The information
 * about an available upgrade is taken from a file named <code>.kiji-bento-upgrade</code> assumed
 * to be in the directory specified by the command-line flag <code>--input-dir</code>. The period
 * between upgrade reminders can be configured with the command-line flag
 * <code>--reminder-period-millis</code>.</p>
 *
 * <p>The tool will check for an upgrade information file in the directory specified. This file
 * will contain a JSON representation of information for an upgrade,
 * which also includes a timestamp of the last time the user was reminded of the upgrade. If it
 * is time for an upgrade (as determined using the reminder timestamp in the JSON and the
 * reminder period specified) the user will be reminded, and the upgrade information file will be
 * rewritten with an updated timestamp.</p>
 *
 * <p>This tool is resilient to stale upgrade files, in that if the upgrade file contains
 * information for a version not greater than the current version, the user will not be reminded.
 * This tool also swallows and logs any {@link Exception} encountered while running. Clients of
 * this tool should ensure that logs are redirected to an appropriate location to ensure that
 * users are not unnecessarily bothered.</p>
 */
public final class UpgradeInformerTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeInformerTool.class);

  /** The name of the file in the user's home directory that contains upgrade information. */
  private static final String UPGRADE_FILE_NAME = ".kiji-bento-upgrade";

  @Flag(name = "input-dir", usage = "The path to a directory containing an upgrade info file.")
  private String mUpgradeDirPath = "";

  @Flag(name = "reminder-period-millis", usage = "The minimum number of milliseconds that should "
      + "pass between reminders to the user about an upgrade.")
  /** Use a default of one day between reminders. */
  private long mReminderPeriodMillis = 24 * 60 * 60 * 1000;

  /**
   * Reads upgrade information from the file <code>.kiji-bento-upgrade</code> in the specified
   * directory. If an {@link Exception} is encountered while reading the file,
   * it is swallowed and logged.
   *
   * @param directory contains the upgrade information file.
   * @return an upgrade response containing the upgrade information, or <code>null</code> if the
   *     file does not exist or an error was encountered while reading the file.
   */
  private UpgradeResponse getSavedUpgradeResponse(File directory) {
    File upgradeFile = new File(directory, UPGRADE_FILE_NAME);
    try {
      return UpgradeResponse.fromFile(upgradeFile);
    } catch (Exception e) {
      LOG.warn("Couldn't read upgrade info file: " + upgradeFile.getAbsolutePath(), e);
      return null;
    }
  }

  /**
   * Writes upgrade information to the file <code>.kiji-bento-upgrade</code> in the specified
   * directory. If an {@link Exception} is encountered while reading the file,
   * it is swallowed and logged.
   *
   * @param directory where the upgrade information file will be written.
   * @param response that should be written to the directory.
   * @return <code>true</code> if the file is written without error, <code>false</code> otherwise.
   */
  private boolean writeUpgradeResponse(File directory, UpgradeResponse response) {
    File upgradeFile = new File(directory, UPGRADE_FILE_NAME);
    try {
      response.write(upgradeFile);
      return true;
    } catch (Exception e) {
      LOG.error("There was an error writing an updated upgrade info file: "
          + upgradeFile.getAbsolutePath(), e);
      return false;
    }
  }

  /**
   * Gets the current version of Kiji BentoBox running.
   *
   * @return the current software version, or <code>null</code> if an error is encountered while
   *     retrieving the version.
   */
  private String getCurrentVersion() {
    try {
      return VersionInfo.getSoftwareVersion(this.getClass());
    } catch (Exception e) {
      LOG.error("There was an error getting the current software version. Cannot advise user of "
          + "upgrades.", e);
      return null;
    }
  }

  /**
   * Retrieves the directory that may contain an upgrade information file. The path to the
   * directory is obtained from the command-line flag <code>--input-dir</code>.
   *
   * @return the directory, or <code>null</code> if <code>--input-dir</code> was not specified,
   *     or if the path specified does not exist or is not a directory.
   */
  private File getUpgradeDirectory() {
    if (null == mUpgradeDirPath || mUpgradeDirPath.isEmpty()) {
      LOG.error("The flag --input-dir specifying a path containing an upgrade info file was not "
          + "specified. Cannot continue.");
      return null;
    }

    File upgradeDirectory = new File(mUpgradeDirPath);
    if (!upgradeDirectory.exists() || !upgradeDirectory.isDirectory()) {
      LOG.error("The path specified with --input-dir does not exist or is not a directory. "
          + "Cannot continue.");
      return null;
    }

    return upgradeDirectory;
  }

  /**
   * Advises the user of available upgrades, if an upgrade information file exists and if it is
   * time to remind the user of the upgrade.
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

    // First, get the current version of the software, or fail if it can't be retrieved.
    final String currentVersion = getCurrentVersion();
    if (null == currentVersion) {
      return 1;
    }

    // Get the directory containing the upgrade information file.
    File upgradeDirectory = getUpgradeDirectory();
    if (null == upgradeDirectory) {
      return 1;
    }

    // Get the saved upgrade response, or fail if there is none.
    UpgradeResponse response = getSavedUpgradeResponse(upgradeDirectory);
    if (null == response) {
      return 1;
    }

    // If the upgrade information refers to a later version than the one currently running, and
    // if it's time to give a reminder, do so.
    if (response.isTimeToRemind(mReminderPeriodMillis) && response.isRelevant(currentVersion)) {
      System.out.println(response.getUpgradeReminder(currentVersion));
      response.justReminded();
      return writeUpgradeResponse(upgradeDirectory, response) ? 0 : 1;
    }

    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args passed in from the command-line.
   */
  public static void main(String[] args) {
    System.exit(new UpgradeInformerTool().run(args));
  }
}
