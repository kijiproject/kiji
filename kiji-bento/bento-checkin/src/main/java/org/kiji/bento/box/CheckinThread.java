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

package org.kiji.bento.box;

import java.io.File;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinClient;
import org.kiji.checkin.CheckinUtils;
import org.kiji.checkin.models.UpgradeCheckin;
import org.kiji.checkin.models.UpgradeResponse;

/**
 * A thread that periodically sends check-in messages to a BentoBox upgrade server, and writes
 * the information contained in responses to a file on the local machine.
 */
public final class CheckinThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(CheckinThread.class);

  /**
   * The user id to use in check-in messages.
   */
  private final String mUserId;

  /**
   * A file to read that contains a usage timestamp for the <code>kiji</code> script.
   */
  private final File mUsageTimestampFile;

  /**
   * A file that upgrade information should be written to.
   */
  private final File mUpgradeInfoFile;

  /**
   * The number of milliseconds between check-ins with the upgrade server.
   */
  private final long mCheckinPeriodMillis;

  /** The client used to send check-in messages to the upgrade server and receive responses. */
  private final CheckinClient mUpgradeServerClient;

  /**
   * A flag to indicate when the thread should terminate.
   */
  private boolean mFinished;

  /**
   * Creates a new instance of this thread.
   *
   * @param userId to send with check-in messages.
   * @param usageTimestampFile to read a usage timestamp for the kiji script from.
   * @param upgradeInfoFile to write responses sent by the server to.
   * @param checkinPeriodMillis between check-ins with the upgrade server.
   * @param client to use to communicate with the upgrade server.
   */
  public CheckinThread(String userId, File usageTimestampFile, File upgradeInfoFile,
      long checkinPeriodMillis, CheckinClient client) {
    mUserId = userId;
    mUsageTimestampFile = usageTimestampFile;
    mUpgradeInfoFile = upgradeInfoFile;
    mCheckinPeriodMillis = checkinPeriodMillis;
    mUpgradeServerClient = client;
    mFinished = false;
  }

  /**
   * Signals to this thread that it should terminate.
   */
  public synchronized void finish() {
    mFinished = true;
  }

  /**
   * @return <code>true</code> if the thread has been signaled to terminate, <code>false</code>
   *     otherwise.
   */
  private synchronized boolean isFinished() {
    return mFinished;
  }

  /**
   * Reads the usage timestamp file for the kiji script. This method swallows and logs any
   * {@link Exception}s encountered.
   *
   * @return the timestamp contained in the file, or <code>null</code> if an error was encountered.
   */
  private Long getKijiUsageTimestampMillis() {
    try {
      String timestampStr = CheckinUtils.readFileAsString(mUsageTimestampFile).trim();
      return Long.parseLong(timestampStr);
    } catch (FileNotFoundException e) {
      LOG.warn("The kiji usage timestamp file was not found. This is normal if the kiji script "
          + "included with BentoBox has not been used yet. File: "
          + mUsageTimestampFile.getAbsolutePath());
      return 0L;
    } catch (Exception e) {
      LOG.error("An error was encountered while reading usage timestamp file: "
          + mUsageTimestampFile.getAbsolutePath(), e);
      return null;
    }
  }

  /**
   * Writes a response from the upgrade server to a file. This method swallows and logs any
   * {@link Exception}s encountered.
   *
   * @param response to write to the file.
   * @return <code>true</code> if the file was written successfully, <code>false</code> otherwise.
   */
  private boolean writeUpgradeInfoFile(UpgradeResponse response) {
    try {
      response.write(mUpgradeInfoFile);
      return true;
    } catch (Exception e) {
      LOG.error("An error was encountered while attempting to write an upgrade response from "
          + "the server to file: " + mUpgradeInfoFile.getAbsolutePath(), e);
      return false;
    }
  }

  /**
   * Creates a message to send to the upgrade server using the specified timestamp and the user
   * ID associated with this thread. This method swallows and logs any {@link Exception}s
   * encountered.
   *
   * @param usageTimestamp to include in the message.
   * @return the message created, or <code>null</code> if an error was encountered.
   */
  private UpgradeCheckin getCheckinMessage(Long usageTimestamp) {
    try {
      return new UpgradeCheckin.Builder(this.getClass())
          .withId(mUserId)
          .withLastUsedMillis(usageTimestamp)
          .build();
    } catch (Exception e) {
      LOG.error("An error was encountered while attempting to create a check-in message to "
          + "send to the upgrade server.", e);
      return null;
    }
  }

  /**
   * Sends a check-in message to the upgrade server. This method swallows and logs any
   * {@link Exception}s encountered.
   *
   * @param checkin message to send to the upgrade server.
   * @return the server's response, or <code>null</code> if an error was encountered sending the
   *     message.
   */
  private UpgradeResponse checkin(UpgradeCheckin checkin) {
    try {
      return mUpgradeServerClient.checkin(checkin);
    } catch (Exception e) {
      LOG.error("An error was encountered while sending a check-in to the upgrade server at "
          + mUpgradeServerClient.getServerURI().toString(), e);
      return null;
    }
  }

  /**
   * Periodically sends check-in messages to the upgrade server.
   */
  @Override
  public void run() {
    while (!isFinished()) {
      // Read the usage timestamp from a file.
      Long usageTimestamp = getKijiUsageTimestampMillis();
      if (null == usageTimestamp) {
        // Couldn't get the timestamp, something is likely very wrong, stop sending messages.
        finish();
        break;
      }

      // Get a checkin message to send.
      UpgradeCheckin checkin = getCheckinMessage(usageTimestamp);
      if (null == checkin) {
        // Couldn't get the check-in message, something is likely very wrong,
        // stop sending messages.
        finish();
        break;
      }

      // Now actually check-in.
      UpgradeResponse checkinResponse = checkin(checkin);
      // If an error was encountered, check-in response will be null. This might just be a
      // transient network error, so we'll continue anyway.
      if (null != checkinResponse) {
        // Write the check-in response to a file.
        if (!writeUpgradeInfoFile(checkinResponse)) {
          // Couldn't write the upgrade info file, something is probably very wrong.
          finish();
          break;
        }
      }

      // Now sleep until it's time to send another check-in.
      try {
        Thread.sleep(mCheckinPeriodMillis);
      } catch (InterruptedException e) {
        // We'll get interrupted on termination. So let's make sure we're finished and stop.
        finish();
        break;
      }
    }

    // After we're done running, we should close resources.
    mUpgradeServerClient.close();
  }
}
