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
package org.kiji.checkin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.models.KijiCommand;

/**
 * Simple class to log a command executed by a command line client.
 *
 */
public class CommandLogger {

  private static final Logger LOG = LoggerFactory.getLogger(CommandLogger.class);
  private URI mCheckinServerUri = null;
  private static final String KIJI_CHECKIN_SERVER = "BENTO_CHECKIN_SERVER";

  /**
   * Thread class to allow for asynchronous logging of commands.
   *
   */
  private class CommandLoggerThread extends Thread {
    private KijiCommand mCommand;

    /**
     * Constructs a new instance of the command logger thread.
     *
     * @param command is the command to log.
     */
    public CommandLoggerThread(KijiCommand command) {
      mCommand = command;
    }

    /**
     * Performs the actual logging.
     */
    public void run() {
      doLog(mCommand);
    }
  }

  /**
   * Constructs a new instance of the CommandLogger.
   */
  public CommandLogger() {
    // Environment variable set by script in kiji or bento.
    String checkinServerUrl = System.getenv(KIJI_CHECKIN_SERVER);
    if (checkinServerUrl != null) {
      try {
        // TODO: BENTO-37: environment variable should be a base URI
        // until then, let's do a few checks to make sure the suffix is correct
        if (checkinServerUrl.endsWith("/checkin") || checkinServerUrl.endsWith("/checkin/")) {
          checkinServerUrl = checkinServerUrl.replace("/checkin", "/command");
        } else if (!checkinServerUrl.endsWith("/command")) {
          String suffix = "command";
          if (!checkinServerUrl.endsWith("/")) {
            suffix = "/" + suffix;
          }
          checkinServerUrl += suffix;
        }

        mCheckinServerUri = new URI(checkinServerUrl);
      } catch (URISyntaxException e) {
        LOG.error("ERROR", e);
      }
    }
  }

  /**
   * Logs the command by POSTing the command to the Kiji checkin server.
   *
   * @param command is the command to log.
   * @param logAsynch determines whether or not to log the command asynchronously.
   */
  public void logCommand(KijiCommand command, boolean logAsynch) {
    if (mCheckinServerUri != null && !CheckinUtils.isCheckinDisabled()) {
      if (logAsynch) {
        Thread commandLogger = new CommandLoggerThread(command);
        commandLogger.setDaemon(true);
        commandLogger.start();
      } else {
        doLog(command);
      }
    }
  }

  /**
   * Performs the actual logging of the command.
   *
   * @param command is the command to log.
   */
  private void doLog(KijiCommand command) {
    HttpClient httpClient = new DefaultHttpClient();
    CheckinClient client = new CheckinClient(httpClient, mCheckinServerUri);
    try {
      LOG.debug("Logging " + command.toJSON() + " to " + mCheckinServerUri);
      client.postCommand(command);
      client.close();
    } catch (IOException ioe) {
      LOG.debug("ERROR Logging " + command.toJSON() + " to " + mCheckinServerUri, ioe);
    }
  }
}
