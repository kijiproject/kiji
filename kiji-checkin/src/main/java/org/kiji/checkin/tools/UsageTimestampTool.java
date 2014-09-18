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

package org.kiji.checkin.tools;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinUtils;

/**
 * <p>A tool that writes the last timestamp the <code>kiji</code> script within a BentoBox was
 * used. The tool will write the current time in milliseconds since the Unix epoch in a file
 * named <code>.kiji-last-used</code> in the user's home directory. If the file already exists,
 * it will be overwritten.</p>
 */
public final class UsageTimestampTool {
  private static final Logger LOG = LoggerFactory.getLogger(UsageTimestampTool.class);
  /** The name of the file the timestamp should be written to. */
  private static final String TIMESTAMP_FILE_NAME = ".kiji-last-used";

  /**
   * Writes a file containing a timestamp to the specified directory. The file will be named
   * <code>.kiji-last-used</code> and will contain one line, which will be the timestamp specified.
   *   .
   *
   * @param directory is where the timestamp file <code>.kiji-last-used</code> should be written.
   * @param timestamp to write to the file.
   * @throws java.io.IOException if there is a problem writing the file.
   */
  void writeTimestamp(File directory, Long timestamp) throws IOException {
    File timestampFile = new File(directory, TIMESTAMP_FILE_NAME);
    CheckinUtils.writeObjectToFile(timestampFile, timestamp);
  }

  /**
   * Writes the current time in milliseconds to the file <code>.kiji-last-used</code> in the
   * specified directory. Any {@link Exception}s encountered are swallowed by this method and
   * logged.
   *
   * @param directory is where the timestamp file <code>.kiji-last-used</code> should be written.
   * @return <code>true</code> if no errors are encountered, <code>false</code> otherwise.
   */
  boolean generateAndWriteTimestamp(File directory) {
    try {
      Long timestamp = System.currentTimeMillis();
      writeTimestamp(directory, timestamp);
      return true;
    } catch (Exception e) {
      LOG.error("The following exception was encountered while trying to write the timestamp "
          + "usage file.", e);
      return false;
    }
  }

  /**
   * Writes the current time in milliseconds to the file <code>.kiji-last-used</code> in the
   * home directory of the user running this tool.
   *
   * @return <code>0</code> if no errors are encountered, <code>1</code> otherwise.
   */
  public int run() {
    // Get the home directory and fail if there's a problem.
    File homeDirectory = CheckinUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return 1;
    }

    // Return 0 if write was successful, 1 otherwise.
    return generateAndWriteTimestamp(homeDirectory) ? 0 : 1;
  }

  /**
   * Java program entry point.
   *
   * @param args are the (ignored) command-line arguments.
   */
  public static void main(String[] args) {
    System.exit(new UsageTimestampTool().run());
  }
}
