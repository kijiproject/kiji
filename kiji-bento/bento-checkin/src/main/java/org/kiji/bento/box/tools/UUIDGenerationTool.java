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
import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.box.BentoBoxUtils;

/**
 * <p> A tool that generates a UUID identifying the user currently running a BentoBox.
 * The tool will check the user's home directory for a file named <code>.kiji-bento-uuid</code>.
 * If the file exists, nothing will be done. If not, a new UUID will be generated and written
 * to that file.</p>
 */
public final class UUIDGenerationTool {
  private static final Logger LOG = LoggerFactory.getLogger(UUIDGenerationTool.class);
  /** The name of the file to which UUIDs should be written. */
  private static final String UUID_FILE_NAME = ".kiji-bento-uuid";

  /**
   * Determines if a UUID file exists in the specified directory.
   *
   * @param directory will be checked for a file named ".kiji-bento-uuid".
   * @return <code>true</code> if the UUID file exists in the specified directory,
   *     <code>false</code> otherwise.
   */
  boolean isUUIDFileExists(File directory) {
    File uuidFile = new File(directory, UUID_FILE_NAME);
    return uuidFile.exists() && uuidFile.isFile();
  }

  /**
   * Writes a file containing a UUID to the specified directory. The file will be named
   * <code>.kiji-bento-uuid</code> and will contain one line, which will be the UUID specified.
   *   .
   *
   * @param directory is where the UUID file <code>.kiji-bento-uuid</code> should be written.
   * @param uuid is the UUID to write.
   * @throws IOException if there is a problem writing the file.
   */
  void writeUUID(File directory, UUID uuid) throws IOException {
    File uuidFile = new File(directory, UUID_FILE_NAME);
    BentoBoxUtils.writeObjectToFile(uuidFile, uuid);
  }

  /**
   * Writes a randomly generated UUID to the file <code>.kiji-bento-uuid</code> in the specified
   * directory. The file is not written if it already exists. Any {@link Exception}s encountered
   * are swallowed by this method and logged.
   *
   * @param directory is where the UUID file <code>.kiji-bento-uuid</code> should be written.
   * @return <code>true</code> if no errors are encountered, <code>false</code> otherwise.
   */
  boolean generateAndWriteUUID(File directory) {
    try {
      /** Only write a new UUID if an existing one does not exist. */
      if (!isUUIDFileExists(directory)) {
        UUID uuid = UUID.randomUUID();
        writeUUID(directory, uuid);
      }
      return true;
    } catch (Exception e) {
      LOG.error("UUID generation failed. The following exception was encountered while trying "
          + "to write the UUID file.", e);
      return false;
    }
  }

  /**
   * Writes a randomly generated UUID to the file <code>.kiji-bento-uuid</code> in the home
   * directory of the user running this tool. The file is not written if it already exists.
   *
   * @return <code>0</code> if no errors are encountered, <code>1</code> otherwise.
   */
  public int run() {
    // Get the home directory or fail if there's a problem.
    File homeDirectory = BentoBoxUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return 1;
    }

    // Return 0 if write was successful, 1 otherwise.
    return generateAndWriteUUID(homeDirectory) ? 0 : 1;
  }

  /**
   * Java program entry point.
   *
   * @param args are the (ignored) command-line arguments.
   */
  public static void main(String[] args) {
    System.exit(new UUIDGenerationTool().run());
  }
}
