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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities regarding UUID file management.
 */
public final class UUIDTools {
  private static final Logger LOG = LoggerFactory.getLogger(UUIDTools.class.getName());

  /** Prevent instantiation. */
  private UUIDTools() { }

  /** The name of the file where the user's unique and anonymous ID is stored. */
  public static final String UUID_FILE_NAME = ".kiji-bento-uuid";

  /**
   * Gets a UUID for the user by reading the file <code>.kiji-bento-uuid</code> from a directory.
   *
   * @param directory that should contain the UUID file.
   * @return the UUID read, or <code>null</code> if there was an error doing so.
   */
  public static String getUserUUID(File directory) {
    File uuidFile = new File(directory, UUID_FILE_NAME);
    try {
      return CheckinUtils.readFileAsString(uuidFile).trim();
    } catch (Exception e) {
      LOG.error("An exception was encountered while reading the user's UUID from the file: "
          + uuidFile.getAbsolutePath(), e);
      return null;
    }
  }

  /**
   * Gets the UUID for the current user by checking her home directory for
   * <code>.kiji-bento-uuid</code>. If this file does not exist, generates a
   * new UUID and creates the file.
   *
   * @return the UUID read or created, or <code>null</code> if it couldn't perform I/O to persist
   *     or receive a UUID correctly.
   */
  public static String getOrCreateUserUUID() {
    final File homeDirectory = CheckinUtils.getHomeDirectory();
    if (null == homeDirectory) {
      LOG.error("Could not determine your home directory.");
      return null;
    }
    if (!isUUIDFileExists(homeDirectory)) {
      if (!generateAndWriteUUID(homeDirectory)) {
        LOG.error("Could not save our UUID token to your home directory.");
        return null; // encountered an error.
      }
    }

    return getUserUUID();
  }

  /**
   * Gets the UUID for the current user by checking her home directory for
   * <code>.kiji-bento-uuid</code>.
   *
   * @return the UUID read, or <code>null</code> if there was an error doing so.
   */
  public static String getUserUUID() {
    File homeDirectory = CheckinUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return null;
    }
    return getUserUUID(homeDirectory);
  }

  /**
   * Determines if a UUID file exists in the specified directory.
   *
   * @param directory will be checked for a file named ".kiji-bento-uuid".
   * @return <code>true</code> if the UUID file exists in the specified directory,
   *     <code>false</code> otherwise.
   */
  public static boolean isUUIDFileExists(File directory) {
    File uuidFile = new File(directory, UUID_FILE_NAME);
    return uuidFile.exists() && uuidFile.isFile();
  }

  /**
   * Writes a file containing a UUID to the specified directory. The file will be named
   * <code>.kiji-bento-uuid</code> and will contain one line, which will be the UUID specified.
   *
   * @param directory is where the UUID file <code>.kiji-bento-uuid</code> should be written.
   * @param uuid is the UUID to write.
   * @throws IOException if there is a problem writing the file.
   */
  static void writeUUID(File directory, UUID uuid) throws IOException {
    File uuidFile = new File(directory, UUID_FILE_NAME);
    CheckinUtils.writeObjectToFile(uuidFile, uuid);
  }

  /**
   * Writes a randomly generated UUID to the file <code>.kiji-bento-uuid</code> in the specified
   * directory. The file is not written if it already exists. Any {@link Exception}s encountered
   * are swallowed by this method and logged.
   *
   * @param directory is where the UUID file <code>.kiji-bento-uuid</code> should be written.
   * @return <code>true</code> if no errors are encountered, <code>false</code> otherwise.
   */
  public static boolean generateAndWriteUUID(File directory) {
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
}
