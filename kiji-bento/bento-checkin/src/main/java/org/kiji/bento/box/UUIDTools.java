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
      return BentoBoxUtils.readFileAsString(uuidFile).trim();
    } catch (Exception e) {
      LOG.error("An exception was encountered while reading the user's UUID from the file: "
          + uuidFile.getAbsolutePath(), e);
      return null;
    }
  }

  /**
   * Gets the UUID for the current user by checking her home directory for
   * <code>.kiji-bento-uuid</code>.
   *
   * @return the UUID read, or <code>null</code> if there was an error doing so.
   */
  public static String getUserUUID() {
    File homeDirectory = BentoBoxUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return null;
    }
    return getUserUUID(homeDirectory);
  }
}
