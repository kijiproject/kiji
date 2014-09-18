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

import org.kiji.checkin.CheckinUtils;
import org.kiji.checkin.UUIDTools;

/**
 * <p> A tool that generates a UUID identifying the user currently running a BentoBox.
 * The tool will check the user's home directory for a file named <code>.kiji-bento-uuid</code>.
 * If the file exists, nothing will be done. If not, a new UUID will be generated and written
 * to that file.</p>
 */
public final class UUIDGenerationTool {

  /**
   * Writes a randomly generated UUID to the file <code>.kiji-bento-uuid</code> in the home
   * directory of the user running this tool. The file is not written if it already exists.
   *
   * @return <code>0</code> if no errors are encountered, <code>1</code> otherwise.
   */
  public int run() {
    // Get the home directory or fail if there's a problem.
    File homeDirectory = CheckinUtils.getHomeDirectory();
    if (null == homeDirectory) {
      return 1;
    }

    // Return 0 if write was successful, 1 otherwise.
    return UUIDTools.generateAndWriteUUID(homeDirectory) ? 0 : 1;
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
