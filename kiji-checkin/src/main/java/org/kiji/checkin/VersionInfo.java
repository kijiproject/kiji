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

/**
 * Provides some static helper methods to query for package name and version given a class
 * object. This will query the class's manifest information to return the proper information.
 *
 * The assumption here is that the class used for version and package name querying resides
 * in a properly formed jar that has a proper manifest.
 */
public final class VersionInfo {

  /**
   * Private constructor, as this is a utility class.
   */
  private VersionInfo() {
  }

  /**
   * Returns the software package.
   *
   * @param representativeClass is the class from which to fetch manifest information.
   * @return the title of the package.
   * @throws IOException if there is an exception.
   */
  public static String getSoftwarePackage(Class<?> representativeClass) throws IOException {
    String title = representativeClass.getPackage().getImplementationTitle();
    if (title != null) {
      return title;
    } else {
      return "unknown";
    }
  }

  /**
   * Gets the version of the Kiji BentoBox.
   *
   * @param representativeClass is the class from which to fetch manifest information.
   * @return The version string.
   * @throws IOException on I/O error.
   */
  public static String getSoftwareVersion(Class<?> representativeClass) throws IOException {
    final String version = representativeClass.getPackage().getImplementationVersion();
    if (version != null) {
      // Proper release: use the value of 'Implementation-Version' in META-INF/MANIFEST.MF:
      return version;
    } else {
      return "unknown";
    }
  }
}
