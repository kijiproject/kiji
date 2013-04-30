/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.rest.util;

/**
 * Reports on the version numbers associated with this software bundle
 * as well as the installed format versions in used in a Kiji instance.
 */
public final class VersionInfo {
  /** No constructor since this is a utility class. */
  private VersionInfo() {}

  private static final String KIJI_REST_VERSION = "0.1.0";

  /**
   * Gets the version of the KijiREST.
   *
   * @return The version string.
   */
  public static String getRESTVersion() {
    return KIJI_REST_VERSION;
  }
}
