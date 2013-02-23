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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * <p>Provides a static method that can be used to obtain the version of Kiji BentoBox being
 * run.</p>
 *
 * <p>The version of the Kiji BentoBox is determined using one of the following methods (used in
 * the order in which they are tried).
 * <ol>
 *   <li>By querying <code>META-INF/MANFEST.MF</code> for the version.</li>
 *   <li>By querying the properties file <code>kiji-bento.properties</code> which should be
 *   included as a resource in the BentoBox jar.</li>
 *   <li>Using the string <code>development</code> as the version,
 *   since if the above two methods fail, we must be in a non-released development version or
 *   some other problem has occurred.</li>
 * </ol>
 * </p>
 */
public final class VersionInfo {

  /** Fallback software version ID, in case the properties file is not generated/reachable. */
  public static final String DEFAULT_DEVELOPMENT_VERSION = "development";

  /** A resource containing a properties file that includes a version string. */
  private static final String KIJI_BENTO_PROPERTIES_RESOURCE =
      "org/kiji/bento/box/kiji-bento.properties";

  /** The name of the property which holds the Kiji BentoBox version. */
  private static final String KIJI_BENTO_VERSION_PROP_NAME = "kiji-bento-version";

  /**
   * Private constructor, as this is a utility class.
   */
  private VersionInfo() { }

  /**
   * Loads Kiji BentoBox properties from the resource <code>org/kiji/bento/box/kiji-bento
   * .properties</code>.
   *
   * @return the Kiji BentoBox properties.
   * @throws IOException on I/O error.
   */
  private static Properties loadKijiSchemaProperties() throws IOException {
    final InputStream istream =
        VersionInfo.class.getClassLoader().getResourceAsStream(KIJI_BENTO_PROPERTIES_RESOURCE);
    try {
      final Properties properties = new Properties();
      properties.load(istream);
      return properties;
    } finally {
      IOUtils.closeQuietly(istream);
    }
  }

  /**
   * Gets the version of the Kiji BentoBox.
   *
   * @return The version string.
   * @throws IOException on I/O error.
   */
  public static String getSoftwareVersion() throws IOException {
    final String version = VersionInfo.class.getPackage().getImplementationVersion();
    if (version != null) {
      // Proper release: use the value of 'Implementation-Version' in META-INF/MANIFEST.MF:
      return version;
    }

    // Most likely a development version:
    final Properties kijiProps = loadKijiSchemaProperties();
    return kijiProps.getProperty(KIJI_BENTO_VERSION_PROP_NAME, DEFAULT_DEVELOPMENT_VERSION);
  }
}
