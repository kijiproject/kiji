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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains utility methods used by various classes.
 */
public final class BentoBoxUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BentoBoxUtils.class);

  /**
   * Cannot instantiate this utility class.
   */
  private BentoBoxUtils() { }

  /**
   * @return the home directory of the current user, or <code>null</code> if an error is
   * encountered.
   */
  public static File getHomeDirectory() {
    String homeDirectoryPath = System.getProperty("user.home");
    if (null == homeDirectoryPath || homeDirectoryPath.isEmpty()) {
      LOG.error("Retrieved a null or empty-string value for home directory.");
      return null;
    }
    File homeDirectory = new File(homeDirectoryPath);
    // Ensure there are no problems with the home directory.
    if (!homeDirectory.exists() || !homeDirectory.isDirectory()) {
      LOG.error("The following path was retrieved as the home directory, but it does not exist "
          + "or is not a directory: " + homeDirectoryPath);
      return null;
    }
    return homeDirectory;
  }

  /**
   * Writes the <code>toString</code> of an object to a file. The UTF-8 encoding is used.
   *
   * @param file will be written to.
   * @param object whose <code>toString</code> will be used to generate the file content.
   * @param <T> is the type of object being written.
   * @throws IOException if there is an error writing the file.
   */
  public static <T> void writeObjectToFile(File file, T object) throws IOException {
    OutputStream stream = null;
    OutputStreamWriter writer = null;
    try {
      stream = new FileOutputStream(file);
      writer = new OutputStreamWriter(stream, "UTF-8");
      IOUtils.write(object.toString() + "\n", writer);
    } finally {
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(stream);
    }
  }

  /**
   * Read the contents of a file as a string using the UTF-8 encoding.
   *
   * @param file will have its contents read into a string.
   * @return the file contents as a string.
   * @throws IOException if there is an error reading the file.
   */
  public static String readFileAsString(File file) throws IOException {
    InputStream stream = null;
    InputStreamReader reader = null;
    try {
      stream = new FileInputStream(file);
      reader = new InputStreamReader(stream, "UTF-8");
      return IOUtils.toString(reader);
    } finally {
      IOUtils.closeQuietly(reader);
      IOUtils.closeQuietly(stream);
    }
  }
}
