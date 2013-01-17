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

package org.kiji.mapreduce.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.kiji.annotations.ApiAudience;

/** Utility class for dealing with Java Jar files and their contained classes. */
@ApiAudience.Private
public final class Jars {
  /** Configuration variable name to store jars that export to distributed cache. */
  private static final String TMPJARS_NAME = "tmpjars";

  /** No constructor for this utility class. */
  private Jars() {}

  /**
   * Finds the file path to the jar that contains a particular class.
   * Method mostly cloned from o.a.h.mapred.JobConf.findContainingJar().
   *
   * @param classObj The class of interest.
   * @return The path to the jar that contains <code>classObj</code>.
   * @throws ClassNotFoundException If the class cannot be found in a jar.
   * @throws IOException If there is a problem reading jars from the file system.
   */
  public static String getJarPathForClass(Class<? extends Object> classObj)
      throws ClassNotFoundException, IOException {
    ClassLoader loader = classObj.getClassLoader();
    String classFile = classObj.getName().replaceAll("\\.", "/") + ".class";
    for (Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }
    throw new ClassNotFoundException(
        "Unable to find containing jar for class " + classObj.getName());
  }
}
