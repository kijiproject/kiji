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

package org.kiji.express.tool

import java.io.File

import org.kiji.mapreduce.util.Jars
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * Reads a colon-separated list of classpath entries and outputs comma-separated list of
 * URIs to jar files accessible from that classpath, as well as the HBase library jars.
 *
 * This tool is meant to be used to specify a collection of jars that should be included on the
 * distributed cache of Hadoop jobs run through KijiExpress. It should be used from the `express`
 * script and should be passed a classpath entry for the KijiExpress `lib` directory as well
 * as any classpath entries specified by the user through `EXPRESS_CLASSPATH`.
 *
 * Classpath entries can come in one of three forms:
 *
 * 1. A path to a specific file. In this case we send the file through the distributed cache if
 * it is a jar.
 * 2. A path to a directory. Since directory entries do not include jars on the classpath,
 * we send no jars through the distributed cache.
 * 3. A path to a directory with the wildcard * appended. In this case all jars under the
 * specified directory are sent through the distributed cache.
 *
 * This tool accepts one command line argument: a string containing a colon-separated list of
 * classpath entries.
 */
object TmpJarsTool {

  /**
   * Constructs a file from a classpath entry.
   *
   * @param entry the file will be constructed from.
   * @return a file for the classpath entry.
   */
  private[tool] def entryToFile(entry: String): File = new File(entry)

  /**
   * If a file is a glob entry, transform it into all files present in the globed directory.
   *
   * @param maybeGlob is a file that might be a glob entry from the classpath.
   * @return the original file if it was not a glob, the files present in the globed directory
   *     otherwise.
   */
  private[tool] def globToFiles(maybeGlob: File): Array[File] = maybeGlob.getName match {
    case "*" => maybeGlob.getParentFile
                .listFiles()
                .filterNot { _.isDirectory }
    case _ => Array(maybeGlob)
  }

  /**
   * Determines if a file has the `.jar` or `.JAR` extensions.
   *
   * @param maybeJar is a file that may, in fact, be a jar.
   * @return `true` if the file is jar, `false` otherwise.
   */
  private[tool] def isJar(maybeJar: File): Boolean = {
    maybeJar.getName.endsWith(".jar") || maybeJar.getName.endsWith(".JAR")
  }

  /**
   * Transforms a colon-separated list of classpath entries into the jar files accessible from
   * the classpath entries.
   *
   * @param classpath is a colon-separated list of classpath entries.
   * @return the jar files accessible from the classpath entries.
   */
  private[tool] def getJarsFromClasspath(classpath: String): Array[File] = {
    classpath
        .split(':')
        .filterNot { _.isEmpty }
        .map { entryToFile }
        .filterNot { _.isDirectory }
        .flatMap { globToFiles }
        .filter { isJar }
  }

  /**
   * Retrieves all jars located in the directory containing the HBase library jar.
   *
   * @return the jar files contained in the HBase lib directory.
   */
  private def getJarsForHBase: Array[File] = {
    val hbaseJar: File = new File(Jars.getJarPathForClass(classOf[HBaseConfiguration]))
    hbaseJar.getParentFile.listFiles()
        .filterNot { _.isDirectory }
        .filter { isJar }
  }

  /**
   * Formats the paths to jar files as a comma-separated list of URIs to files on the local file
   * system.
   *
   * @param jars that should be added to the generated comma-separated list.
   * @return a comma-separated list of URIs to jar files.
   */
  private[tool] def toJarURIList(jars: Array[File]): String = {
    val pathToPathWithScheme = (path: String) => "file://" + path
    val joinPaths = (path1: String, path2: String) => path1 + "," + path2
    jars
        .map { _.getCanonicalPath }
        .map { pathToPathWithScheme }
        .reduce { joinPaths }
  }

  /**
   * Transforms a classpath into a comma-separated list of URIs to jar files accessible from the
   * classpath.
   *
   * @param args from the command line, which should only include a colon-separated classpath.
   */
  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: TmpJarsTool <classpath>")
    }
    val jarsFromClasspath = getJarsFromClasspath(args(0))
    val jarsFromHBase = getJarsForHBase
    val jarsForDCache = jarsFromClasspath ++ jarsFromHBase
    println(toJarURIList(jarsForDCache))
  }
}
