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

package org.kiji.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/** Utility class for dealing with Java JAR files and the hadoop distributed cache. */
@ApiAudience.Public
public final class DistributedCacheJars {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedCacheJars.class);

  /**
   * Configuration variable name to store jars that export to distributed cache.
   *
   * The value associated to this variable is a comma-separated list of jar file paths.
   *
   * @see org.apache.hadoop.mapred.JobClient
   */
  private static final String CONF_TMPJARS = "tmpjars";

  /** No constructor for this utility class. */
  private DistributedCacheJars() {
  }

  /**
   * Fully qualifies a path if necessary.
   *
   * If path is not fully qualified, the default FS from the given configuration is used to
   * qualify the path.
   *
   * @param pathStr Path to qualify.
   * @param conf Configuration according to which qualification happens.
   * @return fully-qualified path.
   * @throws IOException on I/O error.
   */
  private static Path qualifiedPathFromString(String pathStr, Configuration conf)
      throws IOException {
    final Path path = new Path(pathStr);
    return path.getFileSystem(conf).makeQualified(path);
  }

  /**
   * Adds the jars from a directory into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarDirectory A path to a directory of jar files.
   * @throws IOException on I/O error.
   */
  public static void addJarsToDistributedCache(Job job, String jarDirectory) throws IOException {
    addJarsToDistributedCache(job, qualifiedPathFromString(jarDirectory, job.getConfiguration()));
  }

  /**
   * Adds the jars from a directory into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarDirectory A path to a directory of jar files.
   * @throws IOException on I/O error.
   */
  public static void addJarsToDistributedCache(Job job, File jarDirectory) throws IOException {
    addJarsToDistributedCache(job, new Path("file:" + jarDirectory.getCanonicalPath()));
  }

  /**
   * Adds the jars from a directory into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarDirectory A path to a directory of jar files.
   * @throws IOException on I/O error.
   */
  public static void addJarsToDistributedCache(Job job, Path jarDirectory) throws IOException {
    if (null == jarDirectory) {
      throw new IllegalArgumentException("Jar directory may not be null");
    }
    addJarsToDistributedCache(job, listJarFilesFromDirectory(job.getConfiguration(), jarDirectory));
  }

  /**
   * Adds the jar files into the distributed cache of a job.
   *
   * @param job The job to configure.
   * @param jarFiles Collection of jar files to add.
   * @throws IOException on I/O error.
   */
  public static void addJarsToDistributedCache(Job job, Collection<Path> jarFiles)
      throws IOException {
    // Get existing jars named in configuration.
    final List<Path> allJars =
        Lists.newArrayList(getJarsFromConfiguration(job.getConfiguration()));

    // Add jars from jarDirectory.
    for (Path path : jarFiles) {
      final Path qualifiedPath = path.getFileSystem(job.getConfiguration()).makeQualified(path);
      LOG.debug("Adding jar {}, fully qualified as {}", path, qualifiedPath);
      allJars.add(qualifiedPath);
    }

    // De-duplicate the list of jar files, based on their names:
    final Collection<Path> deDupedJars = deDuplicateFilenames(allJars);
    job.getConfiguration().set(CONF_TMPJARS, StringUtils.join(deDupedJars, ","));
  }

  /**
   * Lists all jars in the variable tmpjars of this Configuration.
   *
   * @param conf The Configuration to get jar names from
   * @return A list of jars.
   */
  public static List<Path> getJarsFromConfiguration(Configuration conf) {
    final String existingJars = conf.get(CONF_TMPJARS);
    if ((null == existingJars) || existingJars.isEmpty()) {
      return Collections.emptyList();
    }
    final List<Path> jarFiles = Lists.newArrayList();
    for (String jarPath : existingJars.split(",")) {
      jarFiles.add(new Path(jarPath));
    }
    return jarFiles;
  }

  private static final PathFilter JAR_PATH_FILTER;
  static {
    try {
      JAR_PATH_FILTER = new GlobFilter("*.jar");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Lists all jars in the specified directory.
   *
   * @param conf Configuration to get FileSystem from
   * @param jarDirectory The directory of jars to get.
   * @return A list of qualified paths to the jars in jarDirectory.
   * @throws IOException if there's a problem.
   */
  public static Collection<Path> listJarFilesFromDirectory(Configuration conf, Path jarDirectory)
      throws IOException {
    LOG.debug("Listing jar files {}/*.jar", jarDirectory);
    final FileSystem fs = jarDirectory.getFileSystem(conf);
    if (!fs.isDirectory(jarDirectory)) {
      throw new IOException("Attempted to add jars from non-directory: " + jarDirectory);
    }
    final List<Path> jarFiles = Lists.newArrayList();
    for (FileStatus status : fs.listStatus(jarDirectory, JAR_PATH_FILTER)) {
      if (!status.isDirectory()) {
        jarFiles.add(fs.makeQualified(status.getPath()));
      }
    }
    return jarFiles;
  }

  /**
   * Removes files whose name are duplicated in a given collection.
   *
   * @param jarFiles Collection of .jar files to de-duplicate.
   * @return De-duplicated collection of .jar files.
   */
  public static List<Path> deDuplicateFilenames(Iterable<Path> jarFiles) {
    final Set<String> jarFileNames = Sets.newHashSet();
    final List<Path> uniqueFiles = Lists.newArrayList();
    for (Path jarFile : jarFiles) {
      if (jarFileNames.add(jarFile.getName())) {
        uniqueFiles.add(jarFile);
      }
    }
    return uniqueFiles;
  }
}
