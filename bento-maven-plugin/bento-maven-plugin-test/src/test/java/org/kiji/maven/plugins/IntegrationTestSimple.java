/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.maven.plugins;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Set;

import static junit.framework.Assert.assertTrue;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import static org.apache.hadoop.io.SequenceFile.*;

/**
 * Run this integration test against the bento-maven-plugin to demonstrate that the plugin works.
 * // TODO: Test access HDFS, HBase, etc.
 */
public class IntegrationTestSimple {
  @Test
  public void testHDFS() throws Exception {
    Configuration conf = new Configuration(); // takes default conf
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(new Path("/"));
    // Collect directory names.
    Set<String> directoryNames = Sets.newHashSet();
    for (FileStatus file : files) {
      directoryNames.add(file.getPath().getName());
    }
    assertTrue("Remote HDFS must have /hbase/", directoryNames.contains("hbase"));
    assertTrue("Remote HDFS must have /user/", directoryNames.contains("user"));
    assertTrue("Remote HDFS must have /var/", directoryNames.contains("var"));
    assertTrue("Remote HDFS must have /tmp", directoryNames.contains("tmp"));
  }
}
