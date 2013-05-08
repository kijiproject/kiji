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

package org.kiji.mapreduce.platform;

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * Hadoop 1.x-backed implementation of the KijiMRPlatformBridge API.
 */
@ApiAudience.Private
public final class Hadoop1xKijiMRBridge extends KijiMRPlatformBridge {
  private static final Logger LOG = LoggerFactory.getLogger(Hadoop1xKijiMRBridge.class);

  /** {@inheritDoc} */
  @Override
  public TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID id) {
    // Create an instance of this class directly.
    return new TaskAttemptContext(conf, id);
  }

  /** {@inheritDoc} */
  @Override
  public TaskAttemptID newTaskAttemptID(String jtIdentifier, int jobId, TaskType type,
      int taskId, int id) {
    // In Hadoop 1.0, TaskType isn't an arg to TaskAttemptID; instead, there's just a
    // boolean indicating whether it's a map task or not.
    boolean isMap = type == TaskType.MAP;
    return new TaskAttemptID(jtIdentifier, jobId, isMap, taskId, id);
  }


  /** {@inheritDoc} */
  @Override
  public SequenceFile.Writer newSeqFileWriter(Configuration conf, Path filename,
      Class<?> keyClass, Class<?> valueClass) throws IOException {
    Preconditions.checkArgument(conf != null, "Configuration argument must be non-null");
    Preconditions.checkArgument(filename != null, "Filename argument must be non-null");

    final URI fileUri = filename.toUri();
    final FileSystem fs = FileSystem.get(fileUri, conf);
    return new SequenceFile.Writer(fs, conf, filename, keyClass, valueClass);
  }

  /** {@inheritDoc} */
  @Override
  public void setUserClassesTakesPrecedence(Job job, boolean value) {
    // Underlying configuration key is not respected in hadoop 1.x.
    LOG.warn("Cannot setUserClassesTakesPrecedence in Hadoop1.x.  User classes will be added to "
        + "the task classpath after Hadoop and Kiji dependencies.");
  }
}

