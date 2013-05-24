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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Lookups;

/**
 * Abstract representation of an underlying platform for KijiMR. This interface
 * is fulfilled by specific implementation providers that are dynamically chosen
 * at runtime based on the Hadoop &amp; HBase jars available on the classpath.
 */
@ApiAudience.Framework
public abstract class KijiMRPlatformBridge {
  /**
   * This API should only be implemented by other modules within KijiMR;
   * to discourage external users from extending this class, keep the c'tor
   * package-private.
   */
  KijiMRPlatformBridge() {
  }

  /**
   * Create and return a new TaskAttemptContext implementation parameterized by the specified
   * configuration and task attempt ID objects.
   *
   * @param conf the Configuration to use for the task attempt.
   * @param id the TaskAttemptID of the task attempt.
   * @return a new TaskAttemptContext.
   */
  public abstract TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID id);

  /**
   * Create and return a new TaskAttemptID object.
   *
   * @param jtIdentifier the jobtracker id.
   * @param jobId the job id number.
   * @param type the type of the task being created.
   * @param taskId the task id number within this job.
   * @param id the id number of the attempt within this task.
   * @return a newly-constructed TaskAttemptID.
   */
  public abstract TaskAttemptID newTaskAttemptID(String jtIdentifier, int jobId, TaskType type,
      int taskId, int id);


  /**
   * Create and return a new SequenceFile.Writer object.
   *
   * @param conf the current Configuration.
   * @param filename the file to open for write access.
   * @param keyClass the class representing the 'key' data type in the key-value pairs to write.
   * @param valueClass the class representing the 'value' data type in the key-value pairs to write.
   * @return a new SequenceFile.Writer object opened and ready to write to the file.
   * @throws IOException if there is an error opening the file.
   */
  public abstract SequenceFile.Writer newSeqFileWriter(Configuration conf, Path filename,
      Class<?> keyClass, Class<?> valueClass) throws IOException;


  /**
   * Create and return a new SequenceFile.Reader object.
   *
   * @param conf the current Configuration
   * @param filename the file to open for read access.
   * @return a new SequenceFile.Reader object opened and ready to read the file.
   * @throws IOException if there is an error opening the file.
   */
  public abstract SequenceFile.Reader newSeqFileReader(Configuration conf, Path filename)
      throws IOException;

  /**
   * Set the boolean property of a given Job for specifying which classpath takes precedence, the
   * user's or the system's, when tasks are launched.
   *
   * @param job the Job for which to set the property.
   * @param value the value to which to set the property.
   */
  public abstract void setUserClassesTakesPrecedence(Job job, boolean value);

  /**
   * Get a new Mapper.Context.
   *
   * @param conf the Hadoop Configuration used to configure the Context.
   * @param taskId the TaskAttemptID for the Context.
   * @param reader the RecordReader for the Context.
   * @param writer the RecordWriter for the Context.
   * @param committer the OutputCommit for the Context.
   * @param reporter the StatusReporter for the Context.
   * @param split the InputSplit for the Context.
   * @param <KEYIN> the type of the RecordReader key.
   * @param <VALUEIN> the type of the RecordReader value.
   * @param <KEYOUT> the type of the RecordWriter key.
   * @param <VALUEOUT> the type of the RecordWriter value.
   * @return a new Mapper.Context object.
   * @throws IOException in case of an IO error.
   * @throws InterruptedException in case of an interruption.
   */
  public abstract <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Mapper.Context getMapperContext(
      Configuration conf,
      TaskAttemptID taskId,
      RecordReader<KEYIN, VALUEIN> reader,
      RecordWriter<KEYOUT, VALUEOUT> writer,
      OutputCommitter committer,
      StatusReporter reporter,
      InputSplit split
  ) throws IOException, InterruptedException;

  private static KijiMRPlatformBridge mBridge;

  /**
   * @return the KijiMRPlatformBridge implementation appropriate to the current runtime
   * conditions.
   */
  public static final synchronized KijiMRPlatformBridge get() {
    if (null != mBridge) {
      return mBridge;
    }
    mBridge = Lookups.getPriority(KijiMRPlatformBridgeFactory.class).lookup().getBridge();
    return mBridge;
  }
}

