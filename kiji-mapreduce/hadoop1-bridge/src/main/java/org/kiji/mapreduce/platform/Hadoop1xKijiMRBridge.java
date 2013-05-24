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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.Credentials;
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
  public SequenceFile.Reader newSeqFileReader(Configuration conf, Path filename)
      throws IOException {
    Preconditions.checkArgument(conf != null, "Configuration argument must be non-null");
    Preconditions.checkArgument(filename != null, "Filename argument must be non-null");

    final URI fileUri = filename.toUri();
    final FileSystem fs = FileSystem.get(fileUri, conf);
    return new SequenceFile.Reader(fs, filename, conf);
  }

  /** {@inheritDoc} */
  @Override
  public void setUserClassesTakesPrecedence(Job job, boolean value) {
    // Underlying configuration key is not respected in hadoop 1.x.
    LOG.warn("Cannot setUserClassesTakesPrecedence in Hadoop1.x.  User classes will be added to "
        + "the task classpath after Hadoop and Kiji dependencies.");
  }

  /** Mapper implementation for providing custom Mapper.WrappedContext implementations. */
  public static final class KijiWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
      extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    /**
     * Get a Mapper.WrappedContext configured with the given parameters.
     *
     * @param conf the Hadoop Configuration used to configure the WrappedContext.
     * @param taskid the TaskAttemptID for the WrappedContext.
     * @param reader the RecordReader for the WrappedContext.
     * @param writer the RecordWriter for the WrappedContext.
     * @param committer the OutputCommit for the WrappedContext.
     * @param reporter the StatusReporter for the WrappedContext.
     * @param split the InputSplit for the WrappedContext.
     * @return a new Mapper.WrappedContext object.
     * @throws IOException in case of an IO error.
     * @throws InterruptedException in case of an interruption.
     */
    public Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getMapContext(
        Configuration conf,
        TaskAttemptID taskid,
        RecordReader<KEYIN, VALUEIN> reader,
        RecordWriter<KEYOUT, VALUEOUT> writer,
        OutputCommitter committer,
        StatusReporter reporter,
        InputSplit split)
        throws IOException, InterruptedException {
      return new WrappedContext(
          conf, taskid, reader, writer, committer, reporter, split);
    }

    /** Mapper.WrappedContext implementation which delegates to a MapContext. */
    public final class WrappedContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

      /** The MapContext to which to delegate method calls. */
      private final MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mMapContext;

      /**
       * Construct a new WrappedContext.
       *
       * @param conf the Hadoop Configuration used to configure the WrappedContext.
       * @param taskid the TaskAttemptID for the WrappedContext.
       * @param reader the RecordReader for the WrappedContext.
       * @param writer the RecordWriter for the WrappedContext.
       * @param committer the OutputCommit for the WrappedContext.
       * @param reporter the StatusReporter for the WrappedContext.
       * @param split the InputSplit for the WrappedContext.
       * @throws IOException in case of an IO error.
       * @throws InterruptedException in case of an interruption.
       */
      public WrappedContext(
          final Configuration conf,
          final TaskAttemptID taskid,
          final RecordReader<KEYIN, VALUEIN> reader,
          final RecordWriter<KEYOUT, VALUEOUT> writer,
          final OutputCommitter committer,
          final StatusReporter reporter,
          final InputSplit split)
          throws IOException, InterruptedException {
        super(conf, taskid, reader, writer, committer, reporter, split);
        mMapContext = new MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
            conf, taskid, reader, writer, committer, reporter, split);
      }

      /** {@inheritDoc} */
      @Override
      public InputSplit getInputSplit() {
        return mMapContext.getInputSplit();
      }

      /** {@inheritDoc} */
      @Override
      public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return mMapContext.getCurrentKey();
      }

      /** {@inheritDoc} */
      @Override
      public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return mMapContext.getCurrentValue();
      }

      /** {@inheritDoc} */
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return mMapContext.nextKeyValue();
      }

      /** {@inheritDoc} */
      @Override
      public Counter getCounter(Enum<?> counterName) {
        return mMapContext.getCounter(counterName);
      }

      /** {@inheritDoc} */
      @Override
      public Counter getCounter(String groupName, String counterName) {
        return mMapContext.getCounter(groupName, counterName);
      }

      /** {@inheritDoc} */
      @Override
      public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
        mMapContext.write(key, value);
      }

      /** {@inheritDoc} */
      @Override
      public String getStatus() {
        return mMapContext.getStatus();
      }

      /** {@inheritDoc} */
      @Override
      public TaskAttemptID getTaskAttemptID() {
        return mMapContext.getTaskAttemptID();
      }

      /** {@inheritDoc} */
      @Override
      public void setStatus(String msg) {
        mMapContext.setStatus(msg);
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
          throws ClassNotFoundException {
        return mMapContext.getCombinerClass();
      }

      /** {@inheritDoc} */
      @Override
      public Configuration getConfiguration() {
        return mMapContext.getConfiguration();
      }

      /** {@inheritDoc} */
      @Override
      public RawComparator<?> getGroupingComparator() {
        return mMapContext.getGroupingComparator();
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends InputFormat<?, ?>> getInputFormatClass()
          throws ClassNotFoundException {
        return mMapContext.getInputFormatClass();
      }

      /** {@inheritDoc} */
      @Override
      public String getJar() {
        return mMapContext.getJar();
      }

      /** {@inheritDoc} */
      @Override
      public JobID getJobID() {
        return mMapContext.getJobID();
      }

      /** {@inheritDoc} */
      @Override
      public String getJobName() {
        return mMapContext.getJobName();
      }

      /** {@inheritDoc} */
      @Override
      public Class<?> getMapOutputKeyClass() {
        return mMapContext.getMapOutputKeyClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<?> getMapOutputValueClass() {
        return mMapContext.getMapOutputValueClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
          throws ClassNotFoundException {
        return mMapContext.getMapperClass();
      }

      /** {@inheritDoc} */
      @Override
      public int getNumReduceTasks() {
        return mMapContext.getNumReduceTasks();
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
          throws ClassNotFoundException {
        return mMapContext.getOutputFormatClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<?> getOutputKeyClass() {
        return mMapContext.getOutputKeyClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<?> getOutputValueClass() {
        return mMapContext.getOutputValueClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends Partitioner<?, ?>> getPartitionerClass()
          throws ClassNotFoundException {
        return mMapContext.getPartitionerClass();
      }

      /** {@inheritDoc} */
      @Override
      public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
          throws ClassNotFoundException {
        return mMapContext.getReducerClass();
      }

      /** {@inheritDoc} */
      @Override
      public RawComparator<?> getSortComparator() {
        return mMapContext.getSortComparator();
      }

      /** {@inheritDoc} */
      @Override
      public Path getWorkingDirectory() throws IOException {
        return mMapContext.getWorkingDirectory();
      }

      /** {@inheritDoc} */
      @Override
      public void progress() {
        mMapContext.progress();
      }

      /** {@inheritDoc} */
      @Override
      public Credentials getCredentials() {
        return mMapContext.getCredentials();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public <KEYIN, VALUEIN, KEYOUT, VALUEOUT> Context getMapperContext(
      final Configuration conf,
      final TaskAttemptID taskId,
      final RecordReader<KEYIN, VALUEIN> reader,
      final RecordWriter<KEYOUT, VALUEOUT> writer,
      final OutputCommitter committer,
      final StatusReporter reporter,
      final InputSplit split
  ) throws IOException, InterruptedException {
    return new KijiWrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>().getMapContext(
        conf, taskId, reader, writer, committer, reporter, split);
  }

}

