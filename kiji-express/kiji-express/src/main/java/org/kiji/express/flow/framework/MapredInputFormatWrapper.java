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

package org.kiji.express.flow.framework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.twitter.elephantbird.util.HadoopCompat;
import com.twitter.elephantbird.util.HadoopUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This file was adapted from: https://github.com/kevinweil/elephant-bird/blob/master/core/src/main/
 * java/com/twitter/elephantbird/mapred/input/DeprecatedInputFormatWrapper.java
 *
 * The wrapper enables an {@link InputFormat} written for new
 * <code>mapreduce</code> interface to be used unmodified in contexts where
 * a {@link org.apache.hadoop.mapred.InputFormat} with old <code>mapred</code>
 * interface is required. </p>
 *
 * Current restrictions on InputFormat: <ul>
 *    <li> the record reader should reuse key and value objects
 * </ul>
 *
 * While this restriction is satisfied by most input formats,
 * it could be removed with a configuration option.
 * <p>
 *
 * Usage: <pre>
 *    // set InputFormat class using a mapreduce InputFormat
 *    DeprecatedInputFormatWrapper.setInputFormat(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, jobConf);
 *    jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
 *    // ...
 * </pre>
 *
 * @author Raghu Angadi
 */
@SuppressWarnings("deprecation")
public class MapredInputFormatWrapper<K, V>
    implements org.apache.hadoop.mapred.InputFormat<Container<K>, Container<V>> {

  private static final String CLASS_CONF_KEY = "elephantbird.class.for.DeprecatedInputFormatWrapper";

  protected InputFormat<K, V> realInputFormat;

  /**
   * Sets jobs input format to {@link MapredInputFormatWrapper} and stores
   * supplied real {@link InputFormat} class name in job configuration.
   * This configuration is read on the remote tasks to instantiate actual
   * InputFormat correctly.
   */
  public static void setInputFormat(Class<?> realInputFormatClass, JobConf jobConf) {
    jobConf.setInputFormat(MapredInputFormatWrapper.class);
    HadoopUtils.setClassConf(jobConf, CLASS_CONF_KEY, realInputFormatClass);
  }

  @SuppressWarnings("unchecked")
  private void initInputFormat(JobConf conf) {
    if (realInputFormat == null) {
      realInputFormat = ReflectionUtils.newInstance(conf.getClass(CLASS_CONF_KEY, null, InputFormat.class), conf);
    }
  }

  public MapredInputFormatWrapper() {
    // real inputFormat is initialized based on conf.
  }

  public MapredInputFormatWrapper(InputFormat<K, V> realInputFormat) {
    this.realInputFormat = realInputFormat;
  }

  @Override
  public RecordReader<Container<K>, Container<V>> getRecordReader(
      InputSplit split, JobConf job,
      Reporter reporter
  ) throws IOException {
    initInputFormat(job);
    return new RecordReaderWrapper<K, V>(realInputFormat, split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    initInputFormat(job);

    try {
      List<org.apache.hadoop.mapreduce.InputSplit> splits =
          realInputFormat.getSplits(HadoopCompat.newJobContext(job, null));

      if (splits == null) {
        return null;
      }

      InputSplit[] resultSplits = new InputSplit[splits.size()];
      int i = 0;
      for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
        if (split.getClass() == org.apache.hadoop.mapreduce.lib.input.FileSplit.class) {
          org.apache.hadoop.mapreduce.lib.input.FileSplit mapreduceFileSplit =
              ((org.apache.hadoop.mapreduce.lib.input.FileSplit)split);
          resultSplits[i++] = new FileSplit(
              mapreduceFileSplit.getPath(),
              mapreduceFileSplit.getStart(),
              mapreduceFileSplit.getLength(),
              mapreduceFileSplit.getLocations());
        } else {
          resultSplits[i++] = new InputSplitWrapper(split);
        }
      }

      return resultSplits;

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * A reporter that works with both mapred and mapreduce APIs.
   */
  private static class ReporterWrapper extends StatusReporter implements Reporter {
    private Reporter wrappedReporter;

    public ReporterWrapper(Reporter reporter) {
      wrappedReporter = reporter;
    }

    @Override
    public Counters.Counter getCounter(Enum<?> anEnum) {
      return wrappedReporter.getCounter(anEnum);
    }

    @Override
    public Counters.Counter getCounter(String s, String s1) {
      return wrappedReporter.getCounter(s, s1);
    }

    @Override
    public void incrCounter(Enum<?> anEnum, long l) {
      wrappedReporter.incrCounter(anEnum, l);
    }

    @Override
    public void incrCounter(String s, String s1, long l) {
      wrappedReporter.incrCounter(s, s1, l);
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return wrappedReporter.getInputSplit();
    }

    @Override
    public void progress() {
      wrappedReporter.progress();
    }

//    @Override
    public float getProgress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setStatus(String s) {
      wrappedReporter.setStatus(s);
    }
  }

  private static class RecordReaderWrapper<K, V>
      implements RecordReader<Container<K>, Container<V>> {

    private org.apache.hadoop.mapreduce.RecordReader<K, V> realReader;
    private long splitLen; // for getPos()

    // expect readReader return same Key & Value objects (common case)
    // this avoids extra serialization & deserialazion of these objects
    private final Container<K> keyObj;
    private final Container<V> valueObj;

    public RecordReaderWrapper(
        InputFormat<K, V> newInputFormat,
        InputSplit oldSplit,
        JobConf oldJobConf,
        Reporter reporter
    ) throws IOException {
      keyObj = new Container<K>();
      valueObj = new Container<V>();
      splitLen = oldSplit.getLength();

      org.apache.hadoop.mapreduce.InputSplit split;
      if (oldSplit.getClass() == FileSplit.class) {
        split = new org.apache.hadoop.mapreduce.lib.input.FileSplit(
            ((FileSplit)oldSplit).getPath(),
            ((FileSplit)oldSplit).getStart(),
            ((FileSplit)oldSplit).getLength(),
            oldSplit.getLocations());
      } else {
        split = ((InputSplitWrapper)oldSplit).realSplit;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }

      // create a MapContext to pass reporter to record reader (for counters)
      TaskAttemptContext taskContext = HadoopCompat
          .newMapContext(oldJobConf, taskAttemptID, null, null, null,
              new ReporterWrapper(reporter), null);
      try {
        realReader = newInputFormat.createRecordReader(split, taskContext);
        realReader.initialize(split, taskContext);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public Container<K> createKey() {
      return keyObj;
    }

    @Override
    public Container<V> createValue() {
      return valueObj;
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(Container<K> key, Container<V> value) throws IOException {
      try {
        if (realReader.nextKeyValue()) {
          if (keyObj.getContents() != realReader.getCurrentKey()) {
            keyObj.setContents(realReader.getCurrentKey());
          }
          if (valueObj.getContents() != realReader.getCurrentValue()) {
            valueObj.setContents(realReader.getCurrentValue());
          }

          return true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      return false;
    }
  }

  private static class InputSplitWrapper implements InputSplit {

    org.apache.hadoop.mapreduce.InputSplit realSplit;


    @SuppressWarnings("unused") // MapReduce instantiates this.
    public InputSplitWrapper() {}

    public InputSplitWrapper(org.apache.hadoop.mapreduce.InputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
      try {
        return realSplit.getLength();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public String[] getLocations() throws IOException {
      try {
        return realSplit.getLocations();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      String className = WritableUtils.readString(in);
      Class<?> splitClass;

      try {
        splitClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      realSplit = (org.apache.hadoop.mapreduce.InputSplit)
          ReflectionUtils.newInstance(splitClass, null);
      ((Writable)realSplit).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, realSplit.getClass().getName());
      ((Writable)realSplit).write(out);
    }
  }
}

