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

package org.kiji.mapreduce.testlib;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;

import org.kiji.mapreduce.KijiReducer;

/**
 * <p>A KijiReducer that works on key/value pairs where the value is
 * an IntWritable.  For all integer values with the same key, the
 * SimpleIntSumReducer will output a single pair with a value equal to the
 * sum, leaving the key unchanged.</p>
 *
 * This class is for testing in KijiMR.  To use this in actual MapReduce jobs, use the
 * IntSumReducer in the KijiMR Library.
 *
 * @param <K> The type of the reduce input key.
 */
public class SimpleIntSumReducer<K> extends KijiReducer<K, IntWritable, K, IntWritable>
    implements Configurable {
  private IntWritable mValue;

  /** The Hadoop configuration. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return new JobConf(getConf()).getMapOutputKeyClass();
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mValue = new IntWritable();
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    mValue.set(sum);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}
