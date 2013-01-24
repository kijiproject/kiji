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

package org.kiji.mapreduce.lib.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.kiji.mapreduce.reducer.KeyPassThroughReducer;

/**
 * <p>A WibiReducer that works of key/value pairs where the value is
 * an DoubleWritable.  For all integer values with the same key, the
 * DoubleSumReducer will output a single pair with a value equal to the
 * sum, leaving the key unchanged.</p>
 *
 * @param <K> The type of the reduce input key.
 */
public class DoubleSumReducer<K> extends KeyPassThroughReducer<K, DoubleWritable, DoubleWritable> {
  private DoubleWritable mValue;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mValue = new DoubleWritable();
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<DoubleWritable> values,
      Context context) throws IOException, InterruptedException {
    double sum = 0.0;
    for (DoubleWritable value : values) {
      sum += value.get();
    }
    mValue.set(sum);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return DoubleWritable.class;
  }
}
