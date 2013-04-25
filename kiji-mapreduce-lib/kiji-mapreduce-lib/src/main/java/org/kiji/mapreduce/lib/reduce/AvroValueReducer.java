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

import org.apache.avro.mapred.AvroValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.lib.avro.UnwrappedAvroValueIterable;

/**
 * Base class for reducers that use an AvroValue for the input and output value.
 *
 * @param <K> The MapReduce key (same is used for input/output).
 * @param <T> The Java type for the Avro data used in the output MapReduce value.
 */
public abstract class AvroValueReducer<K, T>
    extends KeyPassThroughReducer<K, AvroValue<T>, AvroValue<T>> implements AvroValueWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AvroValueReducer.class);

  /** A shared AvroValue wrapper that is reused when writing reduce output values. */
  private AvroValue<T> mValue;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mValue = new AvroValue<T>(null);
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<AvroValue<T>> values, Context context)
      throws IOException, InterruptedException {
    reduceAvro(key, new UnwrappedAvroValueIterable<T>(values), context);
  }

  /**
   * An alternative reduce method for subclasses for direct access to
   * iterate over the input values as Avro data instead of having to
   * unwrap the AvroValue container objects.
   *
   * @param key The reduce input key.
   * @param values The reduce input values as avro messages.
   * @param context The reduce context.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  protected abstract void reduceAvro(K key, Iterable<T> values, Context context)
      throws IOException, InterruptedException;

  /**
   * Subclasses can use this instead of context.write() to output Avro
   * messages directly instead of having to wrap them in AvroValue container objects.
   *
   * @param key The output key to write.
   * @param value The Avro value to write.
   * @param context The reducer context.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  protected void write(K key, T value, Context context)
      throws IOException, InterruptedException {
    mValue.datum(value);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }
}
