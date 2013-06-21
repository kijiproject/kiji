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

package org.kiji.mapreduce.reducer;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;

/**
 * This MapReduce reducer will pass through all of the input key-value
 * pairs unchanged.  This differs from the basic Hadoop MapReduce
 * IdentityReducer only in that it extends KijiReducer so it can be
 * run within the Kiji framework.
 *
 * This class implements the {@link AvroKeyWriter} and the {@link AvroValueWriter} interfaces
 * so that it can be used for Avro output
 *
 * @param <K> The MapReduce input key type.
 * @param <V> The MapReduce input value type.
 */
@ApiAudience.Public
@ApiStability.Stable
public final class IdentityReducer<K, V>
    extends KijiReducer<K, V, K, V>
    implements Configurable, AvroKeyWriter, AvroValueWriter {
  private static final Logger LOG = LoggerFactory.getLogger(IdentityReducer.class);

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
  protected void reduce(K key, Iterable<V> values, Context context)
      throws IOException, InterruptedException {
    for (V value : values) {
      context.write(key, value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return new JobConf(getConf()).getMapOutputKeyClass();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return new JobConf(getConf()).getMapOutputValueClass();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    Class<? extends Mapper<?, ?, ?, ?>> mapperClass;
    try {
      mapperClass = new Job(getConf()).getMapperClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Mapper class was not configured. "
          + "Could not infer avro key writer schema.", e);
    }
    Mapper<?, ?, ?, ?> mapper = ReflectionUtils.newInstance(mapperClass, getConf());
    if (mapper instanceof AvroKeyWriter) {
      LOG.info("Mapper is an AvroKeyWriter. Using the same schema for Reducer output keys.");
      return ((AvroKeyWriter) mapper).getAvroKeyWriterSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    Class<? extends Mapper<?, ?, ?, ?>> mapperClass;
    try {
      mapperClass = new Job(getConf()).getMapperClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Mapper class was not configured. "
          + "Could not infer avro value writer schema.", e);
    }
    Mapper<?, ?, ?, ?> mapper = ReflectionUtils.newInstance(mapperClass, getConf());
    if (mapper instanceof AvroValueWriter) {
      LOG.info("Mapper is an AvroValueWriter. Using the same schema for Reducer output values.");
      return ((AvroValueWriter) mapper).getAvroValueWriterSchema();
    }
    return null;
  }
}
