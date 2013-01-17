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
import org.kiji.mapreduce.AvroKeyWriter;
import org.kiji.mapreduce.KijiBaseReducer;

/**
 * An abstract KijiReducer to encapsulate common functionality among
 * reducers that pass the key from the map phase through unchanged.
 *
 * @param <K> The input and output key type.
 * @param <INVALUE> The input value type.
 * @param <OUTVALUE> The output value type.
 */
@ApiAudience.Public
public abstract class KeyPassThroughReducer<K, INVALUE, OUTVALUE>
    extends KijiBaseReducer<K, INVALUE, K, OUTVALUE>
    implements Configurable, AvroKeyWriter {
  private static final Logger LOG = LoggerFactory.getLogger(KeyPassThroughReducer.class);

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
}
