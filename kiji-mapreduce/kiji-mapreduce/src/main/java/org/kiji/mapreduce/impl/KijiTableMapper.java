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

package org.kiji.mapreduce.impl;

import static org.kiji.schema.util.ByteArrayFormatter.toHex;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiRowData;

/**
 * Base class for mappers reading from a Kiji table.
 *
 * @param <K> Type of the MapReduce output key.
 * @param <V> Type of the MapReduce output value.
 */
@ApiAudience.Private
@Inheritance.Sealed
public abstract class KijiTableMapper<K, V>
    extends KijiMapper<EntityId, KijiRowData, K, V> implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(KijiTableMapper.class);

  /** Configuration for this instance. */
  private Configuration mConf;

  /** Constructs a new mapper that reads from a Kiji table. */
  protected KijiTableMapper() {
  }

  /**
   * Kiji mapper function that processes an input row.
   *
   * @param input Input row from the configured Kiji table.
   * @param context Hadoop mapper context.
   * @throws IOException on I/O error.
   */
  protected abstract void map(KijiRowData input, Context context)
      throws IOException;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    try {
      super.setup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    if (context.getInputSplit() instanceof TableSplit) {
      TableSplit taskSplit = (TableSplit) context.getInputSplit();
      LOG.info("Setting up map task on region [{} -- {}]",
          toHex(taskSplit.getStartRow()), toHex(taskSplit.getEndRow()));
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void map(EntityId key, KijiRowData values, Context context) throws IOException {
    map(values, context);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    if (context.getInputSplit() instanceof TableSplit) {
      TableSplit taskSplit = (TableSplit) context.getInputSplit();
      LOG.info("Cleaning up task on region [{} -- {}]",
          toHex(taskSplit.getStartRow()), toHex(taskSplit.getEndRow()));
    }
    try {
      super.cleanup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

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
}
