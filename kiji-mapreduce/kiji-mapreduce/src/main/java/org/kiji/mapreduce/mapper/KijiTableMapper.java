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

package org.kiji.mapreduce.mapper;

import static org.kiji.schema.util.ByteArrayFormatter.toHex;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiDataRequester;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiRowData;

/**
 * Base class for {@link org.apache.hadoop.mapreduce.Mapper}s that read from Kiji tables.
 *
 * @param <K> Type of the MapReduce output key.
 * @param <V> Type of the MapReduce output value.
 */
@ApiAudience.Private
public abstract class KijiTableMapper<K, V>
    extends Mapper<EntityId, KijiRowData, K, V>
    implements Configurable, KijiMapper, KijiDataRequester {

  private static final Logger LOG = LoggerFactory.getLogger(KijiTableMapper.class);

  /** Configuration for this instance. */
  private Configuration mConf;

  /**
   * Kiji mapper function that processes an input row.
   *
   * @param input Input row from the configured Kiji table.
   * @param context Hadoop mapper context.
   * @throws IOException on I/O error.
   */
  protected abstract void map(KijiRowData input, Context context)
      throws IOException;

  /**
   * Creates the connection to the kiji instance. This serves as a dependency injection
   * point for unit testing with a mock kiji instance.
   *
   * @param kijiConf Kiji configuration.
   * @return a new kiji connection.
   * @throws IOException on I/O error.
   */
  protected Kiji createKiji(KijiConfiguration kijiConf) throws IOException {
    return Kiji.open(kijiConf);
  }

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
      LOG.info(String.format("Setting up map task on region [%s -- %s]%n",
          toHex(taskSplit.getStartRow()), toHex(taskSplit.getEndRow())));
    }
  }

  @Override
  protected void map(EntityId key, KijiRowData values, Context context)
      throws IOException, InterruptedException {
    map(values, context);
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (context.getInputSplit() instanceof TableSplit) {
      TableSplit taskSplit = (TableSplit) context.getInputSplit();
      LOG.info(String.format("Cleaning up task on region [%s -- %s]%n",
          toHex(taskSplit.getStartRow()), toHex(taskSplit.getEndRow())));
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
