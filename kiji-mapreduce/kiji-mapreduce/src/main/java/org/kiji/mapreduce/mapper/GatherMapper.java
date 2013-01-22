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

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.AvroKeyWriter;
import org.kiji.mapreduce.AvroValueWriter;
import org.kiji.mapreduce.JobHistoryCounters;
import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.mapreduce.context.InternalMapReduceContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Mapper that executes a gatherer over the rows of a Kiji table.
 *
 * @param <K> The type of the MapReduce output key.
 * @param <V> The type of the MapReduce output value.
 */
@ApiAudience.Private
public final class GatherMapper<K, V>
    extends KijiTableMapper<K, V>
    implements AvroKeyWriter, AvroValueWriter {

  private static final Logger LOG = LoggerFactory.getLogger(GatherMapper.class);

  /** The gatherer to execute. */
  private KijiGatherer<K, V> mGatherer;

  /**
   * The context object that allows the gatherer to interact with MapReduce,
   * KVStores, etc.
   */
  private MapReduceContext<K, V> mGathererContext;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    setGatherer(createGatherer(conf));
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    return mGatherer.getDataRequest();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return mGatherer.getOutputKeyClass();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return mGatherer.getOutputValueClass();
  }

  /**
   * Initialize the gatherer instance to execute.
   *
   * @param conf the Configuration to use to initialize the gatherer instance.
   * @return a KijiGatherer instance.
   */
  protected KijiGatherer<K, V> createGatherer(Configuration conf) {
    @SuppressWarnings("unchecked")
    Class<? extends KijiGatherer<K, V>> gatherClass =
        (Class<? extends KijiGatherer<K, V>>)
        conf.getClass(KijiGatherer.CONF_GATHERER_CLASS, null, KijiGatherer.class);
    if (null == gatherClass) {
      LOG.error("Null " + KijiGatherer.CONF_GATHERER_CLASS + " in createGatherer()?");
      return null;
    }
    return ReflectionUtils.newInstance(gatherClass, conf);
  }

  /**
   * Set the gatherer instance to use for the scan.
   *
   * @param gatherer the gatherer instance to use.
   */
  private void setGatherer(KijiGatherer<K, V> gatherer) {
    mGatherer = gatherer;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);

    Preconditions.checkState(null == mGathererContext);
    setConf(context.getConfiguration());

    mGathererContext = InternalMapReduceContext.create(context);
    mGatherer.setup(mGathererContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(KijiRowData input, Context context)
      throws IOException {
    Preconditions.checkNotNull(mGathererContext);
    mGatherer.gather(input, mGathererContext);
    mGathererContext.incrementCounter(JobHistoryCounters.GATHERER_ROWS_PROCESSED);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mGathererContext);
    mGatherer.cleanup(mGathererContext);
    mGathererContext = null;

    super.cleanup(context);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    if (mGatherer instanceof AvroKeyWriter) {
      LOG.debug("Gatherer " + mGatherer.getClass().getName()
          + " implements AvroKeyWriter, querying for writer schema.");
      return ((AvroKeyWriter) mGatherer).getAvroKeyWriterSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    if (mGatherer instanceof AvroValueWriter) {
      LOG.debug("Gatherer " + mGatherer.getClass().getName()
          + " implements AvroValueWriter, querying for writer schema.");
      return ((AvroValueWriter) mGatherer).getAvroValueWriterSchema();
    }
    return null;
  }
}
