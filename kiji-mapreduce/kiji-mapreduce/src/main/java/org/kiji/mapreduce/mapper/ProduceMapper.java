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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.InternalProducerContextInterface;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.JobHistoryCounters;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiProducer;
import org.kiji.mapreduce.context.InternalProducerContext;
import org.kiji.mapreduce.util.KijiProducers;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiPutter;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

/**
 * A Mapper that delegates work to a {@link org.kiji.mapreduce.KijiProducer}.
 *
 * @param <K> The type of the MapReduce output key.
 * @param <V> The type of the MapReduce output value.
 */
public class ProduceMapper<K, V> extends KijiTableMapper<K, V> {
  /** Actual producer implementation. */
  private KijiProducer mProducer;

  /** Configured output column: either a map-type family or a single column. */
  private KijiColumnName mOutputColumn;

  /** Producer context. */
  private InternalProducerContextInterface mProducerContext;

  /** Kiji instance the producer runs on. */
  private Kiji mKiji;

  /** Kiji table the producer runs on. */
  private KijiTable mTable;

  /** Interface to write to the Kiji table. */
  private KijiPutter mPutter;

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    final KijiDataRequest dataRequest = mProducer.getDataRequest();
    if (dataRequest.isEmpty()) {
      throw new JobConfigurationException(mProducer.getClass().getName()
          + " returned an empty KijiDataRequest, which is not allowed.");
    }
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);
    Preconditions.checkState(mProducerContext == null);
    final Configuration conf = context.getConfiguration();
    mProducer = createProducer(conf);

    final String column = Preconditions.checkNotNull(mProducer.getOutputColumn());
    mOutputColumn = new KijiColumnName(column);

    final KijiURI outputURI = getOutputURI(conf);
    mKiji = Kiji.open(outputURI, conf);
    mTable = mKiji.openTable(outputURI.getTable());
    mPutter = mTable.openTableWriter();

    mProducerContext = new InternalProducerContext(context, mPutter, mOutputColumn);
    mProducer.setup(mProducerContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(KijiRowData input, Context mapContext)
      throws IOException {
    mProducerContext.setEntityId(input.getEntityId());
    mProducer.produce(input, mProducerContext);
    mapContext.getCounter(JobHistoryCounters.PRODUCER_ROWS_PROCESSED).increment(1);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mProducerContext);
    mTable.close();
    mKiji.close();
    mProducer.cleanup(mProducerContext);
    mPutter = null;
    mTable = null;
    mKiji = null;
    mProducerContext = null;
    super.cleanup(context);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }

  /**
   * Reports the URI of the configured output table.
   *
   * @param conf Read the output URI from this configuration.
   * @return the configured output URI.
   * @throws IOException on I/O error.
   */
  private static KijiURI getOutputURI(Configuration conf) throws IOException {
    try {
      return KijiURI.parse(conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI));
    } catch (KijiURIException kue) {
      throw new IOException(kue);
    }
  }

  /**
   * Creates a new KijiProducer instance based on a configuration.
   *
   * @param conf Configuration specifying the producer to create.
   * @return A new instance of the producer specified in the configuration.
   * @throws IOException If the KijiProducer cannot be instantiated.
   */
  private static KijiProducer createProducer(Configuration conf) throws IOException {
    return KijiProducers.create(conf);
  }
}
