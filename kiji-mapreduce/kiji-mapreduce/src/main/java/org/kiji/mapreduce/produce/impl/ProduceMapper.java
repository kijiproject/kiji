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

package org.kiji.mapreduce.produce.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.framework.HFileKeyValue;
import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.impl.KijiTableMapper;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Hadoop mapper that runs a Kiji producer.
 */
@ApiAudience.Private
public final class ProduceMapper extends KijiTableMapper<HFileKeyValue, NullWritable> {
  /** Actual producer implementation. */
  private KijiProducer mProducer;

  /** Configured output column: either a map-type family or a single column. */
  private KijiColumnName mOutputColumn;

  /** Producer context. */
  private InternalProducerContextInterface mProducerContext;

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
    mProducer = KijiProducers.create(conf);

    final String column = Preconditions.checkNotNull(mProducer.getOutputColumn());
    mOutputColumn = new KijiColumnName(column);

    mProducerContext = InternalProducerContext.create(context, mOutputColumn);
    mProducer.setup(mProducerContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(KijiRowData input, Context mapContext) throws IOException {
    mProducerContext.setEntityId(input.getEntityId());
    mProducer.produce(input, mProducerContext);
    mapContext.getCounter(JobHistoryCounters.PRODUCER_ROWS_PROCESSED).increment(1);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkState(mProducerContext != null);
    mProducer.cleanup(mProducerContext);
    mProducerContext.close();
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
}
