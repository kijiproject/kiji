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

package org.kiji.mapreduce.pivot.impl;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.HFileKeyValue;
import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.impl.KijiTableContextFactory;
import org.kiji.mapreduce.impl.KijiTableMapper;
import org.kiji.mapreduce.pivot.KijiPivoter;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Hadoop mapper that executes a {@link KijiPivoter} over the rows from a Kiji table.
 */
@ApiAudience.Private
public final class PivoterMapper
    extends KijiTableMapper<HFileKeyValue, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(PivoterMapper.class);

  /** Pivoter that processes input rows. */
  private KijiPivoter mPivoter;

  /** Context allowing interactions with MapReduce, KVStores, etc. */
  private KijiTableContext mContext;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * Returns the specification of the data requested by the pivoter to run over the table.
   * @return the specification of the data requested by the pivoter to run over the table.
   */
  public KijiDataRequest getDataRequest() {
    return mPivoter.getDataRequest();
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

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    super.setup(context);

    Preconditions.checkState(null == mContext);
    setConf(context.getConfiguration());

    mContext = KijiTableContextFactory.create(context);
    mPivoter = KijiPivoters.create(getConf());
    mPivoter.setup(mContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(KijiRowData input, Context context)
      throws IOException {
    Preconditions.checkNotNull(mContext);
    mPivoter.produce(input, mContext);
    mContext.incrementCounter(JobHistoryCounters.PIVOTER_ROWS_PROCESSED);
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mContext);
    mPivoter.cleanup(mContext);
    mContext.close();
    mContext = null;

    super.cleanup(context);
  }

}
