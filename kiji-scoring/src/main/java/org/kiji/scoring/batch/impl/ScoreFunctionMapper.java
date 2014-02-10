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
package org.kiji.scoring.batch.impl;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.framework.HFileKeyValue;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.impl.KijiTableContextFactory;
import org.kiji.mapreduce.impl.KijiTableMapper;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.ScoreFunction.TimestampedValue;
import org.kiji.scoring.batch.ScoreFunctionJobBuilder;
import org.kiji.scoring.impl.InternalFreshenerContext;

/** Hadoop mapper that runs a KijiScoring ScoreFunction. */
@ApiAudience.Private
public final class ScoreFunctionMapper extends KijiTableMapper<HFileKeyValue, NullWritable> {

  private static final Gson GSON = new Gson();

  private ScoreFunction mScoreFunction = null;
  private KijiColumnName mAttachedColumn = null;
  private Map<String, String> mParameters = null;
  private KijiDataRequest mClientDataRequest = null;
  private InternalFreshenerContext mFreshenerContext = null;
  private KijiTableContext mTableContext = null;

  /**
   * Extract and deserialize the client data request from the given Configuration.
   *
   * @param conf Hadoop Configuration from which to extract the client data request.
   * @return the client data request serialized in the given Configuration.
   */
  private static KijiDataRequest getClientDataRequestFromConf(
      final Configuration conf
  ) {
    final String base64DataRequest = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
    Preconditions.checkNotNull(base64DataRequest,
        "ClientDataRequest could not be found in configuration.");
    final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(base64DataRequest));
    return (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  protected void setup(
      final Context context
  ) throws IOException {
    super.setup(context);
    Preconditions.checkState(null == mFreshenerContext);
    final Configuration conf = context.getConfiguration();
    final Class<? extends ScoreFunction> scoreFunctionClass = conf.getClass(
        ScoreFunctionJobBuilder.SCORE_FUNCTION_CLASS_CONF_KEY,
        null,
        ScoreFunction.class);
    if (null == scoreFunctionClass) {
      throw new IOException("ScoreFunction class could not be found in configuration.");
    }
    mScoreFunction = ReflectionUtils.newInstance(scoreFunctionClass, conf);
    mAttachedColumn = new KijiColumnName(
        conf.get(ScoreFunctionJobBuilder.SCORE_FUNCTION_ATTACHED_COLUMN_CONF_KEY));
    mParameters = GSON.fromJson(
        conf.get(ScoreFunctionJobBuilder.SCORE_FUNCTION_PARAMETERS_CONF_KEY),
        Map.class);
    final KeyValueStoreReaderFactory factory = KeyValueStoreReaderFactory.create(conf);
    mClientDataRequest = getClientDataRequestFromConf(conf);
    mFreshenerContext = InternalFreshenerContext.create(
        mClientDataRequest,
        mAttachedColumn,
        mParameters,
        Maps.<String, String>newHashMap(),
        factory);
    mTableContext = KijiTableContextFactory.create(context);
    mScoreFunction.setup(mFreshenerContext);
  }

  /** {@inheritDoc} */
  @Override
  protected void map(
      final KijiRowData input,
      final Context context
  ) throws IOException {
    final TimestampedValue score = mScoreFunction.score(input, mFreshenerContext);
    mTableContext.put(
        input.getEntityId(),
        mAttachedColumn.getFamily(),
        mAttachedColumn.getQualifier(),
        score.getTimestamp(),
        score.getValue());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(
      final Context context
  ) throws IOException {
    Preconditions.checkState(null != mFreshenerContext);
    mScoreFunction.cleanup(mFreshenerContext);
    mTableContext.flush();
    mTableContext.close();
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
