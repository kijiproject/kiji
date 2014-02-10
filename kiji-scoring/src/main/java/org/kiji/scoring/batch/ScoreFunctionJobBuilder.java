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
package org.kiji.scoring.batch;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.KijiMultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.framework.KijiTableInputJobBuilder;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.batch.impl.ScoreFunctionMapper;
import org.kiji.scoring.impl.InternalFreshenerContext;

/**
 * Builder for KijiMapReduceJobs which run ScoreFunction implementations across all rows of a table.
 *
 * A ScoreFunction MapReduce job runs a ScoreFunction against all rows within the specified range
 * of a table. It runs the ScoreFunction as if it was attached with an
 * {@link org.kiji.scoring.lib.AlwaysFreshen} policy which provides no additional parameters or
 * KeyValueStores.
 *
 * ScoreFunction MapReduce jobs require that all information available to a ScoreFunction via the
 * FreshenerContext be specified during construction of the job. This information includes:
 * <ul>
 *   <li>attached column (normally this would be the column where the Freshener is attached)</li>
 *   <li>string-string parameter mapping (defaults to an empty map)</li>
 *   <li>
 *     client data request (normally this would be the request which triggered the run of the
 *     Freshener) (defaults to an empty data request)
 *   </li>
 *   <li>
 *     KeyValueStores will be constructed from the return value of the ScoreFunction's
 *     getRequiredStores method optionally overridden by KeyValueStores specified to
 *     {@link #withKeyValueStoreOverrides(java.util.Map)}. This optional overriding makes up for the
 *     lack of overrides normally provided by the KijiFreshnessPolicy. (defaults to an empty map)
 *   </li>
 * </ul>
 *
 * <p>
 *   Example usage:
 *   <pre>
 *     final KijiMapReduceJob sfJob = ScoreFunctionJobBuilder.create()
 *         .withConf(conf)
 *         .withInputTable(inputTableURI)
 *         .withAttachedColumn(new KijiColumnName("family:qualifier"))
 *         .withScoreFunctionClass(MyScoreFunction.class)
 *         .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(inputTableURI))
 *         .build();
 *     sfJob.run();
 *   </pre>
 * </p>
 */
public final class ScoreFunctionJobBuilder
    extends KijiTableInputJobBuilder<ScoreFunctionJobBuilder> {

  public static final String SCORE_FUNCTION_CLASS_CONF_KEY =
      "org.kiji.scoring.batch.ScoreFunctionJobBuilder.sf_class_conf_key";
  public static final String SCORE_FUNCTION_PARAMETERS_CONF_KEY =
      "org.kiji.scoring.batch.ScoreFunctionJobBuilder.sf_parameters_conf_key";
  public static final String SCORE_FUNCTION_ATTACHED_COLUMN_CONF_KEY =
      "org.kiji.scoring.batch.ScoreFunctionJobBuilder.sf_attached_column_conf_key";
  public static final String SCORE_FUNCTION_CLIENT_DATA_REQUEST_CONF_KEY =
      "org.kiji.scoring.batch.ScoreFunctionJobBuilder.sf_client_data_request_conf_key";

  private static final Gson GSON = new Gson();
  private static final KijiDataRequest DEFAULT_CLIENT_REQUEST = KijiDataRequest.builder().build();
  private static final Map<String, String> DEFAULT_PARAMETERS = Maps.newHashMap();
  private static final int DEFAULT_NUM_THREADS_PER_MAPPER = 1;

  private Class<? extends ScoreFunction> mScoreFunctionClass = null;
  private KijiTableMapReduceJobOutput mJobOutput = null;
  private ScoreFunction<?> mScoreFunction = null;
  private KijiMapper<?, ?, ?, ?> mMapper = null;
  private KijiReducer<?, ?, ?, ?> mReducer = null;
  private KijiDataRequest mScoreFunctionDataRequest = null;
  private int mNumThreadsPerMapper = DEFAULT_NUM_THREADS_PER_MAPPER;
  private KijiDataRequest mClientDataRequest = null;
  private KijiColumnName mAttachedColumn = null;
  private Map<String, String> mParameters = null;
  private Map<String, KeyValueStore<?, ?>> mKeyValueStoreOverrides = null;

  /** Private constructor. Use {@link #create()}. */
  private ScoreFunctionJobBuilder() { }

  /**
   * Create a new ScoreFunctionJobBuilder.
   *
   * @return a new ScoreFunctionJobBuilder.
   */
  public static ScoreFunctionJobBuilder create() {
    return new ScoreFunctionJobBuilder();
  }

  /**
   * Configure the Job to run the given ScoreFunction implementation to generate scores.
   *
   * @param scoreFunctionClass class of the ScoreFunction implementation with which to generate
   *     scores.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withScoreFunctionClass(
      final Class<? extends ScoreFunction> scoreFunctionClass
  ) {
    mScoreFunctionClass = scoreFunctionClass;
    return this;
  }

  /**
   * Configure the Job to output using the given KijiTableMapReduceJobOutput. The output table must
   * match the input table.
   *
   * @param jobOutput KijiTableMapReduceJobOutput which defines the output from this mapreduce job.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withOutput(
      final KijiTableMapReduceJobOutput jobOutput
  ) {
    mJobOutput = jobOutput;
    return super.withOutput(jobOutput);
  }

  /** {@inheritDoc} */
  @Override
  public ScoreFunctionJobBuilder withOutput(
      final MapReduceJobOutput jobOutput
  ) {
    if (jobOutput instanceof KijiTableMapReduceJobOutput) {
      return withOutput((KijiTableMapReduceJobOutput) jobOutput);
    } else {
      throw new RuntimeException("jobOutput parameter of ScoreFunctionJobBuilder.withOutput() must "
          + "be a KijiTableMapReduceJobOutput.");
    }
  }

  /**
   * Sets the number of threads to use for running the ScoreFunction in parallel.
   *
   * <p>
   *   You may use this setting to run multiple instances of your ScoreFunction in parallel within
   *   each map task of the job. This may be useful for increasing your throughput when your
   *   ScoreFunction is not CPU bound.
   * </p>
   *
   * @param numThreads the number of ScoreFunctions which will be run in parallel per mapper.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withNumThreadsPerMapper(
      final int numThreads
  ) {
    Preconditions.checkArgument(0 < numThreads, "numThreads must be positive, got %d", numThreads);
    mNumThreadsPerMapper = numThreads;
    return this;
  }

  /**
   * Configure the Job to include the given client data request. This request will be visible to the
   * ScoreFunction via {@link org.kiji.scoring.FreshenerContext#getClientRequest()}. If unspecified,
   * an empty data request will be used.
   *
   * @param clientDataRequest KijiDataRequest which will be visible to the ScoreFunction.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withClientDataRequest(
      final KijiDataRequest clientDataRequest
  ) {
    mClientDataRequest = clientDataRequest;
    return this;
  }

  /**
   * Configure the Job to include the given attached column. This column will be visible to the
   * ScoreFunction via {@link org.kiji.scoring.FreshenerContext#getAttachedColumn()} and will be
   * used as the output column for values written by the ScoreFunction. The schema of this column
   * should be compatible with the schema of values output by the ScoreFunction.
   *
   * @param attachedColumn column to which to write ScoreFunction return values.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withAttachedColumn(
      final KijiColumnName attachedColumn
  ) {
    mAttachedColumn = attachedColumn;
    return this;
  }

  /**
   * Configure the Job to include the given parameters. These parameters should be the equivalent of
   * merging request and attachment time parameters from the real time execution of a Freshener.
   *
   * @param parameters parameters which will be available to the ScoreFunction via the
   *     FreshenerContext.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withParameters(
      final Map<String, String> parameters
  ) {
    mParameters = parameters;
    return this;
  }

  /**
   * Configures the Job to use the given KeyValueStores in preference to those requested by the
   * ScoreFunction when there are name conflicts. This mirrors the KeyValueStore override behavior
   * provided by a KijiFreshnessPolicy. These KeyValueStores will only replace KeyValueStores
   * requested by the ScoreFunction with the same name. KeyValueStores requested by the
   * ScoreFunction whose names are not shadowed in this map will be available as normal.
   *
   * @param kvStoreOverrides KeyValueStores which will take precedence over stores requested by the
   *     ScoreFunction.
   * @return this builder.
   */
  public ScoreFunctionJobBuilder withKeyValueStoreOverrides(
      final Map<String, KeyValueStore<?, ?>> kvStoreOverrides
  ) {
    mKeyValueStoreOverrides = kvStoreOverrides;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(
      final Job job
  ) throws IOException {
    if (null == mScoreFunctionClass) {
      throw new JobConfigurationException("Must specify a ScoreFunction class.");
    }
    if (null == mClientDataRequest) {
      mClientDataRequest = DEFAULT_CLIENT_REQUEST;
    }
    if (null == mAttachedColumn) {
      throw new JobConfigurationException("Must specified an AttachedColumn.");
    }
    if (null == mParameters) {
      mParameters = DEFAULT_PARAMETERS;
    }

    final Configuration conf = job.getConfiguration();
    conf.setClass(SCORE_FUNCTION_CLASS_CONF_KEY, mScoreFunctionClass, ScoreFunction.class);
    if (!getInputTableURI().equals(mJobOutput.getOutputTableURI())) {
      throw new JobConfigurationException(String.format("Output table must be the same as the input"
          + "table. Got input: %s output: %s", getInputTableURI(), mJobOutput.getOutputTableURI()));
    }
    conf.set(SCORE_FUNCTION_ATTACHED_COLUMN_CONF_KEY, mAttachedColumn.getName());
    conf.set(SCORE_FUNCTION_PARAMETERS_CONF_KEY, GSON.toJson(mParameters, Map.class));
    conf.set(SCORE_FUNCTION_CLIENT_DATA_REQUEST_CONF_KEY,
        Base64.encodeBase64String(SerializationUtils.serialize(mClientDataRequest)));
    mMapper = new ScoreFunctionMapper();
    mReducer = new IdentityReducer<Object, Object>();
    job.setJobName("Kiji ScoreFunction: " + mScoreFunctionClass.getSimpleName());
    mScoreFunction = ReflectionUtils.newInstance(mScoreFunctionClass, conf);
    final FreshenerContext context = InternalFreshenerContext.create(
        mClientDataRequest,
        mAttachedColumn,
        mParameters,
        Maps.<String, String>newHashMap(),
        KeyValueStoreReaderFactory.create(getRequiredStores()));
    mScoreFunctionDataRequest = mScoreFunction.getDataRequest(context);

    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected void configureMapper(
      final Job job
  ) throws IOException {
    super.configureMapper(job);

    if (mNumThreadsPerMapper > 1) {
      @SuppressWarnings("unchecked")
      Class<? extends Mapper<EntityId, KijiRowData, Object, Object>> childMapperClass
          = (Class<? extends Mapper<EntityId, KijiRowData, Object, Object>>) mMapper.getClass();
      KijiMultithreadedMapper.setMapperClass(job, childMapperClass);
      KijiMultithreadedMapper.setNumberOfThreads(job, mNumThreadsPerMapper);
      job.setMapperClass(KijiMultithreadedMapper.class);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    final FreshenerContext context = InternalFreshenerContext.create(mAttachedColumn, mParameters);
    final Map<String, KeyValueStore<?, ?>> combinedStores = Maps.newHashMap();
    combinedStores.putAll(mScoreFunction.getRequiredStores(context));
    if (null != mKeyValueStoreOverrides) {
      combinedStores.putAll(mKeyValueStoreOverrides);
    }
    return combinedStores;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiDataRequest getDataRequest() {
    return mScoreFunctionDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapReduceJob build(
      final Job job
  ) {
    return KijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getCombiner() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mScoreFunctionClass;
  }
}
