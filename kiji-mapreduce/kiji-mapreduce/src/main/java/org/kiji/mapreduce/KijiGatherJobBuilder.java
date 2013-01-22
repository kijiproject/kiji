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

package org.kiji.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.mapper.GatherMapper;
import org.kiji.schema.KijiDataRequest;

/**
 * Builds jobs that run a gatherer over a Kiji table.
 *
 * <p>Example usage:</p>
 * <pre><code>
 * MapReduceJob job = new KijiGatherJobBuilder()
 *     .withInputTable(myTable)
 *     .withGatherer(MyCountGatherer.class)
 *     .withReducer(IntSumReducer.class)
 *     .withOutput(new TextMapReduceJobOutput("path/to/counts", numSplits))
 *     .build();
 * boolean success = job.run();
 * </code></pre>
 */
@SuppressWarnings("rawtypes")
@ApiAudience.Public
public final class KijiGatherJobBuilder extends KijiTableInputJobBuilder<KijiGatherJobBuilder> {
  /** The class of the gatherer to run. */
  private Class<? extends KijiGatherer> mGathererClass;
  /** The class of the combiner to run. */
  private Class<? extends KijiReducer> mCombinerClass;
  /** The class of the reducer to run. */
  private Class<? extends KijiReducer> mReducerClass;

  private GatherMapper mMapper;
  /** The gatherer instance. */
  private KijiGatherer<?, ?> mGatherer;
  /** The combiner instance (may be null if no combiner is specified). */
  private KijiReducer mCombiner;
  /** The reducer instance (may be null if no reducer is specified). */
  private KijiReducer mReducer;

  /** The data request for the job's table input. */
  private KijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Kiji gatherer over a Kiji table. */
  private KijiGatherJobBuilder() {
    mGathererClass = null;
    mCombinerClass = null;
    mReducerClass = null;

    mMapper = new GatherMapper();
    mGatherer = null;
    mCombiner = null;
    mReducer = null;

    mDataRequest = null;
  }

  /**
   * Creates a new builder for Kiji gather jobs.
   *
   * @return a new Kiji gather job builder.
   */
  public static KijiGatherJobBuilder create() {
    return new KijiGatherJobBuilder();
  }

  /**
   * Configures the job with the Kiji gatherer to run in the map phase.
   *
   * @param gathererClass The gatherer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiGatherJobBuilder withGatherer(Class<? extends KijiGatherer> gathererClass) {
    mGathererClass = gathererClass;
    return this;
  }

  /**
   * Configures the job with a combiner to run (optional).
   *
   * @param combinerClass The combiner class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiGatherJobBuilder withCombiner(
      Class<? extends KijiReducer> combinerClass) {
    mCombinerClass = combinerClass;
    return this;
  }

  /**
   * Configures the job with a reducer to run (optional).
   *
   * @param reducerClass The reducer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiGatherJobBuilder withReducer(Class<? extends KijiReducer> reducerClass) {
    mReducerClass = reducerClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Construct the gatherer instance.
    if (null == mGathererClass) {
      throw new JobConfigurationException("Must specify a gatherer.");
    }
    // Serialize the gatherer class name into the job configuration.
    job.getConfiguration().setClass(
        KijiGatherer.CONF_GATHERER_CLASS, mGathererClass, KijiGatherer.class);

    mGatherer = ReflectionUtils.newInstance(mGathererClass, job.getConfiguration());
    mMapper.setConf(job.getConfiguration());
    mDataRequest = mGatherer.getDataRequest();

    // Construct the combiner instance (if specified).
    if (null != mCombinerClass) {
      mCombiner = ReflectionUtils.newInstance(mCombinerClass, job.getConfiguration());
    }

    // Construct the reducer instance (if specified).
    if (null != mReducerClass) {
      mReducer = ReflectionUtils.newInstance(mReducerClass, job.getConfiguration());
    }

    StringBuilder name = new StringBuilder();
    name.append("Kiji gather: ");
    name.append(mGathererClass.getSimpleName());
    if (null != mReducerClass) {
      name.append(" / ");
      name.append(mReducerClass.getSimpleName());
    }
    job.setJobName(name.toString());

    // Configure the table input job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    Map<String, KeyValueStore<?, ?>> requiredStores = new HashMap<String, KeyValueStore<?, ?>>();

    Map<String, KeyValueStore<?, ?>> gathererStores = mGatherer.getRequiredStores();
    if (null != gathererStores) {
      mergeStores(requiredStores, gathererStores);
    }

    if (null != mCombiner && mCombiner instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> combinerStores =
          ((KeyValueStoreClient) mCombiner).getRequiredStores();
      if (null != combinerStores) {
        mergeStores(requiredStores, combinerStores);
      }
    }

    if (null != mReducer && mReducer instanceof KeyValueStoreClient) {
      Map<String, KeyValueStore<?, ?>> reducerStores =
          ((KeyValueStoreClient) mReducer).getRequiredStores();
      if (null != reducerStores) {
        mergeStores(requiredStores, reducerStores);
      }
    }

    return requiredStores;
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJob build(Job job) {
    return KijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getCombiner() {
    return mCombiner;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mGathererClass;
  }
}
