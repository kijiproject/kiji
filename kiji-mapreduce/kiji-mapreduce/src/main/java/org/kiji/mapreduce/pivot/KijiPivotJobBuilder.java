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

package org.kiji.mapreduce.pivot;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.KijiMultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.framework.KijiTableInputJobBuilder;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.pivot.impl.PivoterMapper;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Builds jobs that run a {@link KijiPivoter} over a Kiji table.
 *
 * <p>
 *   {@link KijiPivoter} scans the rows from an input KijiTable and writes cells
 *   into an output KijiTable. The input and the output KijiTable may or may not be the same.
 * </p>
 *
 * <p>
 *   Use the {@link KijiPivotJobBuilder} to configure a {@link KijiPivoter} job, by specifying:
 *   <ul>
 *     <li> the {@link KijiPivoter} class to run over the input KijiTable; </li>
 *     <li> the input {@link KijiTable} to be processed by the {@link KijiPivoter}; </li>
 *     <li> the output {@link KijiTable} the {@link KijiPivoter} writes to. </li>
 *   </ul>
 * </p>
 *
 * <p> Example:
 * <pre><blockquote>
 *   final Configuration conf = ...;
 *   final KijiURI inputTableURI = ...;
 *   final KijiURI outputTableURI = ...;
 *   final KijiMapReduceJob job = KijiPivotJobBuilder.create()
 *       .withConf(conf)
 *       .withPivoter(SomePivoter.class)
 *       .withInputTable(inputTableURI)
 *       .withOutput(MapReduceJobOutputs
 *           .newHFileMapReduceJobOutput(outputTableURI, hfilePath))
 *       .build();
 *   job.run();
 * </blockquote></pre>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class KijiPivotJobBuilder
    extends KijiTableInputJobBuilder<KijiPivotJobBuilder> {

  /** Default number of threads per mapper to use for running pivoters. */
  private static final int DEFAULT_NUM_THREADS_PER_MAPPER = 1;

  /** {@link KijiPivoter} class to run over the table. */
  private Class<? extends KijiPivoter> mPivoterClass;

  /** Configured number of threads per mapper to use for running pivoters. */
  private int mNumThreadsPerMapper;

  /** Pivoter to run for this KijiMR pivot job. */
  private KijiPivoter mPivoter;

  /** Hadoop mapper to run for this KijiMR pivot job. */
  private KijiMapper<?, ?, ?, ?> mMapper;

  /** Hadoop reducer to run for this KijiMR pivot job. */
  private KijiReducer<?, ?, ?, ?> mReducer;

  /** Specification of the data requested for this pivot job. */
  private KijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Kiji table-mapper over a Kiji table. */
  private KijiPivotJobBuilder() {
    mPivoterClass = null;
    mNumThreadsPerMapper = DEFAULT_NUM_THREADS_PER_MAPPER;
    mPivoter = null;
    mMapper = null;
    mReducer = null;
    mDataRequest = null;
  }

  /**
   * Creates a new builder for a {@link KijiPivoter} job.
   *
   * @return a new builder for a {@link KijiPivoter} job.
   */
  public static KijiPivotJobBuilder create() {
    return new KijiPivotJobBuilder();
  }

  /**
   * Configures the job with the {@link KijiPivoter} to run.
   *
   * @param pivoterClass {@link KijiPivoter} class to run over the input Kiji table.
   * @return this builder instance.
   */
  public KijiPivotJobBuilder withPivoter(
      Class<? extends KijiPivoter> pivoterClass
  ) {
    mPivoterClass = pivoterClass;
    return this;
  }

  /**
   * Configures the output table of this pivoter.
   *
   * @param jobOutput Kiji table the pivoter writes to.
   * @return this builder instance.
   */
  public KijiPivotJobBuilder withOutput(KijiTableMapReduceJobOutput jobOutput) {
    return super.withOutput(jobOutput);
  }

  /**
   * {@inheritDoc}
   *
   * <p> The output of a pivoter must be a KijiTable. </p>
   */
  @Override
  public KijiPivotJobBuilder withOutput(MapReduceJobOutput jobOutput) {
    if (jobOutput instanceof KijiTableMapReduceJobOutput) {
      return withOutput((KijiTableMapReduceJobOutput) jobOutput);
    } else {
      throw new RuntimeException("KijiTableRWMapper must output to a Kiji table.");
    }
  }

  /**
   * Sets the number of threads to use for running the producer in parallel.
   *
   * <p>You may use this setting to run multiple instances of the pivoter in parallel
   * within each map task of the job.  This may useful for increasing throughput when the
   * pivoter is not CPU bound.</p>
   *
   * @param numThreads Number of threads to use per mapper.
   * @return this build instance.
   */
  public KijiPivotJobBuilder withNumThreads(int numThreads) {
    Preconditions.checkArgument(numThreads >= 1, "numThreads must be positive, got %d", numThreads);
    mNumThreadsPerMapper = numThreads;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    if (null == mPivoterClass) {
      throw new JobConfigurationException("Must specify a KijiPivoter class.");
    }

    // Serialize the pivoter class name into the job configuration.
    conf.setClass(KijiConfKeys.KIJI_PIVOTER_CLASS, mPivoterClass, KijiPivoter.class);

    // Producers should output to HFiles.
    mMapper = new PivoterMapper();
    mReducer = new IdentityReducer<Object, Object>();

    job.setJobName("KijiPivoter: " + mPivoterClass.getSimpleName());

    mPivoter = ReflectionUtils.newInstance(mPivoterClass, job.getConfiguration());
    mDataRequest = mPivoter.getDataRequest();

    // Configure the table input job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected void configureMapper(Job job) throws IOException {
    super.configureMapper(job);

    // Configure map-parallelism if configured.
    if (mNumThreadsPerMapper > 1) {
      @SuppressWarnings("unchecked")
      Class<? extends Mapper<EntityId, KijiRowData, Object, Object>> childMapperClass =
          (Class<? extends Mapper<EntityId, KijiRowData, Object, Object>>) mMapper.getClass();
      KijiMultithreadedMapper.setMapperClass(job, childMapperClass);
      KijiMultithreadedMapper.setNumberOfThreads(job, mNumThreadsPerMapper);
      job.setMapperClass(KijiMultithreadedMapper.class);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mPivoter.getRequiredStores();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapReduceJob build(Job job) {
    return KijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getCombiner() {
    // A pivoter cannot have combiners.
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
    return mPivoterClass;
  }
}
